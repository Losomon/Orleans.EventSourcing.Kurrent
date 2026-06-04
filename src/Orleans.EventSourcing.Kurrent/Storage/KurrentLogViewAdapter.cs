using KurrentDB.Client;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Observability;
 
using Orleans.Storage;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Orleans.EventSourcing.Kurrent.Storage;

internal sealed class KurrentLogViewAdapter<TLogView, TLogEntry> : ILogViewAdaptor<TLogView, TLogEntry>, IDisposable where TLogView : new()
{
    readonly string streamName;
    readonly IKurrentClient client;
    readonly IEventSerializer<TLogEntry> eventConverter;
    readonly ILogViewAdaptorHost<TLogView, TLogEntry> host;
    readonly Channel<WorkItem> queue = Channel.CreateUnbounded<WorkItem>();
    readonly Task<bool> backgroundWorker;
    readonly CancellationTokenSource cts = new();
    readonly Queue<TLogEntry> pendingSuffix = new();
    readonly TagList observabilityTags;
    bool disposed;

    public KurrentLogViewAdapter(ILogViewAdaptorHost<TLogView, TLogEntry> host, IKurrentClient client, IEventSerializer<TLogEntry> eventConverter, ILogConsistencyProtocolServices services, IKurrentStreamNameProvider streamNameProvider)
    {
        this.streamName = streamNameProvider.GetStreamName(services.GrainId);
        this.client = client;
        this.eventConverter = eventConverter;
        this.host = host;
        this.observabilityTags = new TagList
        {
            { "GrainType", services.GrainId.Type.ToString() },
        };
        using (ExecutionContext.SuppressFlow())
        {
            this.backgroundWorker = StartWorker(cts.Token);
        }
    }

    enum WorkItemType
    {
        Load,
        Append,
        ConfirmSubmittedEntries,
        Clear
    }

    readonly struct WorkItem(WorkItemType type, TaskCompletionSource<bool>? taskCompletionSource, ExecutionContext? executionContext)
    {
        public WorkItemType Type { get; } = type;
        public TaskCompletionSource<bool>? CompletionSource { get; } = taskCompletionSource;
        public ExecutionContext? ExecutionContext { get; } = executionContext;
    }

    public TLogView TentativeView { get; private set; } = new();

    public TLogView ConfirmedView { get; private set; } = new();

    public int ConfirmedVersion { get; private set; }

    public IEnumerable<TLogEntry> UnconfirmedSuffix => pendingSuffix;

    public async Task ConfirmSubmittedEntries()
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        TaskCompletionSource<bool> taskCompletionSource = new();
        EnqueueNewWorkItem(WorkItemType.ConfirmSubmittedEntries, taskCompletionSource);
        var result = await Task.WhenAny(backgroundWorker, taskCompletionSource.Task).ConfigureAwait(true);
        await result.ConfigureAwait(true);
    }

    public async Task ClearLogAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        TaskCompletionSource<bool> taskCompletionSource = new();
        EnqueueNewWorkItem(WorkItemType.Clear, taskCompletionSource);
        var result = await Task.WhenAny(backgroundWorker, taskCompletionSource.Task).ConfigureAwait(true);
        await result.ConfigureAwait(true);
    }

    #region stats

    public void DisableStatsCollection() => throw new NotImplementedException();

    public void EnableStatsCollection() => throw new NotImplementedException();

    public LogConsistencyStatistics GetStats() => throw new NotImplementedException();

    #endregion

    public Task PostOnActivate() => Task.CompletedTask;

    public Task PostOnDeactivate()
    {
        Dispose();
        return Task.CompletedTask;
    }

    public Task PreOnActivate() => Task.CompletedTask;




    public async Task<IReadOnlyList<TLogEntry>> RetrieveLogSegment(int fromVersion, int toVersion)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        ArgumentOutOfRangeException.ThrowIfLessThan(toVersion, fromVersion);

        var maxCount = toVersion - fromVersion + 1; // inclusive range

        return await ReadAsync(fromVersion.ToStreamPosition(), maxCount, CancellationToken.None).Select(x => x.Log).ToListAsync().ConfigureAwait(true); // StreamPosition not needed here
    }

    public void Submit(TLogEntry entry)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        ArgumentNullException.ThrowIfNull(entry);

        Append(entry);

        EnqueueNewWorkItem(WorkItemType.Append, null);
    }

    public void SubmitRange(IEnumerable<TLogEntry> entries)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        Append(entries);

        EnqueueNewWorkItem(WorkItemType.Append, null);
    }

    private async Task<bool> StartWorker(CancellationToken token)
    {
        // This task is observed by all async operations completions, so if it fails their operations 
        // will fail too.
        bool failedLazyWrites = false;

        try
        {
            await foreach (var entry in queue.Reader.ReadAllAsync(token).ConfigureAwait(true))
            {
                if (entry.ExecutionContext is not null)
                {
                    ExecutionContext.Restore(entry.ExecutionContext);
                }

                switch (entry.Type)
                {
                    case WorkItemType.Load:
                        // Rebuilding two views allows us to avoid using the deep copier, which avoids TLogView needing to be Orleans serializable
                        // This may get expensive if there are a lot of events and the views take a long time to build
                        // DeepCopying could be faster, so we might want to look into using the deep copier if available for the type.

                        var newConfirmedView = new TLogView();
                        var newTentativeView = new TLogView();
                        ConfirmedVersion = 0;
                        bool firstEventInStream = true;
                        StreamPosition? pendingTruncationPosition = null;

                        await foreach (var logEntry in ReadAsync(StreamPosition.Start, int.MaxValue, token).ConfigureAwait(true))
                        {
                            if (IsDeletePriorEventsMarker(logEntry.Log) && !firstEventInStream)
                            {
                                newConfirmedView = new TLogView();
                                newTentativeView = new TLogView();

                                pendingTruncationPosition = logEntry.StreamPosition;
                            }

                            host.UpdateView(newConfirmedView, logEntry.Log);
                            host.UpdateView(newTentativeView, logEntry.Log);
                            ConfirmedVersion = logEntry.Version;
                            firstEventInStream = false;
                        }

                        if (pendingTruncationPosition is { } truncPos)
                        {
                            await DeleteBefore(truncPos, token).ConfigureAwait(true);
                        }

                        ConfirmedView = newConfirmedView;
                        TentativeView = newTentativeView;

                        host.OnViewChanged(true, true);
                        entry.CompletionSource?.SetResult(true);
                        break;
                    case WorkItemType.Append:

                        var entries = pendingSuffix.ToArray();
                        pendingSuffix.Clear();

                        if (entries.Length == 0)
                        {
                            entry.CompletionSource?.SetResult(true);
                            break;
                        }

                        var writeResult = await client.ConditionalAppendToStreamAsync(streamName, ConfirmedVersion.ToStreamState(), Serialize(entries), token).ConfigureAwait(true);

                        var success = writeResult.Status switch
                        {
                            ConditionalWriteStatus.VersionMismatch => false,
                            ConditionalWriteStatus.StreamDeleted => throw new InvalidOperationException($"{streamName} has been permanently deleted and cannot be written to ever again"),
                            ConditionalWriteStatus.Succeeded => true,
                            _ => throw new NotSupportedException($"Unexpected result from Kurrent: {writeResult.Status}"),
                        };

                        if (success)
                        {
                            foreach (var logEntry in entries)
                            {
                                if (IsDeletePriorEventsMarker(logEntry))
                                {
                                    ConfirmedView = new TLogView();
                                    var truncatePosition = StreamPosition.FromInt64(ConfirmedVersion);

                                    await DeleteBefore(truncatePosition, token).ConfigureAwait(true);                                
                                }
             

                                host.UpdateView(ConfirmedView, logEntry);
                                ConfirmedVersion++;
                            }

                            host.OnViewChanged(false, true);
                            entry.CompletionSource?.SetResult(true);
                        }
                        else
                        {
                            if (entry.CompletionSource is null)
                            {
                                failedLazyWrites = true;
                            }
                            else
                            {
                                entry.CompletionSource.SetResult(false);
                            }
                        }
                        break;
                    case WorkItemType.Clear:
                        // Soft delete the stream. We use the locally tracked ConfirmedVersion as the expected
                        // stream state, which mirrors the optimistic-concurrency approach used by appends.
                        // If we have no confirmed entries there is nothing to delete (the stream may not exist),
                        // matching KurrentGrainStorageProvider.ClearStateAsync which skips when the ETag is empty.
                        // See https://github.com/kurrent-io/KurrentDB/issues/4637
                        if (ConfirmedVersion > 0)
                        {
                            _ = await client.DeleteStreamAsync(streamName, ConfirmedVersion.ToStreamState(), token).ConfigureAwait(true);
                        }

                        // Reset in-memory state to match an empty stream.
                        pendingSuffix.Clear();
                        ConfirmedView = new TLogView();
                        TentativeView = new TLogView();
                        ConfirmedVersion = 0;
                        failedLazyWrites = false;

                        host.OnViewChanged(true, true);
                        entry.CompletionSource?.SetResult(true);
                        break;
                    case WorkItemType.ConfirmSubmittedEntries when entry.CompletionSource is not null:
                        if (failedLazyWrites)
                        {
                            entry.CompletionSource.SetException(new InconsistentStateException("stream has externally mutated, some events could not be written"));
                            failedLazyWrites = false;
                        }
                        else
                        {
                            entry.CompletionSource.SetResult(true);
                        }
                        break;
                    default:
                        throw new NotImplementedException(entry.Type.ToString());
                }
            }

            return true;
        }
        catch (Exception ex)
        {
            // In all cases this loop is doomed, and we need to kill the grain to restart it 
            // we could probably handle more gracefully, but this is fail safe at least
            throw KurrentExceptionConverter.ConvertException(ex);
        }
    }

    private static bool IsDeletePriorEventsMarker(TLogEntry entry)
        => entry?.GetType().IsDefined(typeof(DiscardPriorEventsAttribute), inherit: true) == true;

    private async Task DeleteBefore(StreamPosition truncateBefore, CancellationToken cancellationToken)
    {
        var streamMetadataResult = await client.GetStreamMetadata(streamName, cancellationToken).ConfigureAwait(true);

        if (streamMetadataResult.Metadata.TruncateBefore != truncateBefore)
        {
            await client.SetStreamMetadata(streamName,
                                           streamMetadataResult.MetastreamRevision.HasValue ? StreamState.StreamRevision(streamMetadataResult.MetastreamRevision.Value) : StreamState.NoStream,
                                           new StreamMetadata(
                                               streamMetadataResult.Metadata.MaxCount,
                                               streamMetadataResult.Metadata.MaxAge,
                                               truncateBefore: truncateBefore,
                                               streamMetadataResult.Metadata.CacheControl,
                                               streamMetadataResult.Metadata.Acl,
                                               streamMetadataResult.Metadata.CustomMetadata),
                                           cancellationToken).ConfigureAwait(true);
        }
    }

    private EventData[] Serialize(TLogEntry[] entries)
    {
        EventData[] serializedEntries = new EventData[entries.Length];

        for (var i = 0; i < entries.Length; i++)
        {
            var sw = Stopwatch.StartNew();
            serializedEntries[i] = eventConverter.SerializeEvent(entries[i]);
            sw.Stop();

            TagList deserializationTags = new();
            foreach (var tag in observabilityTags)
            {
                deserializationTags.Add(tag);
            }

            deserializationTags.Add("EventType", serializedEntries[i].Type);

            Metrics.EventDeserializationLatency.Record(sw.ElapsedMilliseconds, deserializationTags);
        }

        return serializedEntries;
    }

    private TLogEntry Deserialize(ResolvedEvent logEntry)
    {
        TagList deserializationTags = new();
        foreach (var tag in observabilityTags)
        {
            deserializationTags.Add(tag);
        }
        deserializationTags.Add("EventType", logEntry.Event.EventType);
        var sw = Stopwatch.StartNew();
        var eventEntry = eventConverter.DeserializeEvent(logEntry);
        Metrics.EventDeserializationLatency.Record(sw.ElapsedMilliseconds, deserializationTags);
        return eventEntry;
    }

    private async IAsyncEnumerable<(TLogEntry Log, int Version, StreamPosition StreamPosition)> ReadAsync(StreamPosition fromPosition, long maxCount, [EnumeratorCancellation] CancellationToken token)
    {
        var readResult = client.ReadStreamAsync(Direction.Forwards, streamName, fromPosition, maxCount, false, token);

        await foreach (var logEntry in readResult.ConfigureAwait(true))
        {
            yield return (Deserialize(logEntry), logEntry.OriginalEventNumber.ToVersion(), logEntry.OriginalEventNumber);
        }
    }

    public async Task Synchronize()
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        TaskCompletionSource<bool> taskCompletionSource = new();

        EnqueueNewWorkItem(WorkItemType.Load, taskCompletionSource);
        var result = await Task.WhenAny(taskCompletionSource.Task, backgroundWorker).ConfigureAwait(true);
        await result.ConfigureAwait(true);
    }

    public Task<bool> TryAppend(TLogEntry entry)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        Append(entry);

        return AwaitAppend();
    }

    private async Task<bool> AwaitAppend()
    {
        TaskCompletionSource<bool> appendCompletion = new();
        EnqueueNewWorkItem(WorkItemType.Append, appendCompletion);
        var result = await Task.WhenAny(appendCompletion.Task, backgroundWorker).ConfigureAwait(true);
        return await result.ConfigureAwait(true);
    }

    public Task<bool> TryAppendRange(IEnumerable<TLogEntry> entries)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        Append(entries);

        return AwaitAppend();
    }

    private void Append(IEnumerable<TLogEntry> entries)
    {
        foreach (var entry in entries)
        {
            if (IsDeletePriorEventsMarker(entry))
            {
                TentativeView = new TLogView();
            }

            host.UpdateView(TentativeView, entry);
            pendingSuffix.Enqueue(entry);
        }

        host.OnViewChanged(true, false);
    }

    private void Append(TLogEntry entry)
    {
        if (IsDeletePriorEventsMarker(entry))
        {
            TentativeView = new TLogView();
        }

        host.UpdateView(TentativeView, entry);
        pendingSuffix.Enqueue(entry);

        host.OnViewChanged(true, false);
    }

    public void Dispose()
    {
        if (!disposed)
        {
            cts.Dispose();
            queue.Writer.Complete();
            disposed = true;
        }
    }

    private void EnqueueNewWorkItem(WorkItemType type, TaskCompletionSource<bool>? taskCompletionSource) => ObjectDisposedException.ThrowIf(!queue.Writer.TryWrite(new WorkItem(type, taskCompletionSource, ExecutionContext.Capture())), this);
}
