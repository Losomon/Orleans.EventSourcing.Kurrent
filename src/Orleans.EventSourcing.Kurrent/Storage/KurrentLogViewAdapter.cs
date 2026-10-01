using KurrentDB.Client;
using Orleans.EventSourcing.Kurrent.Observability;

using Orleans.Storage;
using System.Diagnostics;

namespace Orleans.EventSourcing.Kurrent.Storage;

internal sealed class KurrentLogViewAdapter<TLogView, TLogEntry> : ILogViewAdaptor<TLogView, TLogEntry>, IDisposable, IAsyncDisposable where TLogView : new()
{
    readonly ILogViewAdaptorHost<TLogView, TLogEntry> host;
    readonly CommandContext context;
    readonly CancellationTokenSource disposeCts = new();

    // Grain-side state. Only mutated from the grain's scheduler; all Kurrent I/O runs on the
    // observation chain, whose links resume on the grain's scheduler at each await point.
    readonly Queue<TLogEntry> pendingSuffix = new();
    Task observationTail = Task.CompletedTask;
    List<Exception>? writeFailures;
    Exception? streamFault;
    bool writeRejected;
    bool disposed;

    public KurrentLogViewAdapter(ILogViewAdaptorHost<TLogView, TLogEntry> host, IKurrentClient client, IEventConverter<TLogEntry> eventConverter, ILogConsistencyProtocolServices services, IKurrentStreamNameProvider streamNameProvider)
    {
        this.host = host;
        this.context = new CommandContext
        {
            Client = client,
            StreamName = streamNameProvider.GetStreamName(services.GrainId),
            Converter = eventConverter,
            Host = host,
            Tags = new TagList
            {
                { "GrainType", services.GrainId.Type.ToString() },
            },
        };
    }

    public TLogView TentativeView { get; private set; } = new();

    public TLogView ConfirmedView { get; private set; } = new();

    public int ConfirmedVersion { get; private set; }

    public IEnumerable<TLogEntry> UnconfirmedSuffix => pendingSuffix;

    public async Task ConfirmSubmittedEntries()
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        var succeeded = await ObservePendingWritesAsync().ConfigureAwait(true);
        if (!succeeded)
        {
            throw new InconsistentStateException("stream has externally mutated, some events could not be written");
        }
    }

    public async Task ClearLogAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        _ = await ObservePendingWritesAsync().ConfigureAwait(true);

        var clearTask = RunClearAsync(observationTail, ConfirmedVersion, cancellationToken);
        observationTail = IgnoreFailuresAsync(clearTask);

        try
        {
            await clearTask.ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
#pragma warning disable CA1031 // Do not catch general exception types
        catch (Exception ex)
#pragma warning restore CA1031 // Converted and rethrown below
        {
            throw KurrentExceptionConverter.ConvertException(ex);
        }

        // Reset in-memory state to match an empty stream.
        pendingSuffix.Clear();
        ConfirmedView = new TLogView();
        TentativeView = new TLogView();
        ConfirmedVersion = 0;

        host.OnViewChanged(true, true);
    }

    #region stats

    public void DisableStatsCollection() => throw new NotImplementedException();

    public void EnableStatsCollection() => throw new NotImplementedException();

    public LogConsistencyStatistics GetStats() => throw new NotImplementedException();

    #endregion

    public Task PostOnActivate() => Task.CompletedTask;

    public Task PostOnDeactivate() => DisposeAsync().AsTask();

    public Task PreOnActivate() => Task.CompletedTask;

    public async Task<IReadOnlyList<TLogEntry>> RetrieveLogSegment(int fromVersion, int toVersion)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        ArgumentOutOfRangeException.ThrowIfLessThan(toVersion, fromVersion);

        var maxCount = toVersion - fromVersion + 1; // inclusive range

        try
        {
            var readResult = await context.Client.ReadStreamAsync(Direction.Forwards, context.StreamName, fromVersion.ToStreamPosition(), maxCount, false, CancellationToken.None)
                                                 .ConfigureAwait(false);

            var result = new List<TLogEntry>();
            await foreach (var resolvedEvent in readResult.ConfigureAwait(false))
            {
                result.Add(Deserialize(context, resolvedEvent));
            }

            return result;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
#pragma warning disable CA1031 // Do not catch general exception types
        catch (Exception ex)
#pragma warning restore CA1031 // Converted and rethrown below
        {
            throw KurrentExceptionConverter.ConvertException(ex);
        }
    }

    public void Submit(TLogEntry entry)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        ArgumentNullException.ThrowIfNull(entry);

        Append(entry);
        EnqueueWrite([entry]);
    }

    public void SubmitRange(IEnumerable<TLogEntry> entries)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        ArgumentNullException.ThrowIfNull(entries);

        var batch = entries.ToArray();

        Append(batch);
        EnqueueWrite(batch);
    }

    public async Task<bool> TryAppend(TLogEntry entry)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        ArgumentNullException.ThrowIfNull(entry);

        Append(entry);
        EnqueueWrite([entry]);

        return await ObservePendingWritesAsync().ConfigureAwait(true);
    }

    public async Task<bool> TryAppendRange(IEnumerable<TLogEntry> entries)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        ArgumentNullException.ThrowIfNull(entries);

        var batch = entries.ToArray();

        Append(batch);
        EnqueueWrite(batch);

        return await ObservePendingWritesAsync().ConfigureAwait(true);
    }

    public async Task Synchronize()
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        _ = await ObservePendingWritesAsync().ConfigureAwait(true);

        var loadTask = RunLoadAsync(observationTail);
        observationTail = IgnoreFailuresAsync(loadTask);

        LoadResult loaded;
        try
        {
            loaded = await loadTask.ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
#pragma warning disable CA1031 // Do not catch general exception types
        catch (Exception ex)
#pragma warning restore CA1031 // Converted and rethrown below
        {
            throw KurrentExceptionConverter.ConvertException(ex);
        }

        ConfirmedView = loaded.ConfirmedView;
        ConfirmedVersion = loaded.Version;

        // The suffix is normally empty here (all in-flight writes were observed above),
        // but reapply anything left over so tentative state remains consistent.
        var tentative = loaded.TentativeView;
        foreach (var entry in pendingSuffix)
        {
            if (IsDeletePriorEventsMarker(entry))
            {
                tentative = new TLogView();
            }

            host.UpdateView(tentative, entry);
        }

        TentativeView = tentative;
        host.OnViewChanged(true, true);
    }

    /// <summary>
    /// Snapshots <paramref name="batch"/> into a chain link so the Kurrent I/O never touches
    /// grain-side state mid-flight. The expected stream version chains after all in-flight
    /// batches. Links execute strictly FIFO and resume on the grain's scheduler, so grain-side
    /// state is still only mutated single-threaded.
    /// </summary>
    private void EnqueueWrite(TLogEntry[] batch)
    {
        if (batch.Length == 0)
        {
            return;
        }

        // pendingSuffix already contains this batch, so subtract it back out.
        var expectedVersion = ConfirmedVersion + pendingSuffix.Count - batch.Length;
        observationTail = ObserveWriteAsync(observationTail, batch, expectedVersion);
    }

    /// <summary>
    /// Performs and observes a single write in FIFO order (chained on the previous link). A
    /// successful batch is pruned from the head of <see cref="pendingSuffix"/> and applied to
    /// <see cref="ConfirmedView"/>; a failed batch is pruned without being applied (it was never
    /// written and will not be retried, matching the tentative-only semantics of a rejected
    /// conditional append). Never throws: failures are recorded and surfaced by
    /// <see cref="ObservePendingWritesAsync"/>.
    /// </summary>
    private async Task ObserveWriteAsync(Task previous, TLogEntry[] batch, int expectedVersion)
    {
        await previous.ConfigureAwait(true); // never faults

        if (disposed)
        {
            // The adapter is being disposed; the write never happened.
            RemoveFromSuffix(batch.Length);
            return;
        }

        bool success;
        try
        {
            ThrowIfStreamFaulted();
            success = await AppendAsync(context, batch, expectedVersion, disposeCts.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            // The adapter is being disposed; the write never happened.
            RemoveFromSuffix(batch.Length);
            return;
        }
#pragma warning disable CA1031 // Do not catch general exception types
        catch (Exception ex)
#pragma warning restore CA1031 // Recorded and rethrown from the next observation point
        {
            // An infrastructure failure leaves the true stream state unknown; poison subsequent
            // writes until a successful load or clear re-establishes a known state.
            streamFault ??= ex;
            (writeFailures ??= []).Add(ex);
            RemoveFromSuffix(batch.Length);
            return;
        }

        if (success)
        {
            ApplyConfirmed(batch);
            host.OnViewChanged(false, true);
        }
        else
        {
            writeRejected = true;
            RemoveFromSuffix(batch.Length);
        }
    }

    /// <summary>
    /// Awaits the observation chain, then surfaces any recorded outcome: throws (converted) for
    /// infrastructure failures, returns false if any write was rejected with a version mismatch.
    /// A rejection means the grain's view of the stream version is stale and the true stream
    /// state is unknown, so <see cref="writeRejected"/> is left set until a reload
    /// (<see cref="Synchronize"/>/<see cref="ClearLogAsync"/>) re-establishes a known state;
    /// every call made in the meantime keeps reporting the rejection.
    /// </summary>
    private async Task<bool> ObservePendingWritesAsync()
    {
        await observationTail.ConfigureAwait(true);

        if (writeFailures is not null)
        {
            var failure = writeFailures[0];
            writeFailures = null;
            throw KurrentExceptionConverter.ConvertException(failure);
        }

        return !writeRejected;
    }

    /// <summary>
    /// Runs a full stream load as a chain link, keeping it FIFO with any writes submitted by
    /// interleaved grain calls. A successful load re-establishes a known stream state, clearing
    /// any write-failure or rejection poison.
    /// </summary>
    private async Task<LoadResult> RunLoadAsync(Task previous)
    {
        await previous.ConfigureAwait(true); // never faults

        disposeCts.Token.ThrowIfCancellationRequested();
        var result = await LoadAsync(context, disposeCts.Token).ConfigureAwait(true);
        streamFault = null;
        writeRejected = false;
        return result;
    }

    /// <summary>
    /// Runs a stream tombstone as a chain link, keeping it FIFO with any writes submitted by
    /// interleaved grain calls. Uses the grain-observed confirmed version as the expected stream
    /// state, mirroring the optimistic-concurrency approach used by appends. If there are no
    /// confirmed entries there is nothing to delete (the stream may not exist), matching
    /// KurrentGrainStorageProvider.ClearStateAsync which skips when the ETag is empty.
    /// See https://github.com/kurrent-io/KurrentDB/issues/4637
    /// </summary>
    private async Task RunClearAsync(Task previous, int expectedVersion, CancellationToken cancellationToken)
    {
        await previous.ConfigureAwait(true); // never faults

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, disposeCts.Token);
        linked.Token.ThrowIfCancellationRequested();

        if (expectedVersion > 0)
        {
            _ = await context.Client.TombstoneStreamAsync(context.StreamName, expectedVersion.ToStreamState(), linked.Token).ConfigureAwait(true);
        }

        // The stream is gone; there is nothing left to be inconsistent with.
        streamFault = null;
        writeRejected = false;
    }

    /// <summary>
    /// Wraps a caller-awaited chain link so the tail itself never faults.
    /// </summary>
    private static async Task IgnoreFailuresAsync(Task task)
        => await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);

    private void ThrowIfStreamFaulted()
    {
        if (streamFault is not null)
        {
            throw new InvalidOperationException("A previous write failed and the stream state is unknown; the log must be resynchronized before further writes.", streamFault);
        }
    }

    private void ApplyConfirmed(TLogEntry[] batch)
    {
        foreach (var entry in batch)
        {
            if (IsDeletePriorEventsMarker(entry))
            {
                ConfirmedView = new TLogView();
            }

            host.UpdateView(ConfirmedView, entry);
            ConfirmedVersion++;
        }

        RemoveFromSuffix(batch.Length);
    }

    private void RemoveFromSuffix(int count)
    {
        for (var i = 0; i < count && pendingSuffix.Count > 0; i++)
        {
            _ = pendingSuffix.Dequeue();
        }
    }

    private void Append(TLogEntry[] entries)
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
            disposed = true;
            disposeCts.Cancel(); // cancelled before disposal so in-flight links observe it safely

            // Don't dispose disposeCts until every in-flight link has actually observed the
            // cancellation and completed; otherwise a link still awaiting Kurrent I/O could
            // access a disposed CancellationTokenSource. Continue off the grain scheduler so
            // this never blocks the caller.
            observationTail.ContinueWith(
                static (_, state) => ((CancellationTokenSource)state!).Dispose(),
                disposeCts,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default).Ignore();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!disposed)
        {
            disposed = true;
            await disposeCts.CancelAsync().ConfigureAwait(true);
            await observationTail.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext); // never faults
            disposeCts.Dispose();
        }
    }

    private static bool IsDeletePriorEventsMarker(TLogEntry entry)
        => entry?.GetType().IsDefined(typeof(DiscardPriorEventsAttribute), inherit: true) == true;

    private static async Task DeleteBeforeAsync(CommandContext context, StreamPosition truncateBefore, CancellationToken cancellationToken)
    {
        var streamMetadataResult = await context.Client.GetStreamMetadata(context.StreamName, cancellationToken).ConfigureAwait(false);

        if (streamMetadataResult.Metadata.TruncateBefore != truncateBefore)
        {
            await context.Client.SetStreamMetadata(context.StreamName,
                                                   streamMetadataResult.MetastreamRevision.HasValue ? StreamState.StreamRevision(streamMetadataResult.MetastreamRevision.Value) : StreamState.NoStream,
                                                   new StreamMetadata(
                                                       streamMetadataResult.Metadata.MaxCount,
                                                       streamMetadataResult.Metadata.MaxAge,
                                                       truncateBefore: truncateBefore,
                                                       streamMetadataResult.Metadata.CacheControl,
                                                       streamMetadataResult.Metadata.Acl,
                                                       streamMetadataResult.Metadata.CustomMetadata),
                                                   cancellationToken).ConfigureAwait(false);
        }
    }

    private static EventData[] Serialize(CommandContext context, TLogEntry[] entries)
    {
        EventData[] serializedEntries = new EventData[entries.Length];

        for (var i = 0; i < entries.Length; i++)
        {
            var sw = Stopwatch.StartNew();
            serializedEntries[i] = context.Converter.SerializeEvent(entries[i]);
            sw.Stop();

            TagList serializationTags = new();
            foreach (var tag in context.Tags)
            {
                serializationTags.Add(tag);
            }

            serializationTags.Add("EventType", serializedEntries[i].Type);

            Metrics.EventSerializationLatency.Record(sw.ElapsedMilliseconds, serializationTags);
        }

        return serializedEntries;
    }

    private static TLogEntry Deserialize(CommandContext context, ResolvedEvent logEntry)
    {
        TagList deserializationTags = new();
        foreach (var tag in context.Tags)
        {
            deserializationTags.Add(tag);
        }
        deserializationTags.Add("EventType", logEntry.Event.EventType);
        var sw = Stopwatch.StartNew();
        var eventEntry = context.Converter.DeserializeEvent(logEntry);
        Metrics.EventDeserializationLatency.Record(sw.ElapsedMilliseconds, deserializationTags);
        return eventEntry;
    }

    /// <summary>
    /// Immutable dependencies shared by all Kurrent I/O executed on the observation chain.
    /// Mutable cross-link state (e.g. <see cref="streamFault"/>) lives on the adapter and is
    /// only touched by chain links on the grain's scheduler.
    /// </summary>
    private sealed class CommandContext
    {
        public required IKurrentClient Client { get; init; }
        public required string StreamName { get; init; }
        public required IEventConverter<TLogEntry> Converter { get; init; }
        public required ILogViewAdaptorHost<TLogView, TLogEntry> Host { get; init; }
        public required TagList Tags { get; init; }
    }

    private sealed record LoadResult(TLogView ConfirmedView, TLogView TentativeView, int Version);

    /// <summary>
    /// Rebuilds the confirmed view by replaying the stream, honouring truncation markers and
    /// healing unset $tb metadata. Builds two identical views so the adapter does not need a
    /// deep copier for <typeparamref name="TLogView"/>.
    /// </summary>
    private static async Task<LoadResult> LoadAsync(CommandContext context, CancellationToken token)
    {
        var confirmedView = new TLogView();
        var tentativeView = new TLogView();
        var version = 0;
        var firstEventInStream = true;
        StreamPosition? pendingTruncationPosition = null;

        var readResult = await context.Client.ReadStreamAsync(Direction.Forwards, context.StreamName, StreamPosition.Start, int.MaxValue, false, token)
                                             .ConfigureAwait(false);

        await foreach (var resolvedEvent in readResult.ConfigureAwait(false))
        {
            var deserializedLog = Deserialize(context, resolvedEvent);
            if (IsDeletePriorEventsMarker(deserializedLog) && !firstEventInStream)
            {
                confirmedView = new TLogView();
                tentativeView = new TLogView();

                pendingTruncationPosition = StreamPosition.FromStreamRevision(resolvedEvent.OriginalEventNumber);
            }

            context.Host.UpdateView(confirmedView, deserializedLog);
            context.Host.UpdateView(tentativeView, deserializedLog);
            firstEventInStream = false;
            version = resolvedEvent.OriginalEventNumber.ToVersion();
        }

        if (readResult.LastStreamPosition.HasValue)
        {
            version = readResult.LastStreamPosition.Value.ToVersion();
        }

        if (pendingTruncationPosition is { } truncPos)
        {
            await DeleteBeforeAsync(context, truncPos, token).ConfigureAwait(false);
        }

        return new LoadResult(confirmedView, tentativeView, version);
    }

    /// <summary>
    /// Conditionally appends a snapshot of submitted entries. Returns false on a version
    /// mismatch; the grain decides how to react when it observes the result.
    /// </summary>
    private static async Task<bool> AppendAsync(CommandContext context, TLogEntry[] batch, int expectedVersion, CancellationToken token)
    {
        var writeResult = await context.Client.ConditionalAppendToStreamAsync(context.StreamName, expectedVersion.ToStreamState(), Serialize(context, batch), token).ConfigureAwait(false);

        var success = writeResult.Status switch
        {
            ConditionalWriteStatus.VersionMismatch => false,
            ConditionalWriteStatus.StreamDeleted => throw new InvalidOperationException($"{context.StreamName} has been permanently deleted and cannot be written to ever again"),
            ConditionalWriteStatus.Succeeded => true,
            _ => throw new NotSupportedException($"Unexpected result from Kurrent: {writeResult.Status}"),
        };

        if (success)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                if (IsDeletePriorEventsMarker(batch[i]))
                {
                    // Truncate everything before the marker's own stream position.
                    await DeleteBeforeAsync(context, StreamPosition.FromInt64(expectedVersion + i), token).ConfigureAwait(false);
                }
            }
        }

        return success;
    }
}
