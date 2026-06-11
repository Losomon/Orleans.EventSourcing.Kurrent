using KurrentDB.Client;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Channels;

namespace Orleans.EventSourcing.Kurrent.Storage;

// A pretend subset of Kurrent implemented entirely in memory
internal sealed class InMemoryKurrentClient : IKurrentClient
{
#pragma warning disable CA2213 // Disposable fields should be disposed
    private readonly SemaphoreSlim singleAccess = new(1);
    private readonly Dictionary<string, List<int>> streams = [];
    private readonly Dictionary<string, List<StreamMetadata>> streamMetadata = [];
    private readonly Collection<EventRecord> all = [];
    private readonly BroadcastPublisher<EventRecord> newEventWatcher = new();
#pragma warning restore CA2213 // Disposable fields should be disposed

    private static readonly ConstructorInfo ConditionalWriteResultConstructor = typeof(ConditionalWriteResult).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance,
                                                                                                                              null,
                                                                                                                              [typeof(StreamState), typeof(Position), typeof(ConditionalWriteStatus),],
                                                                                                                              null)!;

    // Because this type is statically rooted
    // and reused, we do not clean-up
    public void Dispose() { }

    public ValueTask DisposeAsync()
        =>ValueTask.CompletedTask;
    

    public async IAsyncEnumerable<ResolvedEvent> ReadStreamAsync(Direction direction,
                                                                 string streamName,
                                                                 StreamPosition revision,
                                                                 long maxCount,
                                                                 bool resolveLinkTos,
                                                                 [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await singleAccess.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {

            if (!streams.TryGetValue(streamName, out var events))
            {
                yield break;
            }

            var metadata = GetStreamMetadataCore(streamName);

            // --- LINQ Pipeline Setup ---
            var query = all.Where(x => x.EventStreamId == streamName); // Start with the full list of events for the stream

            if (metadata.Metadata.TruncateBefore is { } truncateBefore)
            {
                // Filter out events before the metadata truncate point
                query = query.Where(e => e.EventNumber.CompareTo(truncateBefore) >= 0);
            }

            // Filter based on direction and revision
            if (direction == Direction.Forwards)
            {
                // Reading forwards: include events AT or AFTER the revision
                query = query.Where(e => e.EventStreamId == streamName && e.EventNumber.CompareTo(revision) >= 0);
                if (metadata.Metadata.MaxCount is { } metadataMaxCount)
                {
                    query = query.Skip(events.Count - metadataMaxCount);
                }
            }
            else // Direction.Backwards
            {
                // Reading backwards: include events AT or BEFORE the revision
                query = query.Where(e => e.EventNumber.CompareTo(revision) <= 0);


                // Crucial for backwards: Reverse the *filtered* sequence
                // we process events from the revision point backwards.
                query = query.Reverse();

                if (metadata.Metadata.MaxCount is { } metadataMaxCount)
                {
                    query = query.Take(metadataMaxCount);
                }
            }

            // Take requires an int, ensure maxCount doesn't overflow int.MaxValue
            var countToTake = (int)Math.Min(maxCount, int.MaxValue);
            query = query.Take(countToTake);

            foreach (var eventRecord in query)
            {
                yield return new ResolvedEvent(eventRecord, link: null, commitPosition: null);
                ;
            }

            await Task.CompletedTask.ConfigureAwait(false); // satisfy async method requirement
        }
        finally
        {
            singleAccess.Release();
        }
    }

    public async Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName,
                                                                       StreamState expectedRevision,
                                                                       IEnumerable<EventData> eventData,
                                                                       CancellationToken cancellationToken)
    {
        await singleAccess.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var streamState = StreamState.NoStream;
            if (streams.TryGetValue(streamName, out var events))
            {
                streamState = StreamState.StreamRevision(all[events[^1]].EventNumber);
            }

            var metadata = GetStreamMetadataCore(streamName);
            var isSoftDeleted = metadata.Metadata.TruncateBefore is not null
                                && metadata.Metadata.TruncateBefore == StreamPosition.End;

            if (streamState != expectedRevision && !isSoftDeleted)
            {
                // mismatch and return with expectedRevision (since it didn't change)
                return (ConditionalWriteResult)ConditionalWriteResultConstructor.Invoke([streamState, default, ConditionalWriteStatus.VersionMismatch,]);
            }

            if (events is null)
            {
                events = [];
                streams[streamName] = events;
            }

            // Real KurrentDB: writing to a soft-deleted stream resumes the stream and existing events stay hidden.
            // We model this by moving truncateBefore to the position of the first new event so subsequent reads
            // only return events written after the soft delete.
            if (isSoftDeleted)
            {
                var firstNewEventNumber = events.Count > 0
                    ? StreamPosition.FromInt64(all[events[^1]].EventNumber.ToInt64() + 1)
                    : StreamPosition.Start;

                SetStreamMetadataCore(streamName,
                                      metadata.MetastreamRevision.HasValue
                                          ? StreamState.StreamRevision(metadata.MetastreamRevision.Value)
                                          : StreamState.NoStream,
                                      new StreamMetadata(truncateBefore: firstNewEventNumber));
            }

            foreach (var newEvent in eventData)
            {
                var nextEventNumber = events.Count > 0 ? all[events[^1]].EventNumber.ToUInt64() + 1 : 0;
                var eventRecord = CreateEventRecord(newEvent, streamName, new StreamPosition(nextEventNumber), new Position((ulong)all.Count, (ulong)all.Count));
                events.Add(all.Count);
                all.Add(eventRecord);
                newEventWatcher.Publish(eventRecord);
            }

            streamState = StreamState.StreamRevision(all[events[^1]].EventNumber);
            return (ConditionalWriteResult)ConditionalWriteResultConstructor.Invoke([streamState, default, ConditionalWriteStatus.Succeeded,]);
        }
        finally
        {
            singleAccess.Release();
        }
    }

    private static EventRecord CreateEventRecord(EventData eventData, string eventStreamId, StreamPosition eventNumber, Position position)
    {
        var metadata = new Dictionary<string, string>
                       {
                           { "type", eventData.Type },
                           { "created", DateTime.UtcNow.Ticks.ToString(NumberFormatInfo.InvariantInfo) },
                           { "content-type", eventData.ContentType },
                       };

        return new EventRecord(eventStreamId, eventData.EventId, eventNumber, position, metadata, eventData.Data, eventData.Metadata);
    }

    internal sealed record WriteResult(long NextExpectedVersion,
                                       Position LogPosition,
                                       StreamState NextExpectedStreamState) : IWriteResult;

    public async Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata metadata, CancellationToken token)
    {
        await singleAccess.WaitAsync(token).ConfigureAwait(false);
        try
        {
            return SetStreamMetadataCore(streamName, expectedRevision, metadata);
        }
        finally
        {
            singleAccess.Release();
        }
    }

    private WriteResult SetStreamMetadataCore(string streamName, StreamState _, StreamMetadata metadata)
    {
        // TODO: Check revision
        if (!streamMetadata.TryGetValue(streamName, out var events))
        {
            events = [];
            streamMetadata.Add(streamName, events);
        }

        events.Add(metadata);

        return new WriteResult(events.Count, default, StreamState.StreamRevision((ulong)events.Count));
    }

    public async Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
    {
        await singleAccess.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (!streams.TryGetValue(streamName, out var events))
            {
                // Stream does not exist, return a successful delete result with no changes
                return new DeleteResult();
            }

            // Check if the expected revision matches the current stream revision
            var metadata = GetStreamMetadataCore(streamName);
            var currentRevision = StreamState.StreamRevision(all[events[^1]].EventNumber);

            // Check if the stream revision is valid based on metadata truncation
            if (currentRevision != expectedRevision
                && (metadata.Metadata.TruncateBefore is null || all[events[^1]].EventNumber >= metadata.Metadata.TruncateBefore))
            {
                // Revision mismatch, return a failed delete result
                throw new WrongExpectedVersionException(streamName, expectedRevision, currentRevision);
            }

            SetStreamMetadataCore(streamName,
                                    metadata.MetastreamRevision.HasValue ? StreamState.StreamRevision(metadata.MetastreamRevision.Value) : StreamState.NoStream,
                                    new StreamMetadata(truncateBefore: StreamPosition.End));


            // Return a successful delete result
            return new DeleteResult();
        }
        finally
        {
            singleAccess.Release();
        }
    }

    public async Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
    {
        await singleAccess.WaitAsync(token).ConfigureAwait(false);
        try
        {
            return GetStreamMetadataCore(streamName);
        }
        finally
        {
            singleAccess.Release();
        }
    }

    private StreamMetadataResult GetStreamMetadataCore(string streamName)
    {
        if (!streamMetadata.TryGetValue(streamName, out var events))
        {
            return StreamMetadataResult.None(streamName);
        }

        return StreamMetadataResult.Create(streamName, StreamPosition.FromInt64(events.Count), events[^1]);
    }

    public async IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Regex? eventTypeRegex = null;

        if (eventFilter.Regex.ToString() is { } eventFilterRegex)
        {
            eventTypeRegex = new Regex(eventFilterRegex);
        }

        ulong index = 1;

        bool IncludeEvent(EventRecord @event)
        {
            return (index++ >= start.ToUInt64().commitPosition) &&
             ((eventTypeRegex?.IsMatch(@event.EventType) ?? false) ||
                  eventFilter.Prefixes.Any(x => @event.EventStreamId.StartsWith(x!, StringComparison.OrdinalIgnoreCase)));
        }

        IAsyncEnumerator<EventRecord>? enumerator = null;
        try
        {
            await singleAccess.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                foreach (var @event in all)
                {
                    if (IncludeEvent(@event))
                    {
                        yield return new StreamMessage.Event(new ResolvedEvent(@event, null, index));
                    }
                }
                enumerator = newEventWatcher.GetAsyncEnumerator(cancellationToken);

                yield return new StreamMessage.CaughtUp();
            }
            finally
            {
                singleAccess.Release();
            }

            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                var @event = enumerator.Current;

                if (IncludeEvent(@event))
                {
                    yield return new StreamMessage.Event(new ResolvedEvent(@event, null, index));
                }
            }
        }
        finally
        {
            if (enumerator is not null)
                await enumerator.DisposeAsync().ConfigureAwait(false);
        }

    }

    readonly static ConcurrentDictionary<string, IKurrentClient> Clients = [];

    internal static IKurrentClient Get(string name)
     => Clients.GetOrAdd(name, _ => new InMemoryKurrentClient());

    /// <summary>
    /// A simple multi-subscriber broadcast publisher. Each call to
    /// <see cref="GetAsyncEnumerator"/> creates an independent unbounded channel that
    /// receives every item published after the subscription starts. Items published
    /// before a subscriber attaches are not replayed.
    /// </summary>
    private sealed class BroadcastPublisher<T> : IDisposable
    {
#if NET9_0_OR_GREATER
        private readonly Lock gate = new();
#else
        private readonly object gate = new();
#endif
        private readonly List<Channel<T>> subscribers = [];
        private bool disposed;

        public void Publish(T item)
        {
            Channel<T>[] snapshot;
            lock (gate)
            {
                if (disposed)
                {
                    return;
                }
                snapshot = [.. subscribers];
            }

            foreach (var subscriber in snapshot)
            {
                subscriber.Writer.TryWrite(item);
            }
        }

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            var channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
            });

            lock (gate)
            {
                if (disposed)
                {
                    channel.Writer.TryComplete();
                }
                else
                {
                    subscribers.Add(channel);
                }
            }

            return ReadAllAsync(channel, cancellationToken).GetAsyncEnumerator(cancellationToken);
        }

        private async IAsyncEnumerable<T> ReadAllAsync(Channel<T> channel, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    yield return item;
                }
            }
            finally
            {
                lock (gate)
                {
                    subscribers.Remove(channel);
                }
                channel.Writer.TryComplete();
            }
        }

        public void Dispose()
        {
            Channel<T>[] snapshot;
            lock (gate)
            {
                if (disposed)
                {
                    return;
                }
                disposed = true;
                snapshot = [.. subscribers];
                subscribers.Clear();
            }

            foreach (var subscriber in snapshot)
            {
                subscriber.Writer.TryComplete();
            }
        }
    }
}
