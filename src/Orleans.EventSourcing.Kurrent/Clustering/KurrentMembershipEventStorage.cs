using KurrentDB.Client;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.EventSourcing.Kurrent.Clustering.Events;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Storage;
using System.Globalization;
using System.Text.Json;

namespace Orleans.EventSourcing.Kurrent.Clustering;

// Owns all persistence concerns for membership: Kurrent stream I/O, JSON serialization, stream naming,
// and the translation between the Kurrent StreamState (the read cursor / optimistic-concurrency token)
// and the opaque ETag string carried by MembershipView.
internal sealed class KurrentMembershipEventStorage(IKurrentClient client, IOptions<KurrentClusteringOptions> options, IOptions<ClusterOptions> clusterOptions, JsonSerializerOptions jsonSerializerOptions)
{
    private readonly string streamName = $"{options.Value.StreamPrefix}/{clusterOptions.Value.ServiceId}/{clusterOptions.Value.ClusterId}";
    private static readonly string NoStreamEtag = StreamState.NoStream.ToString();

    // The empty view carries the "no stream" ETag so the first write appends with the correct concurrency token.
    internal MembershipView InitialView { get; } = new() { ETag = NoStreamEtag };

    private static StreamState ToStreamState(string etag)
        => etag == NoStreamEtag
            ? StreamState.NoStream
            : StreamState.StreamRevision(ulong.Parse(etag, CultureInfo.InvariantCulture));

    internal async Task<MembershipView> RefreshState(MembershipView view)
    {
        var expectedStreamState = ToStreamState(view.ETag);

        await foreach (var resolvedEvent in client.ReadStreamAsync(Direction.Forwards, streamName, expectedStreamState == StreamState.NoStream ? StreamPosition.Start : StreamPosition.FromInt64(expectedStreamState.ToInt64()), long.MaxValue, false, CancellationToken.None)
                                                  .ConfigureAwait(true))
        {
            EventBase? deserializedEvent = resolvedEvent.Event.EventType switch
            {
                nameof(MembershipTableSnapshot) => JsonSerializer.Deserialize<MembershipTableSnapshot>(resolvedEvent.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloAlive) => JsonSerializer.Deserialize<SiloAlive>(resolvedEvent.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloAdded) => JsonSerializer.Deserialize<SiloAdded>(resolvedEvent.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloStateUpdated) => JsonSerializer.Deserialize<SiloStateUpdated>(resolvedEvent.Event.Data.Span, jsonSerializerOptions),
                _ => null,
            };

            if (deserializedEvent is null)
            {
                throw new InvalidOperationException($"Event type {resolvedEvent.Event.EventType} could not be deserialized");
            }

            view = deserializedEvent.Apply(view) with
            {
                ETag = StreamState.StreamRevision(resolvedEvent.Event.EventNumber.ToUInt64()).ToString()
            };
        }

        return view;
    }

    internal async Task<MembershipView> Write<T>(MembershipView from, IEnumerable<T> writeEvents, CancellationToken cancellationToken)
        where T : EventBase
    {

        var newEvents = writeEvents.ToList();
        var singleEventType  = newEvents.Count == 1 ? newEvents[0].GetType() : null;

        // IAmAlive does not require a concurrency check - so we can just append it to the stream without checking the expected stream state.
        StreamState expectedState = singleEventType?.IsAssignableTo(typeof(SiloAlive)) ?? false ? StreamState.Any : ToStreamState(from.ETag);
 
        var writeResult = await client.ConditionalAppendToStreamAsync(streamName, expectedState, newEvents.Select(x=> new EventData(Uuid.NewUuid(), x.GetType().Name, JsonSerializer.SerializeToUtf8Bytes(x, x.GetType(), jsonSerializerOptions))), cancellationToken)
                                      .ConfigureAwait(false);

        if (writeResult.Status != ConditionalWriteStatus.Succeeded)
        {
            return from;
        }

        if (singleEventType?.IsAssignableTo(typeof(MembershipTableSnapshot)) ?? false && writeResult.NextExpectedVersion > 0)
        {
            // we can truncate all events in the stream prior to a full snapshot.
            // There is some risk of trampling writes on the truncation point this would mean we might end up retaining previous events longer than needed,
            // but that would not be a correctness issue
            _ = await client.SetStreamMetadata(streamName, StreamState.Any, new StreamMetadata(truncateBefore: StreamPosition.FromInt64(writeResult.NextExpectedVersion)), cancellationToken).ConfigureAwait(false);
        }

        foreach (var evt in newEvents)
        {
            from = evt.Apply(from) with { ETag = writeResult.NextExpectedStreamState.ToString() };
        }

        return from;
    }

    internal async Task<MembershipView> Write(MembershipView from, SiloAlive writeEvent, CancellationToken cancellationToken)
    {
        var writeResult = await client.ConditionalAppendToStreamAsync(streamName, StreamState.Any, [new EventData(Uuid.NewUuid(), nameof(SiloAlive), JsonSerializer.SerializeToUtf8Bytes(writeEvent, jsonSerializerOptions))], cancellationToken)
                                      .ConfigureAwait(false);

        if (writeResult.Status != ConditionalWriteStatus.Succeeded)
        {
            return from;
        }

        from = writeEvent.Apply(from) with { ETag = writeResult.NextExpectedStreamState.ToString() };

        return from;
    }

    internal async Task<MembershipView> Write(MembershipView from, MembershipTableSnapshot writeEvent, CancellationToken cancellationToken)
    {
        var writeResult = await client.ConditionalAppendToStreamAsync(streamName, StreamState.Any, [new EventData(Uuid.NewUuid(), nameof(MembershipTableSnapshot), JsonSerializer.SerializeToUtf8Bytes(writeEvent, jsonSerializerOptions))], cancellationToken)
                                      .ConfigureAwait(false);

        if (writeResult.Status != ConditionalWriteStatus.Succeeded)
        {
            return from;
        }

        // we can truncate all events in the stream prior to a full snapshot.
        // There is some risk of trampling writes into the stream metadata this would mean we might end up retaining previous events longer than needed,
        // but that would not be a correctness issue
        _ = await client.SetStreamMetadata(streamName, StreamState.Any, new StreamMetadata(truncateBefore: StreamPosition.FromInt64(writeResult.NextExpectedVersion)), cancellationToken).ConfigureAwait(false);

        from = writeEvent.Apply(from) with { ETag = writeResult.NextExpectedStreamState.ToString() };

        return from;
    }


    internal async Task<MembershipView> Delete(MembershipView view, string clusterId)
    {
        if (clusterOptions.Value.ClusterId != clusterId)
        { 
            return view;
        }

        _ = await client.DeleteStreamAsync(streamName, StreamState.Any, CancellationToken.None)
                                           .ConfigureAwait(true);

        return InitialView;
    }
}
