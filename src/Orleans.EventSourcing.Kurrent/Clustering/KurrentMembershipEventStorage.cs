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
    private const string NoStreamEtag = "None";

    internal MembershipView InitialView { get; } = new() { ETag = NoStreamEtag };

    private static StreamState ToStreamState(string etag)
        => etag == NoStreamEtag
            ? StreamState.NoStream
            : StreamState.StreamRevision(ulong.Parse(etag, CultureInfo.InvariantCulture));

    private static StreamPosition GetReadStreamPosition(string etag)
    {
        var expectedStreamState = ToStreamState(etag);
        return expectedStreamState == StreamState.NoStream ? StreamPosition.Start : StreamPosition.FromInt64(expectedStreamState.ToInt64()).Next();
    }
    
    internal async Task<MembershipView> RefreshState(MembershipView view, CancellationToken cancellationToken)
    {
        var readResult = await client.ReadStreamAsync(Direction.Forwards, streamName, GetReadStreamPosition(view.ETag), long.MaxValue, false, cancellationToken).ConfigureAwait(false);

        await foreach (var message in readResult.ConfigureAwait(false))
        {
            EventBase? deserializedEvent = message.Event.EventType switch
            {
                nameof(MembershipTableSnapshot) => JsonSerializer.Deserialize<MembershipTableSnapshot>(message.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloAlive) => JsonSerializer.Deserialize<SiloAlive>(message.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloAdded) => JsonSerializer.Deserialize<SiloAdded>(message.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloDefunct) => JsonSerializer.Deserialize<SiloDefunct>(message.Event.Data.Span, jsonSerializerOptions),
                nameof(SiloStateUpdated) => JsonSerializer.Deserialize<SiloStateUpdated>(message.Event.Data.Span, jsonSerializerOptions),
                _ => null,
            };

            if (deserializedEvent is null)
            {
                throw new InvalidOperationException($"Event type {message.Event.EventType} could not be deserialized");
            }

            view = deserializedEvent.Apply(view) with { ETag = message.Event.EventNumber.ToInt64().ToString(CultureInfo.InvariantCulture) };
        }

        if (readResult.LastStreamPosition.HasValue)
        {
            return view with { ETag = readResult.LastStreamPosition.Value.ToInt64().ToString(CultureInfo.InvariantCulture) };
        }
        else
        {
            return view;
        }
    }

    private async Task<MembershipView> WriteSnapshot(MembershipView from, EventBase[] newEvents, CancellationToken cancellationToken)
    {
        MembershipView updatedView = from;

        // Incorporate the new events into the updated view
        foreach (var evt in newEvents)
        {
            updatedView = evt.Apply(updatedView);
        }

        // Write a snapshot of the updated view to the stream, using the ETag for optimistic concurrency
        if (await client.ConditionalAppendToStreamAsync(streamName,
                                                        ToStreamState(updatedView.ETag),
                                                        [new EventData(Uuid.NewUuid(), nameof(MembershipTableSnapshot), JsonSerializer.SerializeToUtf8Bytes<MembershipTableSnapshot>(updatedView.GetSnapshot(), jsonSerializerOptions))],
                                                        cancellationToken)
                        .ConfigureAwait(false) is { Status: ConditionalWriteStatus.Succeeded } success)
        {

            // Write the truncation point for the snapshot to the stream, using the ETag for optimistic concurrency            
            var truncationPoint = StreamPosition.FromInt64(success.NextExpectedVersion);

            while(true)
            {
                var readMetadata = await client.GetStreamMetadata(streamName, cancellationToken)
                                               .ConfigureAwait(false);
  

                if (readMetadata.Metadata.TruncateBefore >= truncationPoint)
                {
                    // Skip if another snapshot has been written after ours, and the truncation point has advanced beyond our event
                    break;
                }

                // Conditional metadata write, if the metadata revision has changed likely means the truncation point has changed
                try
                {
                    var writeMetadata = await client.SetStreamMetadata(streamName,
                                                                       readMetadata.MetastreamRevision.HasValue ? StreamState.StreamRevision(readMetadata.MetastreamRevision.Value) : StreamState.NoStream,
                                                                       new StreamMetadata(maxCount: readMetadata.Metadata.MaxCount,
                                                                                          maxAge: readMetadata.Metadata.MaxAge,
                                                                                          truncateBefore: truncationPoint,
                                                                                          cacheControl: readMetadata.Metadata.CacheControl,
                                                                                          acl: readMetadata.Metadata.Acl,
                                                                                          customMetadata: readMetadata.Metadata.CustomMetadata),
                                                                       cancellationToken)
                                                    .ConfigureAwait(false);

                    if (writeMetadata is WrongExpectedVersionResult)
                    {
                        continue; // try again
                    }
                }
                catch(WrongExpectedVersionException)
                {
                    continue; // try again
                }

                break; // Success
            }
        
            return updatedView with { ETag = success.NextExpectedStreamState.ToString() };
        }

        return from;
    }

    internal async Task<MembershipView> Write(MembershipView from, IEnumerable<EventBase> newEvents, CancellationToken cancellationToken)
    {
        if (newEvents.ToArray() is not { Length: >0 } writeEvents)
        {
            return from;
        }

        if (from.EventsSinceSnapshot + writeEvents.Length >= options.Value.EventCountBeforeSnapshot) // Have we reached the threshold for writing a full snapshot?
        {
            return await WriteSnapshot(from, writeEvents, cancellationToken)
                        .ConfigureAwait(false);
        }
      
        var expectedStreamState = writeEvents.All(x => x is SiloDefunct || x is SiloAlive) // These two events are dirty write safe
                                                       ? StreamState.Any 
                                                       : ToStreamState(from.ETag); // All other events require optimistic concurrency

        if (await client.ConditionalAppendToStreamAsync(streamName,
                                                        expectedStreamState,
                                                        writeEvents.Select(x=> new EventData(Uuid.NewUuid(), x.GetType().Name, JsonSerializer.SerializeToUtf8Bytes(x, x.GetType(), jsonSerializerOptions))),
                                                        cancellationToken)
                        .ConfigureAwait(false) is { Status: ConditionalWriteStatus.Succeeded } success)
        {
            foreach (var evt in writeEvents)
            {
                if (expectedStreamState == StreamState.Any)
                {
                    from = evt.Apply(from); // Do not update ETag as we may not have observed intermediate events because this write was unconditional
                }
                else
                {
                    from = evt.Apply(from) with { ETag = success.NextExpectedStreamState.ToString() };
                }
            }
        }       

        return from;
    }

    internal async Task<MembershipView> Delete(MembershipView view, string clusterId)
    {
        if (clusterOptions.Value.ClusterId != clusterId)
        { 
            return view;
        }

        _ = await client.DeleteStreamAsync(streamName, StreamState.Any, CancellationToken.None)
                        .ConfigureAwait(false);

        return InitialView;
    }
}