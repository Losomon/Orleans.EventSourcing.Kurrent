using KurrentDB.Client;

using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

internal static class IKurrentExtensions
{
    public static async Task DuplicateLastEventInStream(this IKurrentClient client, string streamName)
    {
        var lastEvent = await client.ReadStreamAsync(Direction.Backwards, streamName, StreamPosition.End, 1, false, CancellationToken.None).SingleAsync();
        var result = await client.ConditionalAppendToStreamAsync(streamName, StreamState.StreamRevision(lastEvent.Event.EventNumber), [new EventData(Uuid.NewUuid(), lastEvent.Event.EventType, lastEvent.Event.Data, lastEvent.Event.Metadata)], CancellationToken.None);
        if (result.Status != ConditionalWriteStatus.Succeeded)
        {
            throw new InvalidOperationException(result.Status.ToString());
        }
    }
}
