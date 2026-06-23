using System.Runtime.CompilerServices;

using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Storage;

internal sealed class KurrentClient(KurrentDBClientSettings settings) : IKurrentClient
{
    private readonly KurrentDBClient client = new(settings);

    public Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata streamMetadata, CancellationToken token)
        => client.SetStreamMetadataAsync(streamName,
                                          expectedRevision,
                                          streamMetadata,
                                          cancellationToken: token);

    public Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
        => client.GetStreamMetadataAsync(streamName, cancellationToken: token);


    public Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
        => client.DeleteAsync(streamName,
                               expectedRevision,
                               cancellationToken: token);

    public async IAsyncEnumerable<ResolvedEvent> ReadStreamAsync(Direction direction,
                                                                 string streamName,
                                                                 StreamPosition position,
                                                                 long maxCount,
                                                                 bool resolveLinkTos,
                                                                 [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var events = client.ReadStreamAsync(direction, streamName, position, maxCount, resolveLinkTos, cancellationToken: cancellationToken);

        var readState = await events.ReadState.ConfigureAwait(false);

        if (readState == ReadState.Ok)
        {
            await foreach (var e in events.ConfigureAwait(false))
            {
                yield return e;
            }
        }
    }

    public Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName,
                                                                       StreamState expectedRevision,
                                                                       IEnumerable<EventData> eventData,
                                                                       CancellationToken cancellationToken)
        => client.ConditionalAppendToStreamAsync(streamName, expectedRevision, eventData, cancellationToken: cancellationToken);

    public IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, CancellationToken cancellationToken)
        => client.SubscribeToAll(start, filterOptions: new SubscriptionFilterOptions(eventFilter, checkpointInterval), cancellationToken: cancellationToken).Messages;

    public Task<DeleteResult> TombstoneStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
        => client.TombstoneAsync(streamName, expectedRevision, cancellationToken: token);

    public void Dispose() => client.Dispose();

    public ValueTask DisposeAsync() => client.DisposeAsync();
}
