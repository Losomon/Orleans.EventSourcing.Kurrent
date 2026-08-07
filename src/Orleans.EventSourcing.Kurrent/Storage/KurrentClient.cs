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

    public async ValueTask<IStreamReadResult> ReadStreamAsync(Direction direction,
                                                      string streamName,
                                                      StreamPosition position,
                                                      long maxCount,
                                                      bool resolveLinkTos,
                                                      CancellationToken cancellationToken)
    {
        var result = client.ReadStreamAsync(direction, streamName, position, maxCount, resolveLinkTos, cancellationToken: cancellationToken);
        if (await result.ReadState.ConfigureAwait(false) == ReadState.Ok)
        {
            return new EventResult(result.Messages);
        }
        else
        {
            return StreamDoesNotExistReadResult.Instance;
        }
    }

    private sealed class EventResult(IAsyncEnumerable<StreamMessage> messages) : IStreamReadResult
    {
        public StreamPosition? LastStreamPosition { get; private set; }

        public async IAsyncEnumerator<ResolvedEvent> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await foreach(var message in messages.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                switch (message)
                {
                    case StreamMessage.Event eventMessage:
                        yield return eventMessage.ResolvedEvent;
                        break;
                    case StreamMessage.LastStreamPosition lastStreamPositionMessage:
                        LastStreamPosition = lastStreamPositionMessage.StreamPosition;
                        break;
                }
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
