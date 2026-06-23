using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Storage;

// This very thin wrapper around EventStoreClient allows unit test substitution
internal interface IKurrentClient : IDisposable, IAsyncDisposable
{
    public Task<IWriteResult> SetStreamMetadata(string streamName,
                                                 StreamState expectedRevision,
                                                 StreamMetadata streamMetadata,
                                                 CancellationToken token);

    public Task<StreamMetadataResult> GetStreamMetadata(string streamName,
                                                        CancellationToken token);

    public Task<DeleteResult> DeleteStreamAsync(string streamName,
                                                StreamState expectedRevision,
                                                CancellationToken token);

    public Task<DeleteResult> TombstoneStreamAsync(string streamName,
                                                StreamState expectedRevision,
                                                CancellationToken token);

    public IAsyncEnumerable<ResolvedEvent> ReadStreamAsync(Direction direction,
                                                           string streamName,
                                                           StreamPosition position,
                                                           long maxCount,
                                                           bool resolveLinkTos,
                                                           CancellationToken cancellationToken);

    public Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName,
                                                                       StreamState expectedRevision,
                                                                       IEnumerable<EventData> eventData,
                                                                       CancellationToken cancellationToken);

    public IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start,
                                                               IEventFilter eventFilter,
                                                               uint checkpointInterval,
                                                               CancellationToken cancellationToken);

}
