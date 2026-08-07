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

    public ValueTask<IStreamReadResult> ReadStreamAsync(Direction direction,
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

/// <summary>
/// Read result from the Kurrent client
/// </summary>
internal interface IStreamReadResult : IAsyncEnumerable<ResolvedEvent>
{
    /// <summary>
    /// The expected position of the stream for the next write operation. Only populated after enumeration.
    /// </summary>
    public StreamPosition? LastStreamPosition { get; }
}


/// <summary>
/// Empty read result - stream does not exist
/// </summary>
internal sealed class StreamDoesNotExistReadResult : IStreamReadResult
{
    private StreamDoesNotExistReadResult() { }
    /// <summary>
    /// Base class for read results from the Kurrent client
    /// </summary>
    internal static IStreamReadResult Instance { get; } = new StreamDoesNotExistReadResult();

    /// <inheritdoc />
    public StreamPosition? LastStreamPosition => null;
    
    /// <inheritdoc />
    public IAsyncEnumerator<ResolvedEvent> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    => AsyncEnumerable.Empty<ResolvedEvent>().GetAsyncEnumerator(cancellationToken);
}
