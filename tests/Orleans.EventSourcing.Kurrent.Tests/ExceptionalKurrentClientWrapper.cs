using System.Collections.Concurrent;

using KurrentDB.Client;

using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

internal sealed class ExceptionalKurrentClientWrapper(IKurrentClient passthrough) : IKurrentClient
{
    readonly ConcurrentDictionary<string, Exception> dictionaryExceptionToThrowOnceForTesting = new();

    internal void AddExceptionToThrowOnceForTesting(string streamName, Exception exception) => dictionaryExceptionToThrowOnceForTesting.AddOrUpdate(streamName, exception, (k, v) => v = exception);

    private void ThrowIfException(string streamName)
    {
        if (dictionaryExceptionToThrowOnceForTesting.Remove(streamName, out var ex))
        {
            throw ex;
        }
    }

    #region IKurrentClient
    public Task<ConditionalWriteResult> ConditionalAppendToStreamAsync(string streamName, StreamState expectedRevision, IEnumerable<EventData> eventData, CancellationToken cancellationToken)
    {
        ThrowIfException(streamName);
        return passthrough.ConditionalAppendToStreamAsync(streamName, expectedRevision, eventData, cancellationToken);
    }

    public Task<DeleteResult> DeleteStreamAsync(string streamName, StreamState expectedRevision, CancellationToken token)
    {
        ThrowIfException(streamName);
        return passthrough.DeleteStreamAsync(streamName, expectedRevision, token);
    }

    public void Dispose() => passthrough.Dispose();

    public ValueTask DisposeAsync() => passthrough.DisposeAsync();

    public Task<StreamMetadataResult> GetStreamMetadata(string streamName, CancellationToken token)
    {
        ThrowIfException(streamName);
        return passthrough.GetStreamMetadata(streamName, token);
    }

    public IAsyncEnumerable<ResolvedEvent> ReadStreamAsync(Direction direction, string streamName, StreamPosition position, long maxCount, bool resolveLinkTos, CancellationToken cancellationToken)
    {
        ThrowIfException(streamName);
        return passthrough.ReadStreamAsync(direction, streamName, position, maxCount, resolveLinkTos, cancellationToken);
    }

    public Task<IWriteResult> SetStreamMetadata(string streamName, StreamState expectedRevision, StreamMetadata streamMetadata, CancellationToken token)
    {
        ThrowIfException(streamName);
        return passthrough.SetStreamMetadata(streamName, expectedRevision, streamMetadata, token);
    }

    public IAsyncEnumerable<StreamMessage> CatchUpSubscription(FromAll start, IEventFilter eventFilter, uint checkpointInterval, CancellationToken cancellationToken)
        => passthrough.CatchUpSubscription(start, eventFilter, checkpointInterval, cancellationToken);

    #endregion
}
