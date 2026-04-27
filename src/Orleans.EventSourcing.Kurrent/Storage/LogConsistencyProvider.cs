using Orleans.EventSourcing;
using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     A log-consistency provider that stores the latest view in primary storage, using any standard storage provider.
///     Supports multiple clusters connecting to the same primary storage (doing optimistic concurrency control via e-tags)
///     <para>
///         The log itself is actually saved to storage - the latest view (snapshot) and metadata (the log position, and write flags)
///         and all log entries are stored in the primary.
///     </para>
/// </summary>
internal sealed class LogConsistencyProvider : ILogViewAdaptorFactory
{
    private readonly IEventSerializerFactory eventSerializer;
    private readonly IKurrentClient client;
    private readonly KurrentStorageOptions options;

    /// <summary>
    ///     Initializes a new instance of LogConsistencyProvider class
    /// </summary>
    /// <param name="eventSerializer"></param>
    /// <param name="deepCopier"></param>
    /// <param name="snapshotPolicy"></param>
    internal LogConsistencyProvider(IEventSerializerFactory eventSerializer, IKurrentClient client, KurrentStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(eventSerializer);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);

        this.eventSerializer = eventSerializer;
        this.client = client;
        this.options = options;
    }

    /// <inheritdoc />
    public ILogViewAdaptor<TLogView, TLogEntry> MakeLogViewAdaptor<TLogView, TLogEntry>(ILogViewAdaptorHost<TLogView, TLogEntry> hostGrain, TLogView initialState, string grainTypeName, IGrainStorage? grainStorage, ILogConsistencyProtocolServices services)
        where TLogView : class, new()
        where TLogEntry : class
    {
        ArgumentNullException.ThrowIfNull(services);
        return new KurrentLogViewAdapter<TLogView, TLogEntry>(hostGrain, client, eventSerializer.GetEventSerializer<TLogEntry>(), options, services, options.StreamNameProvider);
    }

    /// <inheritdoc />
    public bool UsesStorageProvider => false;
}
