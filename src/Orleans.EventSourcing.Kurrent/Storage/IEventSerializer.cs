using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
/// An interface for converting between <typeparamref name="TLogEntry"/>, <see cref="EventData"/> and <see cref="ResolvedEvent"/>.
/// </summary>
/// <typeparam name="TLogEntry">The type of the log entry.</typeparam>
public interface IEventSerializer<TLogEntry>
{
    /// <summary>
    /// Convert the <typeparamref name="TLogEntry"/> to a <see cref="EventData" /> including any metadata and activity tracing required
    /// using the serializer configured in <see cref="Configuration.KurrentStorageOptions" />"/>
    /// </summary>
    /// <param name="entry">The log entry to serialize.</param>
    /// <returns>The serialized event data.</returns>
    public EventData SerializeEvent(TLogEntry entry);

    /// <summary>
    /// Convert the <see cref="ResolvedEvent" /> to a <typeparamref name="TLogEntry" /> using the serializer configured in <see cref="Configuration.KurrentStorageOptions" />"/>
    /// </summary>
    /// <param name="evt">The resolved event to deserialize.</param>
    /// <returns>The deserialized log entry.</returns>
    public TLogEntry DeserializeEvent(ResolvedEvent evt);
}
