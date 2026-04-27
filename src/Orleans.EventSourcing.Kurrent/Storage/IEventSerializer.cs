using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Storage;

public interface IEventSerializer<TLogEntry>
{
    /// <summary>
    /// Convert the <typeparamref name="TLogEntry"/> to a <see cref="EventData" /> including any metadata and activity tracing required
    /// using the serializer configured in <see cref="Configuration.KurrentStorageOptions" />"/>
    /// </summary>
    /// <typeparam name="TLogEntry"></typeparam>
    /// <param name="entry"></param>
    /// <returns></returns>
    public EventData SerializeEvent(TLogEntry entry);

    /// <summary>
    /// Convert the <see cref="ResolvedEvent" /> to a <typeparamref name="TLogEntry" /> using the serializer configured in <see cref="Configuration.KurrentStorageOptions" />"/>
    /// </summary>
    /// <typeparam name="TLogEntry"></typeparam>
    /// <param name="evt"></param>
    /// <returns></returns>
    public TLogEntry DeserializeEvent(ResolvedEvent evt);
}
