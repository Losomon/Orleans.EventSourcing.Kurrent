using System.Diagnostics;

using KurrentDB.Client;

using Microsoft.Extensions.Options;

using Orleans.Serialization.TypeSystem;
using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Kurrent-based log consistent event serializer, which uses Kurrent's EventType to avoid encoding type information in the event data.
/// </summary>
internal sealed class DefaultEventConverter<TLogEntry> : IEventConverter<TLogEntry>
{
    private readonly IGrainStorageSerializer _storageSerializer;
    private readonly TypeConverter _typeConverter;

    public DefaultEventConverter(IOptions<KurrentStorageOptions> storageOptions, TypeConverter typeConverter)
    {
        ArgumentNullException.ThrowIfNull(storageOptions, nameof(storageOptions));
        ArgumentNullException.ThrowIfNull(typeConverter, nameof(typeConverter));
        _storageSerializer = storageOptions.Value.GrainStorageSerializer;
        _typeConverter = typeConverter;
    }

    /// <inheritdoc/>
    public EventData SerializeEvent(TLogEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var eventType = entry.GetType();
        var entryData = _storageSerializer.Serialize(eventType, entry);
        var eventUuid = Uuid.NewUuid();

        return new EventData(eventUuid, _typeConverter.Format(eventType), entryData.ToMemory(), Activity.Current?.ToKurrentMetadata(), entryData.MediaType ?? "application/octet-stream");
    }

    /// <inheritdoc/>
    public TLogEntry DeserializeEvent(ResolvedEvent evt)
     => (TLogEntry)_storageSerializer.Deserialize(_typeConverter.Parse(evt.Event.EventType), new BinaryData(evt.Event.Data, evt.Event.ContentType));
}
