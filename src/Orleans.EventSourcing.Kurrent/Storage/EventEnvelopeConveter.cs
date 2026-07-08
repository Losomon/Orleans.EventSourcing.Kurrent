using System.Buffers;
using System.Diagnostics;
using System.Text.Json;

using KurrentDB.Client;

using Orleans.Serialization.TypeSystem;
using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Configuration;


namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Kurrent-based log consistent event serializer
/// </summary>
 internal sealed class EventEnvelopeConveter<TEvent> : IEventConverter<EventEnvelope<TEvent>>
    where TEvent : class
{
    private readonly IGrainStorageSerializer _storageSerializer;
    private readonly TypeConverter _typeConverter;


    public EventEnvelopeConveter(KurrentStorageOptions storageOptions, TypeConverter typeConverter)
    {
        ArgumentNullException.ThrowIfNull(storageOptions, nameof(storageOptions));
        ArgumentNullException.ThrowIfNull(typeConverter, nameof(typeConverter));
        _storageSerializer = storageOptions.GrainStorageSerializer;
        _typeConverter = typeConverter;
    }

    /// <inheritdoc/>
    public EventData SerializeEvent(EventEnvelope<TEvent> entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        ReadOnlyMemory<byte>? metadata = null;

        var eventType = entry.Event.GetType();
        var entryData = _storageSerializer.Serialize(eventType, entry.Event);
        var eventUuid = Uuid.FromGuid(entry.EventId);

        if (entry.Metadata is { Count: > 0 })
        {
            ArrayBufferWriter<byte> byteWriter = new();
            using (Utf8JsonWriter writer = new(byteWriter))
            {
                writer.WriteStartObject();

                foreach (var metadataEntry in entry.Metadata)
                {
                    writer.WritePropertyName(metadataEntry.Key);
                    writer.WriteStringValue(metadataEntry.Value);
                }

                if (Activity.Current is not null && !entry.Metadata.ContainsKey("$correlationId"))
                {
                    writer.WritePropertyName("$correlationId");
                    writer.WriteStringValue(Activity.Current.Id);
                }

                writer.WriteEndObject();
            }

            metadata = byteWriter.WrittenMemory;
        }

        return new EventData(eventUuid, _typeConverter.Format(eventType), entryData.ToMemory(), metadata ?? Activity.Current?.ToKurrentMetadata(), entryData.MediaType ?? "application/octet-stream");
    }

    /// <inheritdoc/>
    public EventEnvelope<TEvent> DeserializeEvent(ResolvedEvent evt)
    {
        Dictionary<string, string>? metadata = null;

        if (evt.Event.Metadata is { IsEmpty: false })
        {
            var jsonReader = new Utf8JsonReader(evt.Event.Metadata.Span);

            if (jsonReader.Read() && jsonReader.TokenType == JsonTokenType.StartObject)
            {
                while (jsonReader.Read())
                {
                    if (jsonReader.TokenType == JsonTokenType.EndObject)
                    {
                        break;
                    }

                    if (jsonReader.TokenType == JsonTokenType.PropertyName)
                    {
                        var propertyName = jsonReader.GetString();
                        if (propertyName is not null
                            && jsonReader.Read()
                            && jsonReader.TokenType == JsonTokenType.String
                            && jsonReader.GetString() is { } value)
                        {
                            metadata ??= [];
                            metadata.Add(propertyName, value);
                        }
                    }
                }
            }
        }

        var eventInstance = (TEvent)_storageSerializer.Deserialize(_typeConverter.Parse(evt.Event.EventType), new BinaryData(evt.Event.Data, evt.Event.ContentType));

        return new EventEnvelope<TEvent>(evt.Event.EventId.ToGuid(), eventInstance, metadata);
    }

}
