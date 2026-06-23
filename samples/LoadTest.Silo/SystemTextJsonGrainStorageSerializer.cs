using System.Text.Json;
using Orleans.Storage;

namespace LoadTest.Silo;

/// <summary>
///     System.Text.Json based <see cref="IGrainStorageSerializer"/>. The Kurrent provider
///     serializes/deserializes with the concrete event type (resolved from the Kurrent EventType),
///     so no polymorphic type discriminators are required.
/// </summary>
internal sealed class SystemTextJsonGrainStorageSerializer(JsonSerializerOptions? options = null) : IGrainStorageSerializer
{
    public BinaryData Serialize<T>(T input)
        => BinaryData.FromObjectAsJson<T>(input, options);

    public T Deserialize<T>(BinaryData input)
        => input.ToObjectFromJson<T>(options);
}
