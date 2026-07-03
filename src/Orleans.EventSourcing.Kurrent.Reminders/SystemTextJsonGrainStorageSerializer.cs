using Orleans.Storage;
using System.Text.Json;

namespace Orleans.EventSourcing.Kurrent.Reminders;
internal sealed class SystemTextJsonGrainStorageSerializer(JsonSerializerOptions? options = null) : IGrainStorageSerializer
{
    public BinaryData Serialize<T>(T input)
        => BinaryData.FromObjectAsJson<T>(input, options);

    public T? Deserialize<T>(BinaryData input)
        => input.ToObjectFromJson<T>(options);
}
