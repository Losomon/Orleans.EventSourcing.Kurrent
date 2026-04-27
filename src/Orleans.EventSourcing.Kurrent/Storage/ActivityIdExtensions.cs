using System.Buffers;
using System.Diagnostics;
using System.Text.Json;

namespace Orleans.EventSourcing.Kurrent.Storage;

internal static class ActivityIdExtensions
{
    public static ReadOnlyMemory<byte>? ToKurrentMetadata(this Activity activity)
    {
        ArgumentNullException.ThrowIfNull(activity);

        ArrayBufferWriter<byte> byteWriter = new();
        using (Utf8JsonWriter writer = new(byteWriter))
        {
            // Writing any metadata seems to trigger the eventstore c# client to also write "$traceId" and "$spanId" automatically
            writer.WriteStartObject();
            writer.WritePropertyName("$correlationId");
            writer.WriteStringValue(activity.Id);
            writer.WriteEndObject();
        }
        return byteWriter.WrittenMemory;
    }

}
