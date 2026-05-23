using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.EventSourcing.Kurrent;

/// <inheritdoc/>
internal sealed class GlobalEventLogPositionConverter : JsonConverter<GlobalEventLogPosition>
{
    /// <inheritdoc/>
    public override GlobalEventLogPosition Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => new(reader.GetUInt64());

    /// <inheritdoc/>
    public override void Write(Utf8JsonWriter writer, GlobalEventLogPosition value, JsonSerializerOptions options) => writer.WriteNumberValue(value.Value);

    /// <inheritdoc/>
    public override GlobalEventLogPosition ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => new(ulong.Parse(reader.GetString()!, NumberStyles.None, CultureInfo.InvariantCulture));

    /// <inheritdoc/>
    public override void WriteAsPropertyName(Utf8JsonWriter writer, [DisallowNull] GlobalEventLogPosition value, JsonSerializerOptions options) => writer.WritePropertyName(value.Value.ToString("D", CultureInfo.InvariantCulture));
}
