using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.EventSourcing.Kurrent.Clustering;

/// <summary>
///     A <see cref="JsonConverter{T}" /> for <see cref="SiloAddress" /> that supports serializing a
///     <see cref="SiloAddress" /> both as a value and as a JSON dictionary key, using its parsable string form.
/// </summary>
internal sealed class SiloAddressJsonConverter : JsonConverter<SiloAddress>
{
    /// <inheritdoc/>
    public override SiloAddress Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => reader.GetString() is { } str ? SiloAddress.FromParsableString(str) : SiloAddress.Zero;

    /// <inheritdoc/>
    public override void Write(Utf8JsonWriter writer, SiloAddress value, JsonSerializerOptions options)
        => writer.WriteStringValue(value.ToParsableString());

    /// <inheritdoc/>
    public override SiloAddress ReadAsPropertyName(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => reader.GetString() is { } str ? SiloAddress.FromParsableString(str) : SiloAddress.Zero;

    /// <inheritdoc/>
    public override void WriteAsPropertyName(Utf8JsonWriter writer, [DisallowNull] SiloAddress value, JsonSerializerOptions options)
        => writer.WritePropertyName(value.ToParsableString());
}
