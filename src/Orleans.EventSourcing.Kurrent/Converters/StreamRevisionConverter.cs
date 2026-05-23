using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Converters;

[GenerateSerializer, Alias("Orleans.EventSourcing.Kurrent.Converters.StreamStateSurrogate"), Immutable]
internal readonly record struct StreamStateSurrogate(ulong Value);

[RegisterConverter]
internal sealed class StreamStateConverter : IConverter<StreamState, StreamStateSurrogate>
{
    public StreamState ConvertFromSurrogate(in StreamStateSurrogate surrogate) => StreamState.StreamRevision(surrogate.Value);
    public StreamStateSurrogate ConvertToSurrogate(in StreamState value) => new((ulong)value.ToInt64());
}
