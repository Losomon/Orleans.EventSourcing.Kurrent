using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Converters;

[GenerateSerializer, Alias("Orleans.EventSourcing.Kurrent.Converters.StreamStateSurrogate"), Immutable]
public readonly record struct StreamStateSurrogate(ulong Value);

[RegisterConverter]
internal sealed class StreamStateConverter : IConverter<StreamState, StreamStateSurrogate>
{
    public StreamState ConvertFromSurrogate(in StreamStateSurrogate surrogate)
    {
        return StreamState.StreamRevision(surrogate.Value);
    }
    public StreamStateSurrogate ConvertToSurrogate(in StreamState value)
    {
        return new StreamStateSurrogate((ulong)value.ToInt64());
    }
}
