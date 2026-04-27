using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Converters;

[GenerateSerializer, Alias("Orleans.EventSourcing.Kurrent.Converters.StreamDeletedExceptionSurrogate"), Immutable]
internal readonly record struct StreamDeletedExceptionSurrogate(string Stream);

[RegisterConverter]
internal sealed class StreamDeletedExceptionConverter : IConverter<StreamDeletedException, StreamDeletedExceptionSurrogate>
{
    public StreamDeletedException ConvertFromSurrogate(in StreamDeletedExceptionSurrogate surrogate)
    {
        return new StreamDeletedException(surrogate.Stream);
    }

    public StreamDeletedExceptionSurrogate ConvertToSurrogate(in StreamDeletedException value)
    {
        return new(value.Stream);
    }
}
