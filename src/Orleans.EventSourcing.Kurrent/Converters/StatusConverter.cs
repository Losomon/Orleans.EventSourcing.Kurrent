using Grpc.Core;

namespace Orleans.EventSourcing.Kurrent.Converters;

[GenerateSerializer, Alias("Orleans.EventSourcing.Kurrent.Converters.StatusSurrogate"), Immutable]
internal readonly record struct StatusSurrogate(StatusCode StatusCode, string Detail);

[RegisterConverter]
internal sealed class StatusConverter : IConverter<Status, StatusSurrogate>
{
    public Status ConvertFromSurrogate(in StatusSurrogate surrogate)
    {
        return new Status(surrogate.StatusCode, surrogate.Detail);
    }

    public StatusSurrogate ConvertToSurrogate(in Status value)
    {
        return new StatusSurrogate(value.StatusCode, value.Detail);
    }
}
