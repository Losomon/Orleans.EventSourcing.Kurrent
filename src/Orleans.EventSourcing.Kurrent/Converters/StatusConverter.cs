using Grpc.Core;

namespace Orleans.EventSourcing.Kurrent.Converters;

[GenerateSerializer, Alias("Orleans.EventSourcing.Kurrent.Converters.StatusSurrogate"), Immutable]
internal readonly record struct StatusSurrogate(StatusCode StatusCode, string Detail);

[RegisterConverter]
internal sealed class StatusConverter : IConverter<Status, StatusSurrogate>
{
    public Status ConvertFromSurrogate(in StatusSurrogate surrogate) => new(surrogate.StatusCode, surrogate.Detail);

    public StatusSurrogate ConvertToSurrogate(in Status value) => new(value.StatusCode, value.Detail);
}
