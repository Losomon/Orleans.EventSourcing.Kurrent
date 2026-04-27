using Grpc.Core;

namespace Orleans.EventSourcing.Kurrent.Converters;

[GenerateSerializer, Alias("Orleans.EventSourcing.Kurrent.Converters.RpcExceptionSurrogate"), Immutable]

internal readonly record struct RpcExceptionSurrogate(Status Status);

[RegisterConverter]
internal sealed class RpcExceptionConverter : IConverter<RpcException, RpcExceptionSurrogate>
{
    public RpcException ConvertFromSurrogate(in RpcExceptionSurrogate surrogate)
    {
        return new RpcException(surrogate.Status);
    }

    public RpcExceptionSurrogate ConvertToSurrogate(in RpcException value)
    {
        return new RpcExceptionSurrogate(value.Status);
    }
}
