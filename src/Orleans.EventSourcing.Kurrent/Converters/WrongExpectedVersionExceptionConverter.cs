using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Converters;

[GenerateSerializer, Alias("Orleans.EventSourcing.Kurrent.Converters.WrongExpectedVersionException"), Immutable]
internal readonly record struct WrongExpectedVersionExceptionSurrogate(string StreamName, StreamState ExpectedStreamState, StreamState ActualStreamState, string Message);

[RegisterConverter]
internal sealed class WrongExpectedVersionExceptionConverter : IConverter<WrongExpectedVersionException, WrongExpectedVersionExceptionSurrogate>
{
    public WrongExpectedVersionException ConvertFromSurrogate(in WrongExpectedVersionExceptionSurrogate surrogate) => new(surrogate.StreamName, surrogate.ExpectedStreamState, surrogate.ActualStreamState, null, surrogate.Message);

    public WrongExpectedVersionExceptionSurrogate ConvertToSurrogate(in WrongExpectedVersionException value) => new(value.StreamName, value.ExpectedStreamState, value.ActualStreamState, value.Message);
}
