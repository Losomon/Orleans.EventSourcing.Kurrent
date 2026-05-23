using Grpc.Core;

using KurrentDB.Client;

using Orleans.Storage;

namespace Orleans.EventSourcing.Kurrent.Storage;

internal static class KurrentExceptionConverter
{
    public static InconsistentStateException ConvertException(Exception exception) => exception switch
    {
        // Passthrough
        InconsistentStateException inconsistentStateException => inconsistentStateException,

        // Pass as wrapped Innerexception
        RpcException or TimeoutException or ArgumentNullException or InvalidOperationException or ArgumentOutOfRangeException => new InconsistentStateException($"Kurrent encountered a problem: ${exception.Message}", exception),
        WrongExpectedVersionException ex => new InconsistentStateException($"Version mismatch: ${exception.Message}", $"{ex.ActualVersion:N}", $"{ex.ExpectedVersion:N}", ex),
        StreamDeletedException ex => new InconsistentStateException($"Stream deleted: ${exception.Message}", ex),

        // Replaced by InconsistentStateException with message, as actual exception may not be serializable
        _ => new InconsistentStateException($"Kurrent encountered an exception {exception.Message}"),
    };
}
