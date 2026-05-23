using KurrentDB.Client;

using Microsoft.Extensions.Logging;

namespace Orleans.EventSourcing.Kurrent;

internal static partial class Log
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Debug, Message = "Init: Name={Name}, initialized in {elapsedTime}")]
    public static partial void Initialized(this ILogger logger, string name, TimeSpan elapsedTime);

    [LoggerMessage(EventId = 2, Level = LogLevel.Error, Message = "Init: Name={Name}, errored in {elapsedTime}")]
    public static partial void InitializingError(this ILogger logger, string name, TimeSpan elapsedTime, Exception ex);

    [LoggerMessage(EventId = 3, Level = LogLevel.Error, Message = "Close: Name={Name}")]
    public static partial void CloseError(this ILogger logger, string name, Exception ex);

    [LoggerMessage(EventId = 4, Level = LogLevel.Error, Message = "Read: Name={Name} Failed to read log entries in stream {StreamName}")]
    public static partial void ReadError(this ILogger logger, string name, string streamName, Exception ex);

    [LoggerMessage(EventId = 5, Level = LogLevel.Error, Message = "ReadLast: Name={Name} Failed to read last log entry for stream {StreamName}")]
    public static partial void ReadLastError(this ILogger logger, string name, string streamName, Exception ex);

    [LoggerMessage(EventId = 6, Level = LogLevel.Error, Message = "Write: Name={Name} Failed to write log entries in stream {StreamName}")]
    public static partial void WriteError(this ILogger logger, string name, string streamName, Exception ex);

    [LoggerMessage(EventId = (int)ErrorCode.LogConsistency_UserCodeException, Level = LogLevel.Warning, Message = "{GrainId} exception caught in user code for {Callback}, called from {Location}")]
    public static partial void UserCodeException(this ILogger logger, GrainId grainId, string callback, string location, Exception ex);

    [LoggerMessage(EventId = (int)ErrorCode.LogConsistency_ProtocolFatalError, Level = LogLevel.Error, Message = "{GrainId} Protocol Error: {Message}")]
    public static partial void ProtcolFatalError(this ILogger logger, GrainId grainId, string message);

    [LoggerMessage(EventId = (int)ErrorCode.LogConsistency_ProtocolError, Level = LogLevel.Warning, Message = "{GrainId} Protocol Warning: {Message}")]
    public static partial void ProtocolError(this ILogger logger, GrainId grainId, string message);

    [LoggerMessage(EventId = (int)ErrorCode.LogConsistency_CaughtException, Level = LogLevel.Error, Message = "{GrainId} exception caught at {Location}")]
    public static partial void CaughtException(this ILogger logger, GrainId grainId, string location, Exception ex);

    [LoggerMessage(EventId = 7, Level = LogLevel.Warning, Message = "{subscription} subscription fell behind")]
    public static partial void SubscriptionFellBehind(this ILogger logger, GrainId subscription);

    [LoggerMessage(EventId = 8, Level = LogLevel.Information, Message = "{subscription} subscription caught-up")]
    public static partial void SubscriptionCaughtUp(this ILogger logger, GrainId subscription);

    [LoggerMessage(EventId = 9, Level = LogLevel.Debug, Message = "{subscription} {position} received notification {eventGrainId} {streamPosition} {eventReceived}")]
    public static partial void EventReceived(this ILogger logger, GrainId subscription, Position? position, GrainId eventGrainId, StreamPosition streamPosition, object? eventReceived);

    [LoggerMessage(EventId = 10, Level = LogLevel.Debug, Message = "{subscription} received checkpoint {checkpoint}")]
    public static partial void Checkpoint(this ILogger logger, GrainId subscription, Position checkpoint);

    [LoggerMessage(EventId = 11, Level = LogLevel.Information, Message = "{subscription} requesting events '{eventFilter}' from {position}")]
    public static partial void Subscribe(this ILogger logger, GrainId subscription, GlobalEventLogPosition position, IEventFilter eventFilter);

    [LoggerMessage(EventId = 12, Level = LogLevel.Debug, Message = "{subscription} {position} received notification {eventGrainId} {streamPosition}")]
    public static partial void EventNotificationReceived(this ILogger logger, GrainId subscription, Position? position, GrainId eventGrainId, StreamPosition streamPosition);
}
