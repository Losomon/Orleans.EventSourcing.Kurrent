using Orleans.EventSourcing;
using Orleans.EventSourcing.Kurrent;
using Orleans.EventSourcing.Kurrent.Projections;


namespace Orleans.EventSourcing.Kurrent.Projections;

/// <summary>
/// Represents an update in the event stream, such as a <see cref="GrainEvent{T}"/> <see cref="Checkpoint"/>, <see cref="CaughtUp"/>, or <see cref="FallenBehind"/> notification.
/// This is the base type for all event stream update notifications returned by event projection subscriptions.
/// </summary>
public abstract record EventStreamUpdate;

/// <summary>
/// Indicates that the event stream subscription has caught up to the latest available event.
/// Used to notify subscribers that no further historical events are pending.
/// </summary>
public sealed record CaughtUp : EventStreamUpdate
{
    private CaughtUp() { }

    internal static CaughtUp Instance { get; } = new();
}

/// <summary>
/// Indicates that the event stream subscription has fallen behind and is no longer up-to-date.
/// Used to notify subscribers that the stream is lagging and may require resynchronization.
/// </summary>
public sealed record FallenBehind : EventStreamUpdate
{
    private FallenBehind() { }

    internal static FallenBehind Instance { get; } = new();
}

/// <summary>
/// Provides a regular checkpoint in the event stream when processing through events not relevent to the subscriber
/// </summary>
/// <param name="Position"></param>
public record Checkpoint(GlobalEventLogPosition Position) : EventStreamUpdate;

/// <summary>
/// Provides a notification that a grain event occured
/// </summary>
/// <param name="Position"></param>
/// <param name="EventGrainId"></param>
/// <param name="EventGrainVersion"></param>
public record GrainEventNotification(GlobalEventLogPosition Position, GrainId EventGrainId, int EventGrainVersion)
    : Checkpoint(Position);


/// <summary>
/// Represents an event emitted by a grain in the event stream.
/// This record encapsulates the event data, its global position in the event log, the originating grain's identifier,
/// and the version of the grain at the time the event was produced.
/// </summary>
/// <typeparam name="TEventBase">The base type of the event payload.</typeparam>
/// <param name="Position">The global position of the event in the event log stream.</param>
/// <param name="Event">The event payload emitted by the grain.</param>
/// <param name="EventGrainId">The unique identifier of the grain that produced the event.</param>
/// <param name="EventGrainVersion">The version of the grain at the time the event was generated.</param>
public sealed record GrainEvent<TEventBase>(
    GlobalEventLogPosition Position,
    TEventBase Event,
    GrainId EventGrainId,
    int EventGrainVersion) : GrainEventNotification(Position, EventGrainId, EventGrainVersion);
