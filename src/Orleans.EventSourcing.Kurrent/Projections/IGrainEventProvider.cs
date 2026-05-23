using System.Diagnostics.CodeAnalysis;

namespace Orleans.EventSourcing.Kurrent.Projections;

/// <summary>
/// Provides a mechanism to subscribe to events emitted by grains, Orleans does not provide a built-in projections framework so this
/// interface and types used by it are designed to provide a provider-agnostic approach.
/// </summary>
/// <remarks>
/// This API is experimental and may change in non-major package releases. Acknowledge with
/// <c>#pragma warning disable OEK0001</c> or <c>&lt;NoWarn&gt;OEK0001&lt;/NoWarn&gt;</c> in your project.
/// </remarks>
[Experimental(diagnosticId: "OEK0001", UrlFormat = "https://github.com/OrleansContrib/Orleans.EventSourcing.Kurrent/blob/main/README.md#experimental-apis")]
public interface IGrainEventProvider // If we want non-grain events, we should have a different interface
{
    /// <summary>
    /// Subscribes to a stream of events for a given subscription, with the event payload included in <see cref="GrainEvent{TEventBase}"/>
    /// </summary>
    /// <typeparam name="TEventBase">The type of events to subscribe to.</typeparam>
    /// <param name="subscriber">The subscriber for observability purposes.</param>
    /// <param name="startingPosition">The position in the event stream to start from.</param>
    /// <param name="eventFilter">An array of event types to filter the subscription.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An asynchronous stream of <see cref="GrainEvent{TEventBase}"/>, <see cref="CaughtUp"/>, <see cref="FallenBehind"/> or <see cref="Checkpoint"/> objects.</returns>
    IAsyncEnumerable<EventStreamUpdate> SubscribeToGrainEvents<TEventBase>(GrainId subscriber, GlobalEventLogPosition startingPosition, Type[] eventFilter, CancellationToken cancellationToken) where TEventBase : notnull;


    /// <summary>
    /// Subscribes to a stream of event notifications for a given subscription, provided by <see cref="EventStreamUpdate"/> which does not include the event itself.
    /// </summary>
    /// <param name="subscriber">The subscriber for observability purposes.</param>
    /// <param name="startingPosition">The position in the event stream to start from.</param>
    /// <param name="eventFilter">An array of event types to filter the subscription.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An asynchronous stream of <see cref="EventStreamUpdate"/>, <see cref="CaughtUp"/>, <see cref="FallenBehind"/> or <see cref="Checkpoint"/> objects.</returns>
    IAsyncEnumerable<EventStreamUpdate> SubscribeToGrainEventNotifications(GrainId subscriber, GlobalEventLogPosition startingPosition, Type[] eventFilter, CancellationToken cancellationToken);


    /// <summary>
    /// Subscribes to a stream of event notifications for a given grain type, provided by <see cref="GrainEventNotification"/> which does not include the event payload.
    /// </summary>
    /// <typeparam name="TGrain">The type of the grain whose events to subscribe to.</typeparam>
    /// <param name="subscriber">The subscriber for observability purposes.</param>
    /// <param name="startingPosition">The position in the event stream to start from.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An asynchronous stream of <see cref="GrainEventNotification"/>, <see cref="CaughtUp"/>, <see cref="FallenBehind"/> or <see cref="Checkpoint"/> objects.</returns>
    IAsyncEnumerable<EventStreamUpdate> SubscribeToGrainEventNotifications<TGrain>(GrainId subscriber, GlobalEventLogPosition startingPosition, CancellationToken cancellationToken) where TGrain : IGrain;

    /// <summary>
    /// Subscribes to all events from <typeparamref name="TGrain"/> with the event payload included in <see cref="GrainEvent{TEventBase}"/>.
    /// </summary>
    /// <typeparam name="TGrain">The type of the grain whose events to subscribe to.</typeparam>
    /// <typeparam name="TEventBase">The base type of events to subscribe to.</typeparam>
    /// <param name="subscriber">The subscriber for observability purposes.</param>
    /// <param name="startingPosition">The position in the event stream to start from.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An asynchronous stream of <see cref="GrainEvent{TEventBase}"/>, <see cref="CaughtUp"/>, <see cref="FallenBehind"/> or <see cref="Checkpoint"/> objects.</returns>
    IAsyncEnumerable<EventStreamUpdate> SubscribeToGrainEvents<TGrain, TEventBase>(GrainId subscriber, GlobalEventLogPosition startingPosition, CancellationToken cancellationToken) where TGrain : IGrain where TEventBase : notnull;
}
