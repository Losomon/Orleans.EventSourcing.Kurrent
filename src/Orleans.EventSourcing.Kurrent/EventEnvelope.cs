namespace Orleans.EventSourcing.Kurrent;

/// <summary>
///     An envelope that contains an EventId and metadata which may be 
///     promoted to EventId and Metadata specific storage in the underlying event sourcing provider 
/// </summary>
/// <param name="eventId"></param>
/// <param name="metadata"></param>
[GenerateSerializer]
[Alias("Orleans.EventSourcing.EventEnvelope")]
[Immutable]
public abstract class EventEnvelope(Guid eventId, IDictionary<string, string>? metadata = null)
{
    /// <summary>
    ///     The unique eventId for this event, which will be used for idempotency/de-duplication.
    /// </summary>
    [Id(0)]
    public Guid EventId { get; } = eventId;

    /// <summary>
    ///     An optional dictionary of metadata that will be stored alongside the event.
    /// </summary>
    [Id(1)]
    public IDictionary<string, string>? Metadata { get; } = metadata;

    public override bool Equals(object? obj) => obj is EventEnvelope other && other.EventId == this.EventId;

    public override int GetHashCode() => EventId.GetHashCode();
}

/// <summary>
///     An envelope for <typeparamref name="TEvent"/> that contains an EventId and metadata which may be 
///     promoted to EventId and Metadata specific storage in the underlying event sourcing provider 
///     <para>
///         The Kurrent provider for Orleans supports this.
///     </para>
/// </summary>
/// <typeparam name="TEvent">The event sourcing event</typeparam>    
// This type needs to be serializable for event sourcing providers that do not natively understand this type
// such as the built-in Orleans providers used for unit testing.
[GenerateSerializer]
[Alias("Events.EventEnvelope`1")]
[Immutable]
public sealed class EventEnvelope<TEvent>(Guid eventId, TEvent @event, IDictionary<string, string>? metadata = null) : EventEnvelope(eventId, metadata), IEquatable<EventEnvelope<TEvent>>
    where TEvent : class

{        /// <summary>
         ///    The event payload itself
         /// </summary>
    [Id(0)]
    public TEvent Event { get; } = @event;

    public bool Equals(EventEnvelope<TEvent>? other) => other is { } x && base.Equals(other);

    public override bool Equals(object? obj) => Equals(obj as EventEnvelope<TEvent>);

    public override int GetHashCode() => base.GetHashCode();
}
