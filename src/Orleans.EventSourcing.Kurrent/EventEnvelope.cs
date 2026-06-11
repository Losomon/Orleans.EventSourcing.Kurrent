using System.Diagnostics.CodeAnalysis;

namespace Orleans.EventSourcing;

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

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as EventEnvelope);

    /// <summary>
    /// Determines whether the given <see cref="EventEnvelope"/> is equal to this instance.
    /// </summary>
    /// <param name="other">The <see cref="EventEnvelope"/> to compare with this instance.</param>
    /// <returns><c>true</c> if the specified <see cref="EventEnvelope"/> is equal to this instance; otherwise, <c>false</c>.</returns>
    protected bool Equals(EventEnvelope? other)
    {
        if (other is null
            || other.EventId != this.EventId
            || other.Metadata?.Count != this.Metadata?.Count)
            return false;

        if (other.Metadata != null && this.Metadata != null)
        {
            foreach (var kvp in other.Metadata)
            {
                if (!this.Metadata.TryGetValue(kvp.Key, out var value)
                    || value != kvp.Value)
                    return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
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
[Alias("Orleans.EventSourcing.EventEnvelope`1")]
[Immutable]
public sealed class EventEnvelope<TEvent>(Guid eventId, TEvent @event, IDictionary<string, string>? metadata = null) : EventEnvelope(eventId, metadata), IEquatable<EventEnvelope<TEvent>>
    where TEvent : class

{        /// <summary>
         ///    The event payload itself
         /// </summary>
    [Id(0)]
    public TEvent Event { get; } = @event;

    /// <inheritdoc />
    public bool Equals(EventEnvelope<TEvent>? other)
    {
        if (!base.Equals(other))
            return false;

        return EqualityComparer<TEvent>.Default.Equals(this.Event, other?.Event);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as EventEnvelope<TEvent>);

    /// <inheritdoc />
    public override int GetHashCode() => base.GetHashCode();
}
