namespace Orleans.EventSourcing.Kurrent.Clustering.Events;

internal sealed record InitializationCanary : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view; // No-op    
}
