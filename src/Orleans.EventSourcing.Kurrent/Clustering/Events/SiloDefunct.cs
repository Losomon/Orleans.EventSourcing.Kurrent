namespace Orleans.EventSourcing.Kurrent.Clustering.Events;

internal sealed record SiloDefunct(SiloAddress SiloAddress) : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view.Apply(this);      
}
