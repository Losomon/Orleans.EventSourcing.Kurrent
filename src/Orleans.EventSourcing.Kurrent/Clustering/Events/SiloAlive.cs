namespace Orleans.EventSourcing.Kurrent.Clustering.Events;

sealed record SiloAlive(SiloAddress SiloAddress, DateTime IAmAliveTime) : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view.Apply(this);
}
