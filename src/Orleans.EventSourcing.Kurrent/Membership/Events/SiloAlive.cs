namespace Orleans.EventSourcing.Kurrent.Membership.Events;

sealed record SiloAlive(SiloAddress SiloAddress, DateTime IAmAliveTime) : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view.Apply(this);
}
