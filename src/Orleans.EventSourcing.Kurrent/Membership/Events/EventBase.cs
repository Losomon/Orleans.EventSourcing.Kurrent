namespace Orleans.EventSourcing.Kurrent.Membership.Events;

abstract record EventBase
{
    internal abstract MembershipView Apply(MembershipView view);
}
