namespace Orleans.EventSourcing.Kurrent.Membership.Events;

sealed record MembershipUpdate(ImmutableMembership Membership, int TableVersion) : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view.Apply(this);
}