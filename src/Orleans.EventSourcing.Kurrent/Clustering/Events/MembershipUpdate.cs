namespace Orleans.EventSourcing.Kurrent.Clustering.Events;

sealed record MembershipUpdate(ImmutableMembership Membership, int TableVersion) : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view.Apply(this);
}