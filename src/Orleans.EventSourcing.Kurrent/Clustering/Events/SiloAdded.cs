namespace Orleans.EventSourcing.Kurrent.Clustering.Events;

internal sealed record SiloAdded(ImmutableMembership Membership, int TableVersion) : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view.Apply(this);
}