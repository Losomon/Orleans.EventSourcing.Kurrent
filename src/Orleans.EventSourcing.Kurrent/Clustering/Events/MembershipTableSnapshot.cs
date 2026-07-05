using System.Collections.Immutable;

namespace Orleans.EventSourcing.Kurrent.Clustering.Events;

sealed record MembershipTableSnapshot(ImmutableDictionary<SiloAddress, ImmutableMembership> Members, int TableVersion) : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view.Apply(this);
}
