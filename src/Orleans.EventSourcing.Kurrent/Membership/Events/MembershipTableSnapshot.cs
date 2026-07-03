using System.Collections.Immutable;

namespace Orleans.EventSourcing.Kurrent.Membership.Events;

sealed record MembershipTableSnapshot(ImmutableDictionary<SiloAddress, ImmutableMembership> Members, int TableVersion) : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view.Apply(this);
}
