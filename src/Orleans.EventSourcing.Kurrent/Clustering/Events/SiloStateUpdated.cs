using System.Collections.Immutable;

namespace Orleans.EventSourcing.Kurrent.Clustering.Events;

internal sealed record SiloStateUpdated(SiloAddress SiloAddress, SiloStatus Status, ImmutableDictionary<SiloAddress, DateTime> SuspectTimes, int TableVersion) : EventBase
{
    internal override MembershipView Apply(MembershipView view) => view.Apply(this);
}
