using Microsoft.CodeAnalysis;
using System.Collections.Immutable;

namespace Orleans.EventSourcing.Kurrent.Clustering;

sealed record ImmutableMembership(SiloAddress SiloAddress,
    SiloStatus Status, ImmutableDictionary<SiloAddress, DateTime> SuspectTimes,
    int ProxyPort,
    string HostName,
    string SiloName,
    string RoleName,
    int UpdateZone,
    int FaultZone,
    DateTime StartTime,
    DateTime IAmAliveTime,
    string ETag)
{
    public static ImmutableMembership FromMembershipEntry(MembershipEntry entry)
    => new(
        entry.SiloAddress,
        entry.Status,
        entry.SuspectTimes.ToImmutableDictionary(x=>x.Item1, x=>x.Item2),
        entry.ProxyPort,
        entry.HostName,
        entry.SiloName,
        entry.RoleName,
        entry.UpdateZone,
        entry.FaultZone,
        entry.StartTime,
        entry.IAmAliveTime,
        Guid.NewGuid().ToString());

    public Tuple<MembershipEntry, string> ToMembershipEntry()
        => new(new()
        {
            SiloAddress = SiloAddress,
            Status = Status,
            SuspectTimes = [.. SuspectTimes.Select(x => new Tuple<SiloAddress, DateTime>(x.Key, x.Value))],
            ProxyPort = ProxyPort,
            HostName = HostName,
            SiloName = SiloName,
            RoleName = RoleName,
            UpdateZone = UpdateZone,
            FaultZone = FaultZone,
            StartTime = StartTime,
            IAmAliveTime = IAmAliveTime
        }, ETag);
}
