using System.Collections.Immutable;
using Orleans.EventSourcing.Kurrent.Clustering.Events;

namespace Orleans.EventSourcing.Kurrent.Clustering;

// The materialized membership state as of a specific stream position.
// It carries both halves of the Orleans optimistic-concurrency token: the domain Version
// and the ETag (derived from the stream position by storage).
// The ETag is stored as an opaque string so this domain type stays free of any Kurrent client types;
// KurrentMembershipEventStorage owns the StreamState <-> ETag translation.
internal sealed record MembershipView
{
    public ImmutableDictionary<SiloAddress, ImmutableMembership> Members { get; init; } = ImmutableDictionary<SiloAddress, ImmutableMembership>.Empty;

    public int Version { get; init; }

    public string ETag { get; init; } = string.Empty;

    private TableVersion ToTableVersion() => new(Version, ETag);

    internal MembershipTableData ReadAll() => new([.. Members.Select(x => x.Value.ToMembershipEntry())], ToTableVersion());

    internal MembershipTableData ReadRow(SiloAddress key)
        => Members.TryGetValue(key, out var entry)
            ? new MembershipTableData(entry.ToMembershipEntry(), ToTableVersion())
            : new MembershipTableData(ToTableVersion());

    internal uint EventsSinceSnapshot { get; init; }

    internal MembershipView Apply(SiloAlive wasAliveEvent)
    => Members.TryGetValue(wasAliveEvent.SiloAddress, out var entry)
           ? this with
           {
               Members = Members.SetItem(wasAliveEvent.SiloAddress, entry with { IAmAliveTime = wasAliveEvent.IAmAliveTime}),
               EventsSinceSnapshot = EventsSinceSnapshot + 1
           }
           : this;

    // A snapshot is a full checkpoint: it replaces the members and version wholesale while keeping the current ETag,
    // which storage overwrites with the actual stream position after the read/write completes.
    internal MembershipView Apply(MembershipTableSnapshot snapshotEvent)
        => this with { Members = snapshotEvent.Members,
            Version = snapshotEvent.TableVersion,
            EventsSinceSnapshot = 0 };

    internal MembershipView Apply(SiloAdded siloAdded)
       => this with
       {
           Members = Members.Add(siloAdded.Membership.SiloAddress, siloAdded.Membership),
           Version = siloAdded.TableVersion,
           EventsSinceSnapshot = EventsSinceSnapshot + 1
       };

    internal MembershipView Apply(SiloStateUpdated siloUpdated)
     => this with
     {
         Members = Members.SetItem(siloUpdated.SiloAddress, Members[siloUpdated.SiloAddress] with { SuspectTimes = siloUpdated.SuspectTimes, Status = siloUpdated.Status, ETag = siloUpdated.ETag}),
         Version = siloUpdated.TableVersion,
         EventsSinceSnapshot = EventsSinceSnapshot + 1
     };

    internal IEnumerable<EventBase> UpdateRow(MembershipEntry entry, string etag, int tableVersion)
    {
        if (!Members.TryGetValue(entry.SiloAddress, out var existingEntry)
            || existingEntry.ETag != etag)
        {
            yield break;
        }

        yield return new SiloStateUpdated(entry.SiloAddress, entry.Status, Guid.NewGuid().ToString(), entry.SuspectTimes.ToImmutableDictionary(x => x.Item1, x => x.Item2), tableVersion);
    }

    internal IEnumerable<EventBase> InsertRow(MembershipEntry entry, int tableVersion)
    {
        if (Members.ContainsKey(entry.SiloAddress))
        {
            yield break;
        }

        yield return new SiloAdded(ImmutableMembership.FromMembershipEntry(entry), tableVersion);
    }

    internal IEnumerable<SiloAlive> UpdateIAmAlive(MembershipEntry updateEntry)
    {
        if (!Members.TryGetValue(updateEntry.SiloAddress, out var entry)
            || entry.IAmAliveTime > updateEntry.IAmAliveTime)
        {
            yield break;
        }

        yield return new SiloAlive(entry.SiloAddress, updateEntry.IAmAliveTime);
    }

    internal IEnumerable<EventBase> CleanUpDefunctEntries(DateTimeOffset beforeDate)
    {  
        foreach (var item in Members)
        {
            if (item.Value.Status != SiloStatus.Active &&
                new DateTime(Math.Max(item.Value.IAmAliveTime.Ticks, item.Value.StartTime.Ticks), DateTimeKind.Utc) < beforeDate)
            {
                yield return new SiloDefunct(item.Key);
            }
        }  
    }

    internal MembershipView Apply(SiloDefunct siloDefunct)
    => this with
    {
        Members = Members.Remove(siloDefunct.SiloAddress),
        EventsSinceSnapshot = EventsSinceSnapshot + 1
    };

    internal MembershipTableSnapshot GetSnapshot()
        => new(Members, Version);
}
