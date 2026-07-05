using System.Collections.Immutable;
using Orleans.EventSourcing.Kurrent.Clustering.Events;

namespace Orleans.EventSourcing.Kurrent.Clustering;

// The materialized membership state as of a specific stream position.
// It carries both halves of the Orleans optimistic-concurrency token: the domain Version
// (persisted in the snapshot payload) and the ETag (derived from the stream position by storage).
// The ETag is stored as an opaque string so this domain type stays free of any Kurrent client types;
// KurrentMembershipStorage owns the StreamState <-> ETag translation.
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

    private uint EventsSinceSnapshot { get; init; }

    // Fold a liveness event into the view.
    internal MembershipView Apply(SiloAlive wasAliveEvent)
    {
        if (Members.TryGetValue(wasAliveEvent.SiloAddress, out var entry)
            && wasAliveEvent.IAmAliveTime > entry.IAmAliveTime)
        {
            return this with
            {
                Members = Members.SetItem(wasAliveEvent.SiloAddress, entry with { IAmAliveTime = wasAliveEvent.IAmAliveTime }),
                EventsSinceSnapshot = EventsSinceSnapshot + 1
            };
        }

        return this;
    }

    // A snapshot is a full checkpoint: it replaces the members and version wholesale while keeping the current ETag,
    // which storage overwrites with the actual stream position after the read/write completes.
    internal MembershipView Apply(MembershipTableSnapshot snapshotEvent)
        => this with { Members = snapshotEvent.Members, 
                       Version = snapshotEvent.TableVersion, 
                       EventsSinceSnapshot = 0 };

    // a single membership entry upsert
    internal MembershipView Apply(MembershipUpdate membershipUpdateEvent)
        => this with { Members = Members.SetItem(membershipUpdateEvent.Membership.SiloAddress, membershipUpdateEvent.Membership), 
                       Version = membershipUpdateEvent.TableVersion, 
                       EventsSinceSnapshot = EventsSinceSnapshot + 1 };

    internal EventBase? UpdateRow(MembershipEntry entry, string etag, int tableVersion)
    {
        if (!Members.TryGetValue(entry.SiloAddress, out var existingEntry)
            || existingEntry.ETag != etag)
        {
            return null;
        }

        return new MembershipUpdate(ImmutableMembership.FromMembershipEntry(entry), tableVersion);
    }

    internal EventBase? InsertRow(MembershipEntry entry, int tableVersion)
    {
        if (Members.ContainsKey(entry.SiloAddress))
        {
            return null;
        }

        return new MembershipUpdate(ImmutableMembership.FromMembershipEntry(entry), tableVersion);
    }

    internal EventBase? UpdateIAmAlive(MembershipEntry updateEntry, int eventsBeforeSnapshot)
    { 
        if (!Members.TryGetValue(updateEntry.SiloAddress, out var entry))
        {
            return null;
        }

        return new SiloAlive(entry.SiloAddress, updateEntry.IAmAliveTime); 
    }
    internal EventBase? CleanUpDefunctEntries(DateTimeOffset beforeDate, int eventsBeforeSnapshot)
    {
        var remaining = Members;

        foreach (var item in Members)
        {
            if (item.Value.Status != SiloStatus.Active && 
                new DateTime(Math.Max(item.Value.IAmAliveTime.Ticks, item.Value.StartTime.Ticks), DateTimeKind.Utc) < beforeDate)
            {
                remaining = remaining.Remove(item.Key);
            }
        }

        return ReferenceEquals(remaining, Members) && EventsSinceSnapshot < eventsBeforeSnapshot
            ? null
            : new MembershipTableSnapshot(remaining, Version);
    } 
}
