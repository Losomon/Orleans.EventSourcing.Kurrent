using Microsoft.Extensions.Options;
using Orleans.EventSourcing.Kurrent.Clustering.Events;
using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Clustering;

internal sealed class KurrentMembershipTable(IOptions<KurrentClusteringOptions> options, KurrentMembershipEventStorage storage) : IMembershipTable
{
    private volatile MembershipView current = storage.InitialView;

    public async Task DeleteMembershipTableEntries(string clusterId)
     => current = await storage.Delete(current, clusterId).ConfigureAwait(false);

    public Task InitializeMembershipTable(bool tryInitTableVersion) => Task.CompletedTask;

    public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
    => TryChange(tableVersion.VersionEtag, x => x.InsertRow(entry, tableVersion.Version), CancellationToken.None);

    public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
    => TryChange(tableVersion.VersionEtag, x => x.UpdateRow(entry, etag, tableVersion.Version), CancellationToken.None);

    private async Task<bool> TryChange(string tableVersionEtag, Func<MembershipView, IEnumerable<EventBase>> action, CancellationToken cancellationToken)
    {
        var refreshed = await storage.RefreshState(current).ConfigureAwait(false);
        current = refreshed;

        if (tableVersionEtag == refreshed.ETag &&
            action(refreshed) is { } emittedEvent)
        {
            var updated = await storage.Write(refreshed, emittedEvent, cancellationToken).ConfigureAwait(true);

            if (!ReferenceEquals(updated, refreshed))
            {
                current = updated;
                return true;
            }
        }

        return false;
    }

    public async Task<MembershipTableData> ReadAll()
    {
        var refreshed = await storage.RefreshState(current).ConfigureAwait(false);
        current = refreshed;
        return refreshed.ReadAll();
    }

    public async Task<MembershipTableData> ReadRow(SiloAddress key)
    {
        var refreshed = await storage.RefreshState(current).ConfigureAwait(false);
        current = refreshed;
        return refreshed.ReadRow(key);
    }

    public async Task UpdateIAmAlive(MembershipEntry entry)
    {
        var refreshed = await storage.RefreshState(current).ConfigureAwait(false);
        current = refreshed;

        if (refreshed.UpdateIAmAlive(entry) is { } emittedEvent)
        {
            current = await storage.Write(refreshed, emittedEvent, CancellationToken.None).ConfigureAwait(true);
        }
    }

    public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
    {
        var refreshed = await storage.RefreshState(current).ConfigureAwait(false);
        current = refreshed;

        if (refreshed.CleanUpDefunctEntries(beforeDate, options.Value.EventCountBeforeSnapshot) is { } emittedEvent)
        {
            current = await storage.Write(refreshed, emittedEvent, CancellationToken.None).ConfigureAwait(true);
        } 
    }
}
