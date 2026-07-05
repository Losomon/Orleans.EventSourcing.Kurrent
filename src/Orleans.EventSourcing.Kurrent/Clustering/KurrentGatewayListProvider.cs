using Microsoft.CodeAnalysis;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Messaging;

namespace Orleans.EventSourcing.Kurrent.Clustering;

internal sealed class KurrentGatewayListProvider(KurrentMembershipTable table, IOptions<GatewayOptions> options) : IGatewayListProvider
{
    private readonly KurrentMembershipTable _table = table;
    private readonly GatewayOptions _gatewayOptions = options.Value;

    public TimeSpan MaxStaleness => _gatewayOptions.GatewayListRefreshPeriod;

    public bool IsUpdatable => true;

    public async Task<IList<Uri>> GetGateways()
    {
        var all = await _table.ReadAll().ConfigureAwait(true);
        var result = all.Members
           .Where(x => x.Item1.Status == SiloStatus.Active && x.Item1.ProxyPort != 0)
           .Select(x =>
           {
               var entry = x.Item1;
               return SiloAddress.New(entry.SiloAddress.Endpoint.Address, entry.ProxyPort, entry.SiloAddress.Generation).ToGatewayUri();
           }).ToList();
        return result;
    }

    public async Task InitializeGatewayListProvider()
    {
        await _table.InitializeMembershipTable(true).ConfigureAwait(true); ;
    }
}
