using Microsoft.CodeAnalysis;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Messaging;

namespace Orleans.EventSourcing.Kurrent.Clustering;

internal sealed class KurrentGatewayListProvider(KurrentMembershipEventStorage storage, IOptions<GatewayOptions> options) : IGatewayListProvider
{
    private readonly KurrentMembershipEventStorage _storage = storage;
    private readonly GatewayOptions _gatewayOptions = options.Value;
    private MembershipView _membershipView = storage.InitialView;

    public TimeSpan MaxStaleness => _gatewayOptions.GatewayListRefreshPeriod;

    public bool IsUpdatable => true;

    public async Task<IList<Uri>> GetGateways()
    {
        _membershipView = await _storage.RefreshState(_membershipView).ConfigureAwait(true);
        var result = _membershipView.Members
           .Where(x => x.Value.Status == SiloStatus.Active && x.Value.ProxyPort != 0)
           .Select(x =>
           {
               var entry = x.Key;
               return SiloAddress.New(entry.Endpoint.Address, x.Value.ProxyPort, entry.Generation).ToGatewayUri();
           }).ToList();
        return result;
    }

    public Task InitializeGatewayListProvider() => Task.CompletedTask;
}
