using System.Collections.Concurrent;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Factory used to create instances of KurrentClient
/// </summary>
internal static class KurrentClientFactory
{
    public static Uri InMemoryEmulatorUri { get; } = new Uri("https://kurrentemulator:2113/");
    readonly static ConcurrentDictionary<string, IKurrentClient> Clients = [];

    /// <summary>
    ///     Creates a KurrentClient instance.
    /// </summary>
    public static IKurrentClient Create(IServiceProvider serviceProvider, string name)
    {
        var options = serviceProvider.GetRequiredService<IOptionsMonitor<KurrentStorageOptions>>().Get(name);
        if (InMemoryEmulatorUri.Equals(options.ClientSettings.ConnectivitySettings.Address))
        {
            return Clients.GetOrAdd(name, new InMemoryKurrentClient());
        }
        else
        {
            return Clients.GetOrAdd(name, new KurrentClient(options.ClientSettings));
        }
    }
}
