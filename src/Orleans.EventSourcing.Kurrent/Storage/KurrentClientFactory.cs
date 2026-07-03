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
    public static Uri InMemoryEmulatorUriInsecure { get; } = new Uri("http://kurrentemulator:2113/");
    
    /// <summary>
    ///     Creates a KurrentClient instance.
    /// </summary>
    public static IKurrentClient Create(IServiceProvider serviceProvider, string name)
    {
        var clientSettings = serviceProvider.GetRequiredService<IOptionsMonitor<KurrentStorageOptions>>().Get(name).ClientSettings;

        if (InMemoryEmulatorUri.Equals(clientSettings.ConnectivitySettings.Address) ||
            InMemoryEmulatorUriInsecure.Equals(clientSettings.ConnectivitySettings.Address))
        {
            return InMemoryKurrentClient.Get(name);
        }
        else
        {
            return new KurrentClient(clientSettings);
        }
    }

    public static IKurrentClient Create(IServiceProvider serviceProvider)
    {
        var clientSettings = serviceProvider.GetRequiredService<IOptions<KurrentClusteringOptions>>().Value.ClientSettings;

        if (InMemoryEmulatorUri.Equals(clientSettings.ConnectivitySettings.Address) ||
            InMemoryEmulatorUriInsecure.Equals(clientSettings.ConnectivitySettings.Address))
        {
            return InMemoryKurrentClient.Get(string.Empty);
        }
        else
        {
            return new KurrentClient(clientSettings);
        }
    }
}
