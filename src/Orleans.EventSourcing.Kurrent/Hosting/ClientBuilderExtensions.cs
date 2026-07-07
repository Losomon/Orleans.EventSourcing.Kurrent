using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.EventSourcing.Kurrent.Clustering;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.Messaging;

namespace Orleans.EventSourcing.Kurrent.Hosting;

/// <summary>
/// Utility class for configuring Orleans clients to use Kurrent as the membership provider.
/// </summary>
public static class ClientBuilderExtensions
{
    /// <summary>
    /// Configures the Orleans client to use Kurrent as the membership provider.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="configureOptions">A delegate to configure the Kurrent clustering options.</param>
    /// <returns>The updated client builder.</returns>
    public static IClientBuilder UseKurrentClustering(this IClientBuilder builder, Action<KurrentClusteringOptions> configureOptions)
        => builder.UseKurrentClustering(options => options.Configure(configureOptions));

    /// <summary>
    /// Configures the Orleans client to use Kurrent as the membership provider.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="configureOptions">A delegate to configure the Kurrent clustering options.</param>
    /// <returns>The updated client builder.</returns>
    public static IClientBuilder UseKurrentClustering(this IClientBuilder builder, Action<OptionsBuilder<KurrentClusteringOptions>> configureOptions)
    => builder.ConfigureServices(services =>
    {
            configureOptions?.Invoke(services.AddOptions<KurrentClusteringOptions>());
            services.TryAddTransient<IConfigurationValidator>(sp => new KurrentClusteringOptionsValidator(sp.GetRequiredService<IOptions<KurrentClusteringOptions>>().Value));
            services.TryAddSingleton(sp => KurrentClientFactory.Create(sp));
            services.TryAddSingleton(KurrentMembershipEventStorageFactory.Create);
            services.TryAddSingleton<IGatewayListProvider, KurrentGatewayListProvider>();
        });
}
