using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

using Orleans.Configuration;
using Orleans.EventSourcing;
using Orleans.Providers;
using Orleans.Runtime.Hosting;
using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.LogConsistency;
using Orleans.EventSourcing.Kurrent.Projections;
using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Hosting;

/// <summary>
/// </summary>
public static class KurrentStorageServiceCollectionExtensions
{
    /// <summary>
    ///     Configures Kurrent as a log consistency storage provider.
    /// </summary>
    public static IServiceCollection AddKurrentBasedLogConsistencyProvider(this IServiceCollection services, string name, Action<OptionsBuilder<KurrentStorageOptions>>? configureOptions = null)
    {
        AddKurrentBasedStorageCommon(services, name, configureOptions);

        // Configure log consistency.
        services.TryAddSingleton<Factory<IGrainContext, ILogConsistencyProtocolServices>>(serviceProvider =>
        {
            var protocolServicesFactory = ActivatorUtilities.CreateFactory(typeof(DefaultProtocolServices), [typeof(IGrainContext)]);
            return grainContext => (ILogConsistencyProtocolServices)protocolServicesFactory(serviceProvider, [grainContext]);
        });

        // Configure log view adaptor.
        services.TryAddSingleton(sp => sp.GetRequiredKeyedService<ILogViewAdaptorFactory>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME));
        services.AddKeyedSingleton<ILogViewAdaptorFactory>(name, (sp, _) => LogConsistencyProviderFactory.Create(sp, name.ToString()));

        // Add a projection provider
        // TODO: Look at using a different IKurrentClient or specific settings for the projection to allow using a follower node or read-only replica
        services.TryAddKeyedSingleton<IGrainEventProvider>(name, (sp, _) =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<KurrentStorageOptions>>().Get(name);
            return ActivatorUtilities.CreateInstance<KurrentGrainEventProvider>(sp, sp.GetRequiredKeyedService<IKurrentClient>(name), sp.GetRequiredKeyedService<IEventConverterFactory>(name), options.StreamNameProvider);
        });

        return services;
    }

    /// <summary>
    ///     Configures Kurrent as a storage provider.
    /// </summary>
    public static IServiceCollection AddKurrentBasedStorageProvider(this IServiceCollection services, string name, Action<OptionsBuilder<KurrentStorageOptions>>? configureOptions = null)
    {
        AddKurrentBasedStorageCommon(services, name, configureOptions);

        services.AddGrainStorage(name, (sp, _) =>
        {
            var client = sp.GetRequiredKeyedService<IKurrentClient>(name);
            var eventSerializer = sp.GetRequiredKeyedService<IEventConverterFactory>(name);
            var options = sp.GetRequiredService<IOptionsMonitor<KurrentStorageOptions>>().Get(name);
            return ActivatorUtilities.CreateInstance<KurrentGrainStorageProvider>(sp, client, eventSerializer, options.StreamNameProvider);
        });

        // Configure log view adaptor.
        services.TryAddSingleton(sp => sp.GetRequiredKeyedService<ILogViewAdaptorFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
        services.AddKeyedSingleton<ILogViewAdaptorFactory>(name, (sp, _) => LogConsistencyProviderFactory.Create(sp, name.ToString()));
        return services;
    }

    private static void AddKurrentBasedStorageCommon(this IServiceCollection services, string name, Action<OptionsBuilder<KurrentStorageOptions>>? configureOptions = null)
    {
        // Create config options
        configureOptions?.Invoke(services.AddOptions<KurrentStorageOptions>(name));

        // Configure log storage.
        services.TryAddTransient<IConfigurationValidator>(sp => new KurrentStorageOptionsValidator(sp.GetRequiredService<IOptionsMonitor<KurrentStorageOptions>>().Get(name), name));
        services.TryAddTransient<IPostConfigureOptions<KurrentStorageOptions>, DefaultStorageProviderSerializerOptionsConfigurator<KurrentStorageOptions>>();
        services.ConfigureNamedOptionForLogging<KurrentStorageOptions>(name);

        services.TryAddKeyedSingleton(name, (sp, _) => KurrentClientFactory.Create(sp, name));
        services.TryAddKeyedSingleton(typeof(DefaultEventConverter<>), name, typeof(DefaultEventConverter<>));
        services.TryAddKeyedSingleton<IEventConverterFactory>(name, (serviceProvider, _) => ActivatorUtilities.CreateInstance<EventConverterFactory>(serviceProvider, name));
    }
}
