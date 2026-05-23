using Microsoft.Extensions.Options;

using Orleans.Providers;

using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Hosting;

/// <summary>
/// Extension methods for configuring Kurrent storage in an Orleans silo.
/// </summary>
public static class KurrentStorageSiloBuilderExtensions
{
    /// <summary>
    ///     Configures Kurrent as the default log consistency storage provider.
    /// </summary>
    /// <param name="builder">The silo builder to configure.</param>
    /// <param name="configureOptions">An action to configure the Kurrent storage options.</param>
    /// <returns>The configured silo builder.</returns>
    public static ISiloBuilder AddKurrentBasedLogConsistencyProviderAsDefault(this ISiloBuilder builder, Action<KurrentStorageOptions> configureOptions)
     => builder.AddKurrentBasedLogConsistencyProvider(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME, configureOptions);

    /// <summary>
    ///     Configures Kurrent as a log consistency storage provider.
    /// </summary>
    /// <param name="builder">The silo builder to configure.</param>
    /// <param name="name">The name of the log consistency provider.</param>
    /// <param name="configureOptions">An action to configure the Kurrent storage options.</param>
    /// <returns>The configured silo builder.</returns>
    public static ISiloBuilder AddKurrentBasedLogConsistencyProvider(this ISiloBuilder builder, string name, Action<KurrentStorageOptions> configureOptions)
     => builder.ConfigureServices(services => services.AddKurrentBasedLogConsistencyProvider(name, x => x.Configure(configureOptions)));    

    /// <summary>
    ///     Configures Kurrent as the default log consistency storage provider.
    /// </summary>
    /// <param name="builder">The silo builder to configure.</param>
    /// <param name="configureOptions">An optional action to configure the Kurrent storage options.</param>
    /// <returns>The configured silo builder.</returns>
    public static ISiloBuilder AddKurrentBasedLogConsistencyProviderAsDefault(this ISiloBuilder builder, Action<OptionsBuilder<KurrentStorageOptions>>? configureOptions = null)
      => builder.AddKurrentBasedLogConsistencyProvider(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME, configureOptions);  

    /// <summary>
    ///     Configures Kurrent as a log consistency storage provider.
    /// </summary>
    /// <param name="builder">The silo builder to configure.</param>
    /// <param name="name">The name of the log consistency provider.</param>
    /// <param name="configureOptions">An optional action to configure the Kurrent storage options.</param>
    /// <returns>The configured silo builder.</returns>
    public static ISiloBuilder AddKurrentBasedLogConsistencyProvider(this ISiloBuilder builder, string name, Action<OptionsBuilder<KurrentStorageOptions>>? configureOptions = null)
       => builder.ConfigureServices(services => services.AddKurrentBasedLogConsistencyProvider(name, configureOptions));    

    /// <summary>
    ///     Configures Kurrent as the default grain storage provider.
    /// </summary>
    /// <param name="builder">The silo builder to configure.</param>
    /// <param name="configureOptions">An optional action to configure the Kurrent storage options.</param>
    /// <returns>The configured silo builder.</returns>
    public static ISiloBuilder AddKurrentBasedGrainStorageProviderAsDefault(this ISiloBuilder builder, Action<OptionsBuilder<KurrentStorageOptions>>? configureOptions = null)
       => AddKurrentBasedGrainStorageProvider(builder, ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);

    /// <summary>
    ///     Configures Kurrent as a grain storage provider with a specified name.
    /// </summary>
    /// <param name="builder">The silo builder to configure.</param>
    /// <param name="name">The name of the grain storage provider.</param>
    /// <param name="configureOptions">An optional action to configure the Kurrent storage options.</param>
    /// <returns>The configured silo builder.</returns>
    public static ISiloBuilder AddKurrentBasedGrainStorageProvider(this ISiloBuilder builder, string name, Action<OptionsBuilder<KurrentStorageOptions>>? configureOptions = null)
       => builder.ConfigureServices(services => services.AddKurrentBasedStorageProvider(name, configureOptions));

    /// <summary>
    ///     Configures Kurrent as the default grain storage provider.
    /// </summary>
    /// <param name="builder">The silo builder to configure.</param>
    /// <param name="configureOptions">An optional action to configure the Kurrent storage options.</param>
    /// <returns>The configured silo builder.</returns>
    public static ISiloBuilder AddKurrentBasedGrainStorageProviderAsDefault(this ISiloBuilder builder, Action<KurrentStorageOptions> configureOptions) 
       => AddKurrentBasedGrainStorageProvider(builder, ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);

    /// <summary>
    ///     Configures Kurrent as a grain storage provider with a specified name.
    /// </summary>
    /// <param name="builder">The silo builder to configure.</param>
    /// <param name="name">The name of the grain storage provider.</param>
    /// <param name="configureOptions">An optional action to configure the Kurrent storage options.</param>
    /// <returns>The configured silo builder.</returns>
    public static ISiloBuilder AddKurrentBasedGrainStorageProvider(this ISiloBuilder builder, string name, Action<KurrentStorageOptions> configureOptions) 
       => builder.ConfigureServices(services => services.AddKurrentBasedStorageProvider(name, x => x.Configure(configureOptions)));
}
