using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.Storage;

namespace Orleans.EventSourcing.Kurrent.Reminders;

/// <summary>
/// Extensions for configuring Kurrent-backed reminder services.
/// </summary>
public static class KurrentReminderServiceCollectionExtensions
{
    internal const string LOG_PROVIDER_NAME = "KurrentReminderService";

    /// <summary>
    ///  Configure Orleans to use Kurrent for reminders. This will add a reminder service implementation that stores reminders in Kurrent, and a log consistency provider that the reminder service relies on to function correctly.
    /// </summary>
    /// <param name="siloBuilder">The silo builder to configure.</param>
    /// <param name="configureOptions">An action to configure Kurrent storage options for the reminder service.</param>
    /// <returns>The configured silo builder.</returns>    
    public static ISiloBuilder AddKurrentReminderService(this ISiloBuilder siloBuilder, Action<OptionsBuilder<KurrentStorageOptions>> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(siloBuilder);
        siloBuilder.Services.AddKurrentReminderService(configureOptions);
        return siloBuilder;
    }

    /// <summary>
    ///  Configure Orleans to use Kurrent for reminders. This will add a reminder service implementation that stores reminders in Kurrent, and a log consistency provider that the reminder service relies on to function correctly.
    /// </summary>
    /// <param name="siloBuilder">The silo builder to configure.</param>
    /// <param name="configureOptions">Configure Kurrent storage options for the reminder service.</param>
    /// <returns>The configured silo builder.</returns>
    public static ISiloBuilder AddKurrentReminderService(this ISiloBuilder siloBuilder, Action<KurrentStorageOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(siloBuilder);
        siloBuilder.Services.AddKurrentReminderService(options =>
        {
            options.Configure(x => x.GrainStorageSerializer = new SystemTextJsonGrainStorageSerializer());
            options.Configure(configureOptions);
        });
        return siloBuilder;
    }

    internal static IServiceCollection AddKurrentReminderService(this IServiceCollection serviceCollection, Action<OptionsBuilder<KurrentStorageOptions>>? configureOptions = null)
    {
        serviceCollection.AddKurrentBasedLogConsistencyProvider(LOG_PROVIDER_NAME, configureOptions);
        serviceCollection.AddReminders();
        serviceCollection.AddSingleton<IReminderTable, KurrentReminderTableGrainProxy>();
        
        return serviceCollection;
    }
}
