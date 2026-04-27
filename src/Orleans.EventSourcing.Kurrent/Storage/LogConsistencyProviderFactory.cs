using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Factory used to create instances of log consistent provider.
/// </summary>
internal static class LogConsistencyProviderFactory
{
    /// <summary>
    ///     Creates a Kurrent log consistent storage instance.
    /// </summary>
    public static LogConsistencyProvider Create(IServiceProvider serviceProvider, string name)
    {
        var options = serviceProvider.GetRequiredService<IOptionsMonitor<KurrentStorageOptions>>();
        return new LogConsistencyProvider(serviceProvider.GetRequiredKeyedService<IEventSerializerFactory>(name), serviceProvider.GetRequiredKeyedService<IKurrentClient>(name), options.Get(name));
    }
}
