using System.Collections.Concurrent;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Factory used to create instances of event converter
/// </summary>
public sealed class EventConverterFactory(IServiceProvider serviceProvider, string name) : IEventConverterFactory
{
    readonly IOptionsMonitor<KurrentStorageOptions> options = serviceProvider.GetRequiredService<IOptionsMonitor<KurrentStorageOptions>>();

    readonly ConcurrentDictionary<Type, object> eventConverters = new();
    /// <summary>
    ///     Creates a event converter instance.
    /// </summary>
    public IEventConverter<TLogView> GetEventConverter<TLogView>()
    {
        // This could be improved with extensible registration of event converters, perhaps via KurrentStorageOptions
        var logViewType = typeof(TLogView);
        return (IEventConverter<TLogView>)eventConverters.GetOrAdd(logViewType,
                                                                     (_) =>
                                                                     {
                                                                         if (logViewType.IsGenericType && logViewType.GetGenericTypeDefinition() == typeof(EventEnvelope<>))
                                                                         {
                                                                             var eventType = logViewType.GetGenericArguments()[0]!;

                                                                             // If the type is an event, we need to use the event serializer
                                                                             return ActivatorUtilities.CreateInstance(serviceProvider, typeof(EventEnvelopeConveter<>).MakeGenericType(eventType), options.CurrentValue);
                                                                         }
                                                                         else
                                                                         {
                                                                             // Fallback to the default serializer
                                                                             return serviceProvider.GetRequiredKeyedService<DefaultEventConverter<TLogView>>(name);
                                                                         }
                                                                     }
                                                                     );
    }
}
