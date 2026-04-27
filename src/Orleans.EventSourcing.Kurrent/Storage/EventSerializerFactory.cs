using System.Collections.Concurrent;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.EventSourcing.Kurrent.Configuration;

namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Factory used to create instances of event serializer
/// </summary>
public class EventSerializerFactory(IServiceProvider serviceProvider, string name) : IEventSerializerFactory
{
    readonly IOptionsMonitor<KurrentStorageOptions> options = serviceProvider.GetRequiredService<IOptionsMonitor<KurrentStorageOptions>>();

    readonly ConcurrentDictionary<Type, object> eventSerializers = new();
    /// <summary>
    ///     Creates a event serializer instance.
    /// </summary>
    public IEventSerializer<TLogView> GetEventSerializer<TLogView>()
    {
        // This could be improved with extensible registration of event serializers, perhaps via KurrentStorageOptions
        var logViewType = typeof(TLogView);
        return (IEventSerializer<TLogView>)eventSerializers.GetOrAdd(logViewType,
                                                                     (_) =>
                                                                     {
                                                                         if (logViewType.IsGenericType && logViewType.GetGenericTypeDefinition() == typeof(EventEnvelope<>))
                                                                         {
                                                                             var eventType = logViewType.GetGenericArguments()[0]!;

                                                                             // If the type is an event, we need to use the event serializer
                                                                             return ActivatorUtilities.CreateInstance(serviceProvider, typeof(EventEnvelopeSerializer<>).MakeGenericType(eventType), options.CurrentValue);
                                                                         }
                                                                         else
                                                                         {
                                                                             // Fallback to the default serializer
                                                                             return serviceProvider.GetRequiredKeyedService<DefaultEventSerializer<TLogView>>(name);
                                                                         }
                                                                     }
                                                                     );
    }
}
