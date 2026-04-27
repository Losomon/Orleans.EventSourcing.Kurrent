using System.Diagnostics;
using System.Text.Json;

using KurrentDB.Client;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

using Orleans.Serialization;
using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests
{
    public class EventSerializerTests
    {
        private readonly IEventSerializerFactory eventSerializerFactory;
        private readonly IGrainStorageSerializer grainStorageSerializer;
        private static readonly ActivityListener Listener;

        static EventSerializerTests()
        {
            Listener = new ActivityListener
            {
                ShouldListenTo = p => p.Name == "Kurrent.Unit.Test",
                Sample = Sample,
                SampleUsingParentId = SampleUsingParentId,
            };
            return;

            static ActivitySamplingResult Sample(ref ActivityCreationOptions<ActivityContext> options)
            {
                //Trace id has to be accessed in sample to reproduce the scenario when SetParentId does not work
                var _ = options.TraceId;
                return ActivitySamplingResult.PropagationData;
            }

            static ActivitySamplingResult SampleUsingParentId(ref ActivityCreationOptions<string> options)
            {
                //Trace id has to be accessed in sample to reproduce the scenario when SetParentId does not work
                var _ = options.TraceId;
                return ActivitySamplingResult.PropagationData;
            }
        }

        public EventSerializerTests()
        {
            var hb = new HostApplicationBuilder();
            hb.Services.AddLogging();

            hb.Services.AddSingleton<OrleansJsonSerializer>();
            hb.Services.AddOptions<OrleansJsonSerializerOptions>();

            hb.Services.AddSingleton<IGrainStorageSerializer, JsonGrainStorageSerializer>();

            hb.Services.AddOptions<KurrentStorageOptions>();
            hb.Services.AddTransient<IPostConfigureOptions<KurrentStorageOptions>, DefaultStorageProviderSerializerOptionsConfigurator<KurrentStorageOptions>>();

            hb.Services.AddKeyedSingleton(typeof(DefaultEventSerializer<>), "test", typeof(DefaultEventSerializer<>));

            hb.Services.AddSingleton<IEventSerializerFactory>((sp) => new EventSerializerFactory(sp, "test"));
            hb.Services.AddSerializer();

            var sp = hb.Build();

            eventSerializerFactory = sp.Services.GetRequiredService<IEventSerializerFactory>();
            grainStorageSerializer = sp.Services.GetRequiredService<IGrainStorageSerializer>();
        }

        [Alias("EventWithMetadataAlias")]
        public record Event(int SomeValue)
        {

        }

        private static ResolvedEvent ConvertToRecord(EventData serializedEvent)
        {
            ArgumentNullException.ThrowIfNull(serializedEvent, nameof(serializedEvent));

            var metadata = new Dictionary<string, string>()
            {
                { "type", serializedEvent.Type},
                { "created", $"{DateTime.UtcNow.Ticks}" },
                { "content-type", serializedEvent.ContentType }
            };

            return new ResolvedEvent(new EventRecord(string.Empty, serializedEvent.EventId, StreamPosition.Start, Position.Start, metadata, serializedEvent.Data, serializedEvent.Metadata), null, null);
        }

        [Fact]
        public void BasicEventRoundtrip()
        {
            var instance = new Event(12435353);
            var eventSerializer = eventSerializerFactory.GetEventSerializer<Event>();
            var eventData = eventSerializer.SerializeEvent(instance);
            Assert.Equal("EventWithMetadataAlias", eventData.Type);
            Assert.Equal(0, eventData.Metadata.Length);

            var copyOfEvent = eventSerializer.DeserializeEvent(ConvertToRecord(eventData));
            Assert.Equal(instance, copyOfEvent);
        }

        [Fact]
        public void ActivityTracingPropagatedToEvent()
        {
            ActivitySource.AddActivityListener(Listener);

            using var source = new ActivitySource("Kurrent.Unit.Test");
            Assert.True(source.HasListeners());

            using var activity = source.StartActivity("SomeWork");

            Assert.NotNull(activity);
            var eventWithMetadata = new Event(885633);
            var eventSerializer = eventSerializerFactory.GetEventSerializer<Event>();

            var eventData = eventSerializer.SerializeEvent(eventWithMetadata);

            JsonDocument jsonDocument = JsonDocument.Parse(eventData.Metadata);
            Assert.Equal(activity.Id, jsonDocument.RootElement.GetProperty("$correlationId").GetString());
        }

        [Fact]
        public void EventEnvelopeTests()
        {
            var eventEnvelope = new EventEnvelope<string>(Guid.NewGuid(), "howdy", new Dictionary<string, string>() { { "key", "value" } });

            var eventSerializer = eventSerializerFactory.GetEventSerializer<EventEnvelope<string>>();

            Assert.IsType<EventEnvelopeSerializer<string>>(eventSerializer);

            var eventData = eventSerializer.SerializeEvent(eventEnvelope);

            // Check the EventId, event and metadata have been promoted correctly into EventData
            Assert.Equal(eventEnvelope.EventId, eventData.EventId.ToGuid());
            Assert.Equal("value", JsonDocument.Parse(eventData.Metadata).RootElement.GetProperty("key").GetString());
            Assert.Equal("howdy", grainStorageSerializer.Deserialize<string>(eventData.Data));
        }
    }
}
