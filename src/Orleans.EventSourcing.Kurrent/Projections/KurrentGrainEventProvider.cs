using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

using KurrentDB.Client;

using Microsoft.Extensions.Logging;

using Orleans.Metadata;
using Orleans.Serialization.TypeSystem;

using Orleans.EventSourcing.Kurrent.Observability;
using Orleans.EventSourcing.Kurrent.Storage;
using System.Collections.Immutable;


namespace Orleans.EventSourcing.Kurrent.Projections;

internal sealed class KurrentGrainEventProvider(GrainInterfaceTypeResolver grainInterfaceTypeResolver, GrainInterfaceTypeToGrainTypeResolver grainInterfaceTypeToGrainTypeResolver, TypeConverter typeConverter, IKurrentClient kurrentClient, IEventConverterFactory eventSerializerFactory, IKurrentStreamNameProvider streamNameProvider, ILogger<KurrentGrainEventProvider> logger) : IGrainEventProvider
{
    const uint ALL_STREAM_CHECKPOINT_INTERVAL = 10_000; // The interval of AllStreamCheckpointReached

    private IEventFilter GetEventFilterRegex<TEvent>(Type[] eventFilter)
    {
        ArgumentNullException.ThrowIfNull(eventFilter);

        if (eventFilter.Length == 0)
        {
            throw new ArgumentException("Cannot be empty", nameof(eventFilter));
        }
        if (eventFilter.Any(x => x.IsAbstract))
        {
            throw new NotSupportedException($"Abstract event types like {string.Join(',', eventFilter.Where(x => x.IsAbstract))} are not supported");
        }
        if (eventFilter.Any(x => !x.IsAssignableTo(typeof(TEvent))))
        {
            throw new ArgumentException($"Event types {string.Join(',', eventFilter.Where(x => x.IsAbstract))} cannot be cast to {typeof(TEvent)}", nameof(eventFilter));
        }
        var eventNameRegex = string.Join('|', eventFilter.Select(x => $"^{Regex.Escape(typeConverter.Format(x))}$"));
        return KurrentDB.Client.EventTypeFilter.RegularExpression(eventNameRegex);

    }

    /// <inheritdoc />
    public IAsyncEnumerable<EventStreamUpdate> SubscribeToGrainEvents<TEventBase>(GrainId subscriber, GlobalEventLogPosition startingPosition, Type[] eventFilter, CancellationToken cancellationToken) where TEventBase : class
        => SubscribeCore(subscriber, startingPosition, GetEventFilterRegex<TEventBase>(eventFilter), eventSerializerFactory.GetEventConverter<EventEnvelope<TEventBase>>(), cancellationToken);

    /// <inheritdoc /> 
    public IAsyncEnumerable<EventStreamUpdate> SubscribeToGrainEventNotifications(GrainId subscriber, GlobalEventLogPosition startingPosition, Type[] eventFilter, CancellationToken cancellationToken)
      => SubscribeCore<object>(subscriber, startingPosition, GetEventFilterRegex<object>(eventFilter), null, cancellationToken);

    private async IAsyncEnumerable<EventStreamUpdate> SubscribeCore<TEventBase>(GrainId subscriber, GlobalEventLogPosition startingPosition, IEventFilter eventFilter, IEventConverter<EventEnvelope<TEventBase>>? eventSerializer,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TEventBase : class
    {
        if (subscriber.IsDefault)
        {
            throw new ArgumentException("GrainId cannot be default", nameof(subscriber));
        }

        var observabilityTags = new TagList
        {
            { "GrainType", subscriber.Type.ToString() },
        };

        logger.Subscribe(subscriber, startingPosition, eventFilter);
        Metrics.CatchUpLive.Record(0, observabilityTags);

        await foreach (var message in kurrentClient.CatchUpSubscription(startingPosition.ToAllPosition(), eventFilter, ALL_STREAM_CHECKPOINT_INTERVAL, cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(true))
        {
            switch (message)
            {
                case StreamMessage.Event eventMessage:

                    var grainId = streamNameProvider.GetGrainId(eventMessage.ResolvedEvent.OriginalStreamId);

                    if (eventSerializer is not null)
                    {
                        TagList eventTags = new();
                        foreach (var tag in observabilityTags)
                        {
                            eventTags.Add(tag);
                        }
                        eventTags.Add("EventType", eventMessage.ResolvedEvent.Event.EventType);

                        var stopwatch = Stopwatch.StartNew();
                        var deserializedEvent = eventSerializer.DeserializeEvent(eventMessage.ResolvedEvent);
                        Metrics.EventDeserializationLatency.Record(stopwatch.ElapsedMilliseconds, eventTags);

                        logger.EventReceived(subscriber, eventMessage.ResolvedEvent.OriginalPosition, grainId, eventMessage.ResolvedEvent.OriginalEventNumber, deserializedEvent);
                        Metrics.CatchupEventsProcessed.Add(1, eventTags);
                        stopwatch.Restart();
                        yield return new GrainEvent<TEventBase>(eventMessage.ResolvedEvent.OriginalPosition!.Value.ToGlobalEventLogPosition(), deserializedEvent.Event, grainId, eventMessage.ResolvedEvent.OriginalEventNumber.ToVersion(), deserializedEvent.EventId, deserializedEvent.Metadata?.AsReadOnly() ?? (IReadOnlyDictionary<string,string>)ImmutableDictionary<string,string>.Empty);
                        Metrics.CatchupEventYieldLatency.Record(stopwatch.ElapsedMilliseconds, eventTags);
                    }
                    else
                    {
                        TagList eventTags = new();
                        foreach (var tag in observabilityTags)
                        {
                            eventTags.Add(tag);
                        }
                        eventTags.Add("EventType", eventMessage.ResolvedEvent.Event.EventType);
                        logger.EventNotificationReceived(subscriber, eventMessage.ResolvedEvent.OriginalPosition, grainId, eventMessage.ResolvedEvent.OriginalEventNumber);
                        Metrics.CatchUpNotificationsProcessed.Add(1, eventTags);

                        var stopwatch = Stopwatch.StartNew();
                        yield return new GrainEventNotification(eventMessage.ResolvedEvent.OriginalPosition!.Value.ToGlobalEventLogPosition(), grainId, eventMessage.ResolvedEvent.OriginalEventNumber.ToVersion(), eventMessage.ResolvedEvent.Event.EventId.ToGuid());
                        Metrics.CatchupNotificationYieldLatency.Record(stopwatch.ElapsedMilliseconds, eventTags);
                    }
                    break;
                case StreamMessage.AllStreamCheckpointReached checkpoint:

                    logger.Checkpoint(subscriber, checkpoint.Position);
                    Metrics.CatchUpCheckpoints.Add(1, observabilityTags);

                    var checkpointYieldTime = Stopwatch.StartNew();
                    yield return new Checkpoint(checkpoint.Position.ToGlobalEventLogPosition());
                    Metrics.CatchupCheckpointYieldLatency.Record(checkpointYieldTime.ElapsedMilliseconds, observabilityTags);
                    break;
                case StreamMessage.CaughtUp:

                    logger.SubscriptionCaughtUp(subscriber);
                    Metrics.CatchUpLive.Record(1, observabilityTags);

                    yield return CaughtUp.Instance;
                    break;
                case StreamMessage.FellBehind:

                    logger.SubscriptionFellBehind(subscriber);
                    Metrics.CatchUpLive.Record(0, observabilityTags);

                    yield return FallenBehind.Instance;
                    break;
            }
        }
    }

    public IAsyncEnumerable<EventStreamUpdate> SubscribeToGrainEventNotifications<TGrain>(GrainId subscriber, GlobalEventLogPosition startingPosition, CancellationToken cancellationToken) where TGrain : IGrain
        => SubscribeCore<object>(subscriber, startingPosition, StreamFilter.Prefix(streamNameProvider.GetStreamPrefix(grainInterfaceTypeToGrainTypeResolver.GetGrainType(grainInterfaceTypeResolver.GetGrainInterfaceType(typeof(TGrain))))), null, cancellationToken);


    public IAsyncEnumerable<EventStreamUpdate> SubscribeToGrainEvents<TGrain, TEventBase>(GrainId subscriber, GlobalEventLogPosition startingPosition, CancellationToken cancellationToken) where TGrain : IGrain where TEventBase : class
        => SubscribeCore<TEventBase>(subscriber, startingPosition, StreamFilter.Prefix(streamNameProvider.GetStreamPrefix(grainInterfaceTypeToGrainTypeResolver.GetGrainType(grainInterfaceTypeResolver.GetGrainInterfaceType(typeof(TGrain))))), eventSerializerFactory.GetEventConverter<EventEnvelope<TEventBase>>(), cancellationToken);
}
