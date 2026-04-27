using System.Diagnostics.Metrics;

namespace Orleans.EventSourcing.Kurrent.Observability;

internal static class Metrics
{
    readonly static Meter Shared = new Meter("Orleans.EventSourcing.Kurrent");

    public readonly static Counter<int> CatchupEventsProcessed = Shared.CreateCounter<int>("Catchup.Events", "count", "Number of events processed");
    public readonly static Counter<int> CatchUpNotificationsProcessed = Shared.CreateCounter<int>("Catchup.Notifications", "count", "Number of notifications processed");
    public readonly static Counter<int> CatchUpCheckpoints = Shared.CreateCounter<int>("Catchup.Checkpoints", "count", "Number of checkpoints reached");
    public readonly static Gauge<byte> CatchUpLive = Shared.CreateGauge<byte>("Catchup.Live", "bool", "1=Live, 0=Catching-up");
    public readonly static Histogram<long> CatchupEventYieldLatency = Shared.CreateHistogram<long>("Catchup.Event.Processing", "ms", "Time taken yielding events");
    public readonly static Histogram<long> CatchupNotificationYieldLatency = Shared.CreateHistogram<long>("Catchup.Notification.Processing", "ms", "Time taken yielding notifications");
    public readonly static Histogram<long> CatchupCheckpointYieldLatency = Shared.CreateHistogram<long>("Catchup.Checkpoint.Processing", "ms", "Time taken yielding checkpoint");

    public readonly static Histogram<long> EventDeserializationLatency = Shared.CreateHistogram<long>("Event.Deserialization", "ms", "Time taken to deserialize");
    public readonly static Histogram<long> EventSerializationLatency = Shared.CreateHistogram<long>("Event.Serialization", "ms", "Time taken to serialize");

    public readonly static Histogram<long> StateDeserializationLatency = Shared.CreateHistogram<long>("State.Deserialization", "ms", "Time taken to deserialize");
    public readonly static Histogram<long> StateSerializationLatency = Shared.CreateHistogram<long>("State.Serialization", "ms", "Time taken to serialize");
}
