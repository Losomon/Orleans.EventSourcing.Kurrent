using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Configuration;
/// <summary>
///     Kurrent clustering options.
/// </summary>
public sealed class KurrentClusteringOptions
{
    internal const int DefaultEventCountBeforeSnapshot = 5_000;
    internal const string DefaultStreamPrefix = "Orleans.Cluster.Membership";

    /// <summary>
    ///     The Kurrent client settings.
    /// </summary>
    [Redact]
    public KurrentDBClientSettings ClientSettings { get; set; } = null!;

    /// <summary>
    /// The number of events before a full snapshot is written, default value: <code>5000</code>
    /// 
    /// <para>
    /// Use this setting to control how many events are written to the event stream
    /// before a full snapshot is written and the stream is truncated to allow prior events to be scavenged.
    /// </para>
    /// <para>
    /// If this is set to 0 a full snapshot will be written every time, this will result in more write contention and higher storage usage, but may be useful for testing or debugging purposes.
    /// </para>
    /// </summary>
    public int EventCountBeforeSnapshot { get; set; } = DefaultEventCountBeforeSnapshot;

    /// <summary>
    /// Configure the default stream prefix for the cluster membership stream, default value: <code>Orleans.Cluster.Membership</code>.
    /// </summary>
    public string StreamPrefix { get; set; } = DefaultStreamPrefix;
}
