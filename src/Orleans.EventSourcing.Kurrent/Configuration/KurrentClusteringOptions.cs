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
    /// The number of events before a full snapshot is written, default value 5_000.
    /// 
    /// <para>
    /// Use this setting to control when a full snapshot of the membership table is written to the event stream
    /// and the stream is truncated to allow prior events to be scavenged.
    /// </para>
    /// </summary>
    public int EventCountBeforeSnapshot { get; set; } = DefaultEventCountBeforeSnapshot;

    /// <summary>
    /// Configure the default stream prefix for the cluster membership stream
    /// </summary>
    public string StreamPrefix { get; set; } = DefaultStreamPrefix;
}
