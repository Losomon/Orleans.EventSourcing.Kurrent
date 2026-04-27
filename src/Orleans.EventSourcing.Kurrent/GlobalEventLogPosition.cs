using System.Text.Json.Serialization;

namespace Orleans.EventSourcing.Kurrent;

/// <summary>
/// A global position in an event-sourced log. 
/// </summary>
/// <param name="Value"></param>
[GenerateSerializer]
[Immutable]
[Alias("Orleans.EventSourcing.GlobalEventLogPosition")]
[JsonConverter(typeof(GlobalEventLogPositionConverter))]
public readonly record struct GlobalEventLogPosition(ulong Value)
{
    public static GlobalEventLogPosition Start { get; } = new GlobalEventLogPosition(0);

    public static bool operator <(GlobalEventLogPosition left, GlobalEventLogPosition right) => left.Value < right.Value;
    public static bool operator >(GlobalEventLogPosition left, GlobalEventLogPosition right) => left.Value > right.Value;
    public static bool operator <=(GlobalEventLogPosition left, GlobalEventLogPosition right) => left.Value <= right.Value;
    public static bool operator >=(GlobalEventLogPosition left, GlobalEventLogPosition right) => left.Value >= right.Value;
    public override string ToString() => $"{Value:N}";
}

