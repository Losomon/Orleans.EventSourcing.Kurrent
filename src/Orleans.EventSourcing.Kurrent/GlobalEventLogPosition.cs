using System.Text.Json.Serialization;

namespace Orleans.EventSourcing;

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
    /// <summary>
    /// The position of the first event in the log. This is the default value for <see cref="GlobalEventLogPosition"/>.
    /// </summary>
    public static GlobalEventLogPosition Start { get; } = new GlobalEventLogPosition(0);

    /// <inheritdoc />
    public static bool operator <(GlobalEventLogPosition left, GlobalEventLogPosition right) => left.Value < right.Value;

    /// <inheritdoc />
    public static bool operator >(GlobalEventLogPosition left, GlobalEventLogPosition right) => left.Value > right.Value;

    /// <inheritdoc />
    public static bool operator <=(GlobalEventLogPosition left, GlobalEventLogPosition right) => left.Value <= right.Value;

    /// <inheritdoc />
    public static bool operator >=(GlobalEventLogPosition left, GlobalEventLogPosition right) => left.Value >= right.Value;

    /// <inheritdoc />
    public override string ToString() => $"{Value:N}";

    /// <inheritdoc />
    public int CompareTo(GlobalEventLogPosition other)
        => Value.CompareTo(other.Value);
}

