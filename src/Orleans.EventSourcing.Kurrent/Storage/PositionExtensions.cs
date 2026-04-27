using KurrentDB.Client;

namespace Orleans.EventSourcing.Kurrent.Storage;

internal static class PositionExtensions
{
    public static GlobalEventLogPosition ToGlobalEventLogPosition(this Position position) => new GlobalEventLogPosition(position.CommitPosition);

    public static FromAll ToAllPosition(this GlobalEventLogPosition globalEventLogPosition) => FromAll.After(new Position(globalEventLogPosition.Value, globalEventLogPosition.Value));

    public static int ToVersion(this StreamPosition streamPosition)
    {
        checked
        {
            return (int)streamPosition.ToInt64() + 1; // +1 because we are using 0-based indexes in Kurrent and 1-based indexes in Orleans
        }
    }

    public static StreamState ToStreamState(this int version)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 0);
        return StreamState.StreamRevision((ulong)(version - 1)); // -1 because we are using 1-based indexes in Orleans and 0-based indexes in Kurrent
    }

    public static StreamPosition ToStreamPosition(this int version)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 0);
        if (version == 0)
        {
            return StreamPosition.Start;
        }
        else
        {
            return StreamPosition.FromInt64(version - 1); // -1 because we are using 1-based indexes in Orleans and 0-based indexes in Kurrent
        }
    }
}
