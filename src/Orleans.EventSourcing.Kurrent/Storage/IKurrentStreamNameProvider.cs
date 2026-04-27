namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Builds and parses the Kurrent stream names that the Orleans providers use to persist grain state and events.
///     Implement and register a custom <see cref="IKurrentStreamNameProvider"/> in DI to override the default
///     <c>{GrainType}-{Key}</c> naming scheme.
/// </summary>
public interface IKurrentStreamNameProvider
{
    /// <summary>
    ///     Returns the prefix used for all streams owned by the supplied <paramref name="grainType"/>.
    /// </summary>
    string GetStreamPrefix(GrainType grainType);

    /// <summary>
    ///     Returns the stream name for a named state belonging to a specific grain.
    /// </summary>
    string GetStreamName(string stateName, GrainId grainId);

    /// <summary>
    ///     Returns the stream name for the default (event-sourced) state of a specific grain.
    /// </summary>
    string GetStreamName(GrainId grainId);

    /// <summary>
    ///     Parses a stream name back into a <see cref="GrainId"/>.
    /// </summary>
    /// <exception cref="NotSupportedException">Thrown if <paramref name="streamName"/> cannot be parsed.</exception>
    GrainId GetGrainId(string streamName);

    /// <summary>
    ///     Attempts to parse a stream name back into a <see cref="GrainId"/>.
    /// </summary>
    bool TryGetGrainId(string streamName, out GrainId grainId);
}
