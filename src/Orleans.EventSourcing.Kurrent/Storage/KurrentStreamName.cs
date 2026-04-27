namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Default <see cref="IKurrentStreamNameProvider"/> implementation that uses the
///     <c>{GrainType}-{Key}</c> stream naming convention.
/// </summary>
public sealed class KurrentStreamName : IKurrentStreamNameProvider
{
    const char GRAIN_TYPE_SEPARATOR = '-';
    
    /// <inheritdoc />
    public string GetStreamPrefix(GrainType grainType) => $"{grainType}{GRAIN_TYPE_SEPARATOR}";

    /// <inheritdoc />
    public string GetStreamName(string stateName, GrainId grainId) => $"{GetStreamPrefix(grainId.Type)}{stateName}|{grainId.Key}";

    /// <inheritdoc />
    public string GetStreamName(GrainId grainId) => $"{GetStreamPrefix(grainId.Type)}{grainId.Key}"; // decided to exclude ServiceId and conform to pattern {type}-{id} as per ResponseStream in FeedbackProcessor 

    /// <inheritdoc />
    public GrainId GetGrainId(string streamName)
    {
        if (!TryGetGrainId(streamName, out var grainId))
        {
            throw new NotSupportedException($"Cannot parse stream '{streamName}' to GrainId, expected format 'GrainType{GRAIN_TYPE_SEPARATOR}Key'");
        }
        return grainId;
    }

    /// <inheritdoc />
    public bool TryGetGrainId(string streamName, out GrainId grainId)
    {
        if (string.IsNullOrWhiteSpace(streamName))
        {
            grainId = default;
            return false;
        }

        var seperatorIndex = streamName.IndexOf(GRAIN_TYPE_SEPARATOR, StringComparison.OrdinalIgnoreCase);
        if (seperatorIndex < 0)
        {
            grainId = default;
            return false;
        }

        GrainType grainType = GrainType.Create(streamName[0..seperatorIndex]);
        IdSpan grainKey = IdSpan.Create(streamName[(seperatorIndex + 1)..]);

        grainId = new GrainId(grainType, grainKey);
        return true;
    }
}
