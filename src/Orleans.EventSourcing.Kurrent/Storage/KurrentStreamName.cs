namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
///     Default <see cref="IKurrentStreamNameProvider"/> implementation that uses the
///     <c>{GrainType}-{Key}</c> stream naming convention.
/// </summary>
public sealed class KurrentStreamName : IKurrentStreamNameProvider
{
    const char GRAIN_TYPE_SEPARATOR = '-';
    const char STATE_KEY_SEPARATOR = '|';
    const char ESCAPE = '\\';

    // Encodes \→\\, |→\|, -→\- so neither | nor - can be mistaken for a structural separator.
    static string Encode(string value)
    {
        // Escape backslash first to avoid double-escaping
        var s = value.Replace($"{ESCAPE}", @"\\", StringComparison.Ordinal);
        s = s.Replace($"{GRAIN_TYPE_SEPARATOR}", @"\-", StringComparison.Ordinal);
        s = s.Replace($"{STATE_KEY_SEPARATOR}", @"\|", StringComparison.Ordinal);
        return s.Replace("$", @"\$", StringComparison.Ordinal);
    }
    
    // Reverses Encode: \x → x for any x.
    static string Decode(ReadOnlySpan<char> value)
    {
        var sb = new System.Text.StringBuilder(value.Length);
        for (int i = 0; i < value.Length; i++)
        {
            if (value[i] == ESCAPE && i + 1 < value.Length)
                sb.Append(value[++i]);
            else
                sb.Append(value[i]);
        }
        return sb.ToString();
    }

    // Returns the index of the first unescaped occurrence of ch, or -1.
    static int IndexOfUnescaped(string s, char ch, int startIndex = 0)
    {
        for (int i = startIndex; i < s.Length; i++)
        {
            if (s[i] == ESCAPE) { i++; continue; } // skip escaped char
            if (s[i] == ch) return i;
        }
        return -1;
    }

    /// <inheritdoc />
    public string GetStreamPrefix(GrainType grainType)
    {
        ArgumentOutOfRangeException.ThrowIfEqual(grainType, default);
        return $"{Encode(grainType.ToString() ?? throw new ArgumentException("grainType.ToString() cannot be null", nameof(grainType)))}";
    }

    /// <inheritdoc />
    public string GetStreamName(string stateName, GrainId grainId)
    {
        ArgumentNullException.ThrowIfNullOrWhiteSpace(stateName);
        if (grainId.Key.ToString() is not {  } keyStr)
        {
            return $"{GetStreamPrefix(grainId.Type)}{GRAIN_TYPE_SEPARATOR}{Encode(stateName)}";
        }
        else
        {
            return $"{GetStreamPrefix(grainId.Type)}{GRAIN_TYPE_SEPARATOR}{Encode(keyStr)}{STATE_KEY_SEPARATOR}{Encode(stateName)}";
        }
    }

    /// <inheritdoc />
    public string GetStreamName(GrainId grainId)
    {
        if (grainId.IsDefault)
        {
            ArgumentOutOfRangeException.ThrowIfEqual(grainId, default);
        }
        var keyStr = grainId.Key.ToString();
        if (keyStr is null)
        {
            return $"{GetStreamPrefix(grainId.Type)}";
        }
        else
        {
            return $"{GetStreamPrefix(grainId.Type)}{GRAIN_TYPE_SEPARATOR}{Encode(keyStr)}";
        }
    }
    /// <inheritdoc />
    public GrainId GetGrainId(string streamName)
    {
        if (!TryGetGrainId(streamName, out var grainId))
        {
            throw new ArgumentException($"Cannot parse stream '{streamName}' to GrainId, expected format 'GrainType{GRAIN_TYPE_SEPARATOR}Key'");
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

        var typeSepIndex = IndexOfUnescaped(streamName, GRAIN_TYPE_SEPARATOR);
        if (typeSepIndex < 0)
        {
            GrainType grainType = GrainType.Create(Decode(streamName));
            grainId = new GrainId(grainType, default);
            return true;
        }
        else
        {
            var encodedType = streamName[0..typeSepIndex];
            var rest = streamName[(typeSepIndex + 1)..];

            // rest is either {enc_key}  or  {enc_stateName}|{enc_key}
            var stateSepIndex = IndexOfUnescaped(rest, STATE_KEY_SEPARATOR);
            var encodedKey = stateSepIndex < 0 ? rest : rest[..stateSepIndex];

            GrainType grainType = GrainType.Create(Decode(encodedType));
            IdSpan grainKey = IdSpan.Create(Decode(encodedKey));

            grainId = new GrainId(grainType, grainKey);
            return true;
        }
    }
}
