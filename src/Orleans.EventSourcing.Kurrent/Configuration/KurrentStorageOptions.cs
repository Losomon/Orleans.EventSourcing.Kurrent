using KurrentDB.Client;

using Orleans.Storage;

using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Configuration;
/// <summary>
///     Kurrent log consistent storage options.
/// </summary>
public sealed class KurrentStorageOptions : IStorageProviderSerializerOptions
{
    /// <summary>
    ///     The serializer used in serialize state to Kurrent stream.
    /// </summary>
    public IGrainStorageSerializer GrainStorageSerializer { get; set; } = null!;

    /// <summary>
    ///     The Kurrent client settings.
    /// </summary>
    [Redact]
    public KurrentDBClientSettings ClientSettings { get; set; } = null!;

    /// <summary>
    ///     Builds and parses the Kurrent stream names used to persist grain state and events.
    ///     Defaults to <see cref="KurrentStreamName"/> which uses the <c>{GrainType}-{Key}</c> convention.
    ///     Replace with a custom <see cref="IKurrentStreamNameProvider"/> to change the naming scheme.
    /// </summary>
    public IKurrentStreamNameProvider StreamNameProvider { get; set; } = new KurrentStreamName();
}
