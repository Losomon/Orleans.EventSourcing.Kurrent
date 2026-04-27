namespace Orleans.EventSourcing.Kurrent.Configuration;
/// <summary>
///     Configuration validator for KurrentStorageOptions
/// </summary>
/// <remarks>
/// </remarks>
/// <param name="options"></param>
/// <param name="name"></param>
/// <exception cref="OrleansConfigurationException"></exception>
internal sealed class KurrentStorageOptionsValidator(KurrentStorageOptions options, string name) : IConfigurationValidator
{
    private readonly KurrentStorageOptions _options = options ?? throw new OrleansConfigurationException($"Invalid KurrentStorageOptions for KurrentLogConsistentStorage {name}. Options is required.");

    /// <inheritdoc />
    public void ValidateConfiguration()
    {
        if (_options.ClientSettings is null)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentStorageOptions)} with name {name}. {nameof(KurrentStorageOptions)}.{nameof(_options.ClientSettings)} is required.");
        }
        if (_options.StreamNameProvider is null)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentStorageOptions)} with name {name}. {nameof(KurrentStorageOptions)}.{nameof(_options.StreamNameProvider)} is required.");
        }
    }
}
