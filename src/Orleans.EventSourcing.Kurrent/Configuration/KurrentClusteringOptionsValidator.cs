namespace Orleans.EventSourcing.Kurrent.Configuration;

internal sealed class KurrentClusteringOptionsValidator(KurrentClusteringOptions options) : IConfigurationValidator
{
    private readonly KurrentClusteringOptions _options = options ?? throw new OrleansConfigurationException($"Invalid KurrentClusteringOptions. Options is required.");

    /// <inheritdoc />
    public void ValidateConfiguration()
    {
        if (_options.ClientSettings is null)
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentClusteringOptions)}. {nameof(KurrentClusteringOptions)}.{nameof(_options.ClientSettings)} is required.");
        }

        if (_options.EventCountBeforeSnapshot <= 0) 
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentClusteringOptions)}. {nameof(KurrentClusteringOptions)}.{nameof(_options.EventCountBeforeSnapshot)} must be greater than zero.");
        }

        if (string.IsNullOrWhiteSpace(_options.StreamPrefix))
        {
            throw new OrleansConfigurationException($"Invalid configuration for {nameof(KurrentClusteringOptions)}. {nameof(KurrentClusteringOptions)}.{nameof(_options.StreamPrefix)} is required and must not be empty or whitespace");
        }
    }
}