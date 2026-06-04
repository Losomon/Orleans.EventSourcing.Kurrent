namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
/// Provides a factory for creating event serializers.
/// </summary>
public interface IEventConverterFactory
{

    /// <summary>
    /// Get a serializer for <typeparamref name="TLogView"/>
    /// </summary>
    /// <typeparam name="TLogView">The type of the log view.</typeparam>
    /// <returns>An event serializer for the specified log view type.</returns>
    public IEventConverter<TLogView> GetEventConverter<TLogView>();
}
