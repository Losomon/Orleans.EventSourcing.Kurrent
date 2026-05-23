namespace Orleans.EventSourcing.Kurrent.Storage;

/// <summary>
/// Provides a factory for creating event serializers.
/// </summary>
public interface IEventSerializerFactory
{

    /// <summary>
    /// Get a serializer for <typeparamref name="TLogView"/>
    /// </summary>
    /// <typeparam name="TLogView">The type of the log view.</typeparam>
    /// <returns>An event serializer for the specified log view type.</returns>
    public IEventSerializer<TLogView> GetEventSerializer<TLogView>();
}
