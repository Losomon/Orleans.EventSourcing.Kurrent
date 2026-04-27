namespace Orleans.EventSourcing.Kurrent.Storage;
public interface IEventSerializerFactory
{
    public IEventSerializer<TLogView> GetEventSerializer<TLogView>();
}
