using Orleans.EventSourcing.Kurrent.Reminders.Events;

namespace Orleans.EventSourcing.Kurrent.Reminders;

internal sealed class KurrentReminderTableGrainState
{  
    private readonly Dictionary<GrainId, ReminderCollection> reminders = [];
    public IReadOnlyDictionary<GrainId, ReminderCollection> Reminders => reminders;

    internal void Apply(Upserted upsertedEvent)
    {
        if (!reminders.TryGetValue(upsertedEvent.Entry.GrainId, out var list))
        {
            reminders[upsertedEvent.Entry.GrainId] = [upsertedEvent.Entry];
        }
        else
        {
            list.Remove(upsertedEvent.Entry.ReminderName);
            list.Add(upsertedEvent.Entry);
        }
    }

    internal void Apply(RemovedV1 removedEvent)
    {
        if (reminders.TryGetValue(removedEvent.Entry.GrainId, out var list)
            && list.Remove(removedEvent.Entry.ReminderName)
            && list.Count == 0)
        {
            _ = reminders.Remove(removedEvent.Entry.GrainId);            
        }
    }

    internal void Apply(ClearedV1 _)
     => reminders.Clear();    
}
