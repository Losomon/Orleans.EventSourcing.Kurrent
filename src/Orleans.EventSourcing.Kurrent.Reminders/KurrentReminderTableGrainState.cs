using Orleans.EventSourcing.Kurrent.Reminders.Events;

namespace Orleans.EventSourcing.Kurrent.Reminders;

internal sealed class KurrentReminderTableGrainState
{  
    private readonly Dictionary<GrainId, ReminderCollection> reminders = [];
    public IReadOnlyDictionary<GrainId, ReminderCollection> Reminders => reminders;

    internal void Apply(UpsertedV1 upsertedEvent, Guid eventId)
    {
        if (!reminders.TryGetValue(upsertedEvent.GrainId, out var list))
        {
            reminders[upsertedEvent.GrainId] = [new ReminderEntry()
            {
                GrainId = upsertedEvent.GrainId,
                ReminderName = upsertedEvent.ReminderName,
                StartAt = upsertedEvent.StartAt,
                Period = upsertedEvent.Period,
                ETag = eventId.ToString("N")
            }];
        }
        else
        {
            list.Remove(upsertedEvent.ReminderName);
            list.Add(new ReminderEntry()
            {
                GrainId = upsertedEvent.GrainId,
                ReminderName = upsertedEvent.ReminderName,
                StartAt = upsertedEvent.StartAt,
                Period = upsertedEvent.Period,
                ETag = eventId.ToString("N")
            });
        }
    }

    internal void Apply(RemovedV1 removedEvent, Guid eventId)
    {
        if (reminders.TryGetValue(removedEvent.GrainId, out var list)
            && list.Remove(removedEvent.ReminderName)
            && list.Count == 0)
        {
            _ = reminders.Remove(removedEvent.GrainId);            
        }
    }

    internal void Apply(ClearedV1 _, Guid __)
     => reminders.Clear();    
}
