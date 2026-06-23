using Orleans.Concurrency;
using Orleans.EventSourcing.Kurrent.Reminders.Events;
using Orleans.Providers;

namespace Orleans.EventSourcing.Kurrent.Reminders;

[LogConsistencyProvider(ProviderName = KurrentReminderServiceCollectionExtensions.LOG_PROVIDER_NAME)]
[Reentrant]
internal sealed class KurrentReminderTableGrain : JournaledGrain<KurrentReminderTableGrainState, EventEnvelope<ReminderEvent>>, IReminderTableGrain
{
    protected override void TransitionState(KurrentReminderTableGrainState state, EventEnvelope<ReminderEvent> @event)
     => @event.Event.Apply(state, @event.EventId);
    
    public Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName)
    {
        if (TentativeState.Reminders.TryGetValue(grainId, out var remindersForGrain)
            && remindersForGrain.TryGetValue(reminderName, out var reminder))
        {
            return Task.FromResult<ReminderEntry?>(reminder);
        }

        return Task.FromResult<ReminderEntry?>(null);      
    }

    public Task<ReminderTableData> ReadRows(GrainId grainId)
    {
        if (TentativeState.Reminders.TryGetValue(grainId, out var remindersForGrain))
        {
            return Task.FromResult(new ReminderTableData(remindersForGrain));
        }
        else
        {
            return Task.FromResult(new ReminderTableData());
        }
    }

    public Task<ReminderTableData> ReadRows(uint begin, uint end)
    {
        return Task.FromResult(new ReminderTableData(TentativeState.Reminders.Values.SelectMany(x => x)
                                                           .Where(x =>
                                                                 {
                                                                     var hashCode = x.GrainId.GetUniformHashCode();
                                                                     if (begin >= end)
                                                                     {
                                                                         return hashCode > begin || hashCode <= end;
                                                                     }
                                                                     else
                                                                     {
                                                                         return hashCode > begin && hashCode <= end;
                                                                     }                                                                  
                                                                 }))); 
    }

    public async Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
    {
        if (!TentativeState.Reminders.TryGetValue(grainId, out var reminderCollection)
         || !reminderCollection.TryGetValue(reminderName, out var reminderEntry))
        {
            return false;
        }

        if (reminderEntry.ETag != eTag)
        {
            throw new InvalidOperationException($"Failed to remove reminder {reminderName} for grain {grainId} because state was modified concurrently. Please retry.");
        }

        await Commit(Guid.NewGuid(), new RemovedV1(reminderEntry.GrainId, reminderEntry.ReminderName)).ConfigureAwait(true);

        return true;
    }

    public Task TestOnlyClearTable() => Commit(Guid.NewGuid(), new ClearedV1());    

    public async Task<string> UpsertRow(ReminderEntry entry)
    {
        if (TentativeState.Reminders.TryGetValue(entry.GrainId, out var reminderCollection)
        && reminderCollection.TryGetValue(entry.ReminderName, out var existingRetry)
        && existingRetry.ETag != entry.ETag)
        {
            throw new InvalidOperationException($"Failed to upsert reminder {entry.ReminderName} for grain {entry.GrainId} because state was modified concurrently. Please retry.");
        }

        var eventId = Guid.NewGuid();
        await Commit(eventId, new UpsertedV1(entry.GrainId, entry.ReminderName, entry.StartAt, entry.Period)).ConfigureAwait(true);
        return eventId.ToString("N");
    }

    private Task Commit(Guid eventId, ReminderEvent reminderEvent)
    {
        RaiseEvent(new EventEnvelope<ReminderEvent>(eventId, reminderEvent));
        return ConfirmEvents();
    }
}
