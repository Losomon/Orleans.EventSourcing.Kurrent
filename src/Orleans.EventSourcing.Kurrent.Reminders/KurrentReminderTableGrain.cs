using KurrentDB.Client;
using Orleans.EventSourcing.Kurrent.Reminders.Events;
using Orleans.Providers;

namespace Orleans.EventSourcing.Kurrent.Reminders;

[LogConsistencyProvider(ProviderName = KurrentReminderServiceCollectionExtensions.LOG_PROVIDER_NAME)]
internal sealed class KurrentReminderTableGrain : JournaledGrain<KurrentReminderTableGrainState, ReminderEvent>, IReminderTableGrain
{
    protected override void TransitionState(KurrentReminderTableGrainState state, ReminderEvent @event)
     => @event.Apply(state);
    
    public Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName)
    {
        if (State.Reminders.TryGetValue(grainId, out var remindersForGrain)
            && remindersForGrain.TryGetValue(reminderName, out var reminder))
        {
            return Task.FromResult<ReminderEntry?>(reminder);
        }

        return Task.FromResult<ReminderEntry?>(null);      
    }

    public Task<ReminderTableData> ReadRows(GrainId grainId)
    {
        if (State.Reminders.TryGetValue(grainId, out var remindersForGrain))
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
        return Task.FromResult(new ReminderTableData(State.Reminders.Values.SelectMany(x => x)
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
        if (!State.Reminders.TryGetValue(grainId, out var reminderCollection)
         || !reminderCollection.TryGetValue(reminderName, out var reminderEntry))
        {
            return false;
        }

        if (reminderEntry.ETag != eTag)
        {
            throw new InvalidOperationException($"Failed to remove reminder {reminderName} for grain {grainId} because state was modified concurrently. Please retry.");
        }

        await Commit(new RemovedV1(reminderEntry)).ConfigureAwait(true);

        return true;
    }

    public Task TestOnlyClearTable() => Commit(new ClearedV1());    

    public async Task<string> UpsertRow(ReminderEntry entry)
    {
        if (State.Reminders.TryGetValue(entry.GrainId, out var reminderCollection)
        && reminderCollection.TryGetValue(entry.ReminderName, out var existingRetry)
        && existingRetry.ETag != entry.ETag)
        {
            throw new InvalidOperationException($"Failed to upsert reminder {entry.ReminderName} for grain {entry.GrainId} because state was modified concurrently. Please retry.");
        }

        await Commit(new Upserted(entry)).ConfigureAwait(true);

        return $"{Version:N}";
    }

    private async Task Commit(ReminderEvent reminderEvent)
    {
        RaiseEvent(reminderEvent);
        await ConfirmEvents().ConfigureAwait(true);
    }
}
