
namespace Orleans.EventSourcing.Kurrent.Reminders;

internal sealed class KurrentReminderTableGrainProxy(IGrainFactory grainFactory) : IReminderTable
{
    private readonly IReminderTableGrain grain = grainFactory.GetGrain<IReminderTableGrain>(default(IdSpan));

    public Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName)
    =>  grain.ReadRow(grainId, reminderName);

    public Task<ReminderTableData> ReadRows(GrainId grainId)
        => grain.ReadRows(grainId);

    public Task<ReminderTableData> ReadRows(uint begin, uint end)
        => grain.ReadRows(begin, end);

    public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
        => grain.RemoveRow(grainId, reminderName, eTag);

    public Task TestOnlyClearTable()
        => grain.TestOnlyClearTable();

    public Task<string> UpsertRow(ReminderEntry entry)
        => grain.UpsertRow(entry);
}
