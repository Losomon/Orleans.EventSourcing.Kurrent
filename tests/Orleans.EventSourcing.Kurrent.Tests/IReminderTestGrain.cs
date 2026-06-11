using Orleans.Concurrency;

namespace Orleans.EventSourcing.Kurrent.Tests;

internal interface IReminderTestGrain : IRemindable, IGrainWithGuidKey
{
    Task RegisterReminder(string reminderName, TimeSpan timeSpan1, TimeSpan timeSpan2);
    Task<bool> Get(string reminderName);
    Task DeleteReminder(string reminderName);
}

[Reentrant]
class ReminderTestGrain : Grain, IReminderTestGrain
{
    public async Task<bool> Get(string reminderName)
    {
        var reminders = await this.GetReminders();
        return reminders.Any(x =>x.ReminderName == reminderName);
    }

    public Task RegisterReminder(string reminderName, TimeSpan dueTime, TimeSpan period)
        => this.RegisterOrUpdateReminder(reminderName, dueTime, period);

    public Task ReceiveReminder(string reminderName, TickStatus status)
        => Task.CompletedTask;

    public async Task DeleteReminder(string reminderName)
    {
        if (await this.GetReminder(reminderName) is { } reminder) 
        { 
            await this.UnregisterReminder(reminder);
        }
    }
}
 