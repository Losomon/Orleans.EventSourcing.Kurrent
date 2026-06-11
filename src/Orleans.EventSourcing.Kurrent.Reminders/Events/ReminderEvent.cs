namespace Orleans.EventSourcing.Kurrent.Reminders.Events;

internal abstract record ReminderEvent
{
    internal abstract void Apply(KurrentReminderTableGrainState state);
}