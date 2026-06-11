namespace Orleans.EventSourcing.Kurrent.Reminders.Events;

[Alias("Orleans.EventSourcing.Reminders.Events.RemovedV1")]
internal sealed record RemovedV1(ReminderEntry Entry) : ReminderEvent
{
    internal override void Apply(KurrentReminderTableGrainState state)
        => state.Apply(this);
}

