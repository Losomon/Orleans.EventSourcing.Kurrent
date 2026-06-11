namespace Orleans.EventSourcing.Kurrent.Reminders.Events;

[Alias("Orleans.EventSourcing.Reminders.Events.UpsertedV1")]
internal sealed record Upserted(ReminderEntry Entry) : ReminderEvent
{
    internal override void Apply(KurrentReminderTableGrainState state)
        => state.Apply(this);
}

