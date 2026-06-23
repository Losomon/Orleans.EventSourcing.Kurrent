namespace Orleans.EventSourcing.Kurrent.Reminders.Events;

[Alias("Orleans.EventSourcing.Reminders.Events.RemovedV1")]
internal sealed record RemovedV1(GrainId GrainId, string ReminderName) : ReminderEvent
{
    internal override void Apply(KurrentReminderTableGrainState state, Guid eventId)
        => state.Apply(this, eventId);
}
