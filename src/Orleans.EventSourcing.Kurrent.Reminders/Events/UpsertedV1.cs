namespace Orleans.EventSourcing.Kurrent.Reminders.Events;

[Alias("Orleans.EventSourcing.Reminders.Events.UpsertedV1")]
internal sealed record UpsertedV1(GrainId GrainId,
                                  string ReminderName,
                                  DateTime StartAt,
                                  TimeSpan Period) : ReminderEvent
{
    internal override void Apply(KurrentReminderTableGrainState state, Guid eventId)
        => state.Apply(this, eventId);
}
