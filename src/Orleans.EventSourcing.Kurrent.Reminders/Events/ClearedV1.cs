namespace Orleans.EventSourcing.Kurrent.Reminders.Events;

[Alias("Orleans.EventSourcing.Reminders.Events.ClearedV1")]
#pragma warning disable OEK0002 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
[DiscardPriorEvents]
#pragma warning restore OEK0002 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
internal sealed record ClearedV1() : ReminderEvent
{
    internal override void Apply(KurrentReminderTableGrainState state)
     => state.Apply(this);    
}

