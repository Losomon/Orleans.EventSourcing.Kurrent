using System.Collections.ObjectModel;

namespace Orleans.EventSourcing.Kurrent.Reminders;

internal sealed class ReminderCollection() : KeyedCollection<string, ReminderEntry>(StringComparer.OrdinalIgnoreCase)
{
    protected override string GetKeyForItem(ReminderEntry item) => item.ReminderName;
}
