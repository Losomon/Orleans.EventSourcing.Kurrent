using Orleans.EventSourcing;
using Orleans.Runtime;

namespace LoadTest.Grains;

[GenerateSerializer]
[Alias("LoadTest.Grains.AccountEvent")]
public abstract record AccountEvent
{
    [GenerateSerializer]
    [Alias("LoadTest.Grains.AccountEvent.Deposited")]
    public sealed record Deposited(decimal Amount) : AccountEvent;

    [GenerateSerializer]
    [Alias("LoadTest.Grains.AccountEvent.Withdrawn")]
    public sealed record Withdrawn(decimal Amount) : AccountEvent;

    [GenerateSerializer]
    [Alias("LoadTest.Grains.AccountEvent.InterestAccrued")]
    public sealed record InterestAccrued(DateTimeOffset At) : AccountEvent;
}

[GenerateSerializer]
[Alias("LoadTest.Grains.AccountState")]
public sealed record AccountState
{
    [Id(0)]
    public decimal Balance { get; set; }

    [Id(1)]
    public int InterestTicks { get; set; }

    public void Apply(AccountEvent.Deposited deposited) => Balance += deposited.Amount;

    public void Apply(AccountEvent.Withdrawn withdrawn) => Balance -= withdrawn.Amount;

    public void Apply(AccountEvent.InterestAccrued _) => InterestTicks++;
}

[GenerateSerializer]
[Alias("LoadTest.Grains.AccountSummary")]
public sealed record AccountSummary(
    [property: Id(0)] decimal Balance,
    [property: Id(1)] int ConfirmedVersion,
    [property: Id(2)] int InterestTicks,
    [property: Id(3)] bool ReminderActive);

[Alias("LoadTest.Grains.IAccountGrain")]
public interface IAccountGrain : IGrainWithGuidKey
{
    [Alias("Deposit")]
    Task Deposit(decimal amount);

    [Alias("Withdraw")]
    Task<bool> Withdraw(decimal amount);

    [Alias("GetBalance")]
    ValueTask<decimal> GetBalance();

    [Alias("GetSummary")]
    Task<AccountSummary> GetSummary();

    [Alias("EnableInterestReminder")]
    Task EnableInterestReminder(TimeSpan dueTime, TimeSpan period);

    [Alias("DisableInterestReminder")]
    Task DisableInterestReminder();
}

public sealed class AccountGrain : JournaledGrain<AccountState, AccountEvent>, IAccountGrain, IRemindable
{
    private const string InterestReminderName = "accrue-interest";
    private IGrainReminder? reminder;

    public async Task Deposit(decimal amount)
    {
        RaiseEvent(new AccountEvent.Deposited(amount));
        await ConfirmEvents();
    }

    public async Task<bool> Withdraw(decimal amount)
    {
        if (State.Balance < amount)
        {
            return false;
        }

        RaiseEvent(new AccountEvent.Withdrawn(amount));
        await ConfirmEvents();
        return true;
    }

    public ValueTask<decimal> GetBalance() => ValueTask.FromResult(State.Balance);

    public async Task<AccountSummary> GetSummary()
    {
        var reminder = await this.GetReminder(InterestReminderName);
        return new AccountSummary(State.Balance, Version, State.InterestTicks, reminder is not null);
    }

    public async Task EnableInterestReminder(TimeSpan dueTime, TimeSpan period)
    {
        reminder ??= await this.RegisterOrUpdateReminder(InterestReminderName, dueTime, period);
    }

    public async Task DisableInterestReminder()
    {
        if (await this.GetReminder(InterestReminderName) is { } reminder)
        {
            await this.UnregisterReminder(reminder);
            this.reminder = null;
        }
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == InterestReminderName)
        {
            // Accrue interest as an event so reminder ticks flow through the Kurrent event log too.
            RaiseEvent(new AccountEvent.InterestAccrued(DateTimeOffset.UtcNow));
            await ConfirmEvents();
        }
    }
}
