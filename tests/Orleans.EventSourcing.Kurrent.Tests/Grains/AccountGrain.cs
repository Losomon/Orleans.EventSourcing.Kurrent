using Orleans.EventSourcing;
using Orleans.EventSourcing.Kurrent;

namespace Orleans.EventSourcing.Kurrent.Tests.Grains
{
    [GenerateSerializer]
    [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent")]
    public abstract record AccountEvent
    {
        [Id(0)]
        public Guid EventId { get; set; }
        [GenerateSerializer]
        [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent.Closed")]
        public sealed record Closed() : AccountEvent;
        [GenerateSerializer]
        [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent.Truncation")]
        [DiscardPriorEvents]
        public sealed record Truncation(decimal Balance) : AccountEvent;
        [GenerateSerializer]
        [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent.Deposited")]
        public sealed record Deposited(decimal Amount) : AccountEvent;
        [GenerateSerializer]
        [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountEvent.Withdrawn")]
        public sealed record Withdrawn(decimal Amount) : AccountEvent;
    }

    [GenerateSerializer]
    [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.AccountState")]
    public sealed record AccountState
    {
        public void Apply(AccountEvent.Deposited deposited)
        {
            Balance += deposited.Amount;
            Assert.True(Balance >= 0);
        }

        public void Apply(AccountEvent.Withdrawn withdrawn)
        {
            Balance -= withdrawn.Amount;
            Assert.True(Balance >= 0);
        }

        public void Apply(AccountEvent.Closed _)
        {

        }

        public void Apply(AccountEvent.Truncation truncation)
        {
            Balance = truncation.Balance;
        }

        [Id(0)]
        public decimal Balance { get; set; }
    }

    [Alias("Orleans.EventSourcing.Kurrent.Tests.Grains.IAccountGrain")]
    interface IAccountGrain : IGrainWithGuidKey
    {
        [Alias("Withdraw")]
        Task<bool> Withdraw(decimal amount);
        [Alias("Deposit")]
        Task Deposit(decimal amount);
        [Alias("DepositWithEventId")]
        Task Deposit(decimal amount, Guid eventId);
        [Alias("DepositWithoutConfirm")]
        ValueTask DepositWithoutConfirm(decimal amount);
        [Alias("TruncateAccountWithoutConfirm")]
        ValueTask TruncateAccountWithoutConfirm();
        [Alias("ConfirmPendingEvents")]
        Task ConfirmPendingEvents();
        [Alias("GetConfirmedBalance")]
        ValueTask<decimal> GetConfirmedBalance();
        [Alias("GetTentativeBalance")]
        ValueTask<decimal> GetTentativeBalance();
        [Alias("RefreshNow")]
        Task RefreshNow();
        [Alias("CloseAccount")]
        Task<decimal> CloseAccount();
        [Alias("GetEvents")]
        Task<IReadOnlyList<AccountEvent>> GetEvents();
        [Alias("GetEventAtVersion")]
        Task<AccountEvent?> GetEventAtVersion(int version);
        [Alias("GetConfirmedVersion")]
        Task<int> GetConfirmedVersion();
        [Alias("ClearLog")]
        Task ClearLog();
        [Alias("TruncateAccount")]
        Task TruncateAccount();
    }

    internal class AccountGrain : JournaledGrain<AccountState, AccountEvent>, IAccountGrain
    {
        public async Task<decimal> CloseAccount()
        {
            var finalBalance = State.Balance;
            base.RaiseEvents<AccountEvent>([new AccountEvent.Withdrawn(finalBalance), new AccountEvent.Closed(),]);
            await ConfirmEvents();
            return finalBalance;
        }

        public async Task Deposit(decimal amount)
        {
            RaiseEvent(new AccountEvent.Deposited(amount) { EventId = Guid.NewGuid(), });
            await ConfirmEvents();
        }

        public async Task Deposit(decimal amount, Guid eventId)
        {
            RaiseEvent(new AccountEvent.Deposited(amount) { EventId = eventId, });
            await ConfirmEvents();
        }

        public ValueTask DepositWithoutConfirm(decimal amount)
        {
            RaiseEvent(new AccountEvent.Deposited(amount) { EventId = Guid.NewGuid(), });
            return ValueTask.CompletedTask;
        }

        public ValueTask TruncateAccountWithoutConfirm()
        {
            RaiseEvent(new AccountEvent.Truncation(State.Balance));
            return ValueTask.CompletedTask;
        }

        public Task ConfirmPendingEvents() => ConfirmEvents();

        public ValueTask<decimal> GetConfirmedBalance() => ValueTask.FromResult(State.Balance);

        public ValueTask<decimal> GetTentativeBalance() => ValueTask.FromResult(TentativeState.Balance);

        public new Task RefreshNow() =>
            // call base class RefreshNow directly (bypass protected definition)
            base.RefreshNow();

        public Task<IReadOnlyList<AccountEvent>> GetEvents() => base.RetrieveConfirmedEvents(0, this.Version);

        public async Task<AccountEvent?> GetEventAtVersion(int version) =>
            (await base.RetrieveConfirmedEvents(version, version)).FirstOrDefault();

        public Task<int> GetConfirmedVersion() => Task.FromResult(this.Version);

        public async Task<bool> Withdraw(decimal amount)
        {
            if (State.Balance < amount)
            {
                return false;
            }

            return await base.RaiseConditionalEvent(new AccountEvent.Withdrawn(amount));
        }

        public Task ClearLog() => base.ClearLogAsync(CancellationToken.None);

        public async Task TruncateAccount()
        {
            RaiseEvent(new AccountEvent.Truncation(State.Balance));
            await ConfirmEvents();
        }
    }
}
