using System.Data;

using Grpc.Core;

using KurrentDB.Client;

using Microsoft.Extensions.DependencyInjection;

using Orleans.Core.Internal;
using Orleans.Providers;
using Orleans.Storage;
using Orleans.TestingHost;


using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.EventSourcing.Kurrent.Tests.Grains;
using Orleans.EventSourcing.Kurrent.Projections;

namespace Orleans.EventSourcing.Kurrent.Tests;

public sealed class BasicTests : IAsyncLifetime
{
    static readonly KurrentDBClientSettings clientSettings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");

    private InProcessTestCluster cluster = null!;
    private ExceptionalKurrentClientWrapper kurrentClient = null!;
    private IKurrentStreamNameProvider streamNameProvider = null!;

    public async ValueTask InitializeAsync()
    {
        bool useRealKurrent = false; // Toggle to use real kurrent rather than in-memory                 
        
        clientSettings.DefaultDeadline = TimeSpan.FromSeconds(5);
        kurrentClient = new ExceptionalKurrentClientWrapper(useRealKurrent ? new KurrentClient(clientSettings) : new InMemoryKurrentClient()); // Lets us throw exceptions for testing

        var builder = new InProcessTestClusterBuilder();
        builder.ConfigureClient(c => { c.AddActivityPropagation(); });
        builder.ConfigureSilo((s, c) =>
                              {
                                  c.Services.AddKeyedSingleton<IKurrentClient>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME, kurrentClient);
                                  c.AddKurrentBasedLogConsistencyProviderAsDefault(o => o.ClientSettings = clientSettings);
                                  c.AddKurrentBasedGrainStorageProviderAsDefault();
                                  c.AddActivityPropagation();
                              });
        builder.Options.ConfigureFileLogging = false;
        cluster = builder.Build();
        await cluster.DeployAsync();

        streamNameProvider = cluster.Silos.First().ServiceProvider
            .GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<Configuration.KurrentStorageOptions>>()
            .Get(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME)
            .StreamNameProvider;
    }

    public ValueTask DisposeAsync()
    {
        cluster.Dispose();
        kurrentClient.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact(Timeout = 5000)]
    public async Task DepositWithdraw()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        Assert.Equal(0, await account.GetConfirmedBalance());
        await account.Deposit(10);
        Assert.Equal(10, await account.GetConfirmedBalance());
        Assert.False(await account.Withdraw(15));
        Assert.True(await account.Withdraw(5));
        Assert.Equal(5, await account.GetConfirmedBalance());

        await account.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        Assert.Equal(5, await account.GetConfirmedBalance());

        var @events = await account.GetEvents();
        Assert.Equal(2, @events.Count);
        var deposited = Assert.IsType<AccountEvent.Deposited>(events[0]);
        Assert.Equal(10, deposited.Amount);
        var withdrawn = Assert.IsType<AccountEvent.Withdrawn>(events[1]);
        Assert.Equal(5, withdrawn.Amount);
    }

    [Fact(Timeout = 5000)]
    public async Task CheckEventsNumbering()
    {
        // GetEvents calls internally RetrieveConfirmedEvents(0, ConfirmedVersion);
        // by checking the explicit version we check events start at 1
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        var @events = await account.GetEvents();
        Assert.Empty(@events);
        Assert.Equal(0, await account.GetConfirmedVersion());
        Assert.Equal(0, await account.GetConfirmedBalance());

        await account.Deposit(10);

        @events = await account.GetEvents();
        Assert.Single(@events);
        Assert.Equal(1, await account.GetConfirmedVersion());
        Assert.Equal(10, await account.GetConfirmedBalance());
        Assert.Equal(10, Assert.IsType<AccountEvent.Deposited>(await account.GetEventAtVersion(1)).Amount);

        await account.RefreshNow(); // re-read state from storage

        @events = await account.GetEvents();
        Assert.Single(@events);
        Assert.Equal(1, await account.GetConfirmedVersion());
        Assert.Equal(10, await account.GetConfirmedBalance());
        Assert.Equal(10, Assert.IsType<AccountEvent.Deposited>(await account.GetEventAtVersion(1)).Amount);

        await account.Withdraw(5);

        @events = await account.GetEvents();
        Assert.Equal(2, @events.Count);
        Assert.Equal(2, await account.GetConfirmedVersion());
        Assert.Equal(5, await account.GetConfirmedBalance());
        Assert.Equal(10, Assert.IsType<AccountEvent.Deposited>(await account.GetEventAtVersion(1)).Amount);
        Assert.Equal(5, Assert.IsType<AccountEvent.Withdrawn>(await account.GetEventAtVersion(2)).Amount);
    }

    [Fact(Timeout = 5000)]
    public async Task EventIdPropagation()
    {
        var eventId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());
        await account.Deposit(10, eventId);
        var @events = await account.GetEvents();
        Assert.Single(@events);
        var deposited = Assert.IsType<AccountEvent.Deposited>(events[0]);
        Assert.Equal(10, deposited.Amount);
        Assert.Equal(eventId, deposited.EventId);
    }

    [Fact(Timeout = 5000)]
    public async Task CheckVersionMismatch()
    {
        var accountGrainId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountGrainId);

        await account.Deposit(100);

        await kurrentClient.DuplicateLastEventInStream(streamNameProvider.GetStreamName(account.GetGrainId()));

        Assert.Equal(100, await account.GetConfirmedBalance());
        Assert.False(await account.Withdraw(66));
        Assert.Equal(34, await account.GetTentativeBalance());
        Assert.Equal(100, await account.GetConfirmedBalance());
        Assert.False(await account.Withdraw(33));
        Assert.Equal(1, await account.GetTentativeBalance());
        Assert.Equal(100, await account.GetConfirmedBalance());

        await Assert.ThrowsAsync<InconsistentStateException>(async () => { await account.Deposit(11); }); // causes grain restart
        Assert.Equal(200, await account.GetConfirmedBalance());
        await account.Deposit(22);
        Assert.Equal(222, await account.GetConfirmedBalance());

        var @events = await account.GetEvents();
        Assert.Equal(3, @events.Count);
        Assert.Equal(100, Assert.IsType<AccountEvent.Deposited>(events[0]).Amount);
        Assert.Equal(100, Assert.IsType<AccountEvent.Deposited>(events[1]).Amount);
        Assert.Equal(22, Assert.IsType<AccountEvent.Deposited>(events[2]).Amount);
    }

    [Fact(Timeout = 5000)]
    public async Task CheckHandleTruncation()
    {
        var kurrentClient = cluster.Silos.First().ServiceProvider.GetRequiredKeyedService<IKurrentClient>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        Assert.NotNull(kurrentClient);

        var accountGrainId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountGrainId);

        var powersOfTwo = Enumerable.Range(0, 10).Select(i => (int)Math.Pow(2, i)).ToList();

        foreach (var i in powersOfTwo)
        {
            await account.Deposit(i);
        }
        foreach (var i in powersOfTwo)
        {
            await account.Deposit(i);
            await account.Withdraw(i);
        }

        Assert.Equal((powersOfTwo[^1] * 2) - 1, await account.GetConfirmedBalance());
        Assert.Equal(powersOfTwo.Count * 3, (await account.GetEvents()).Count);

        // remove the first iteration of deposits
        await kurrentClient.SetStreamMetadata(streamNameProvider.GetStreamName(account.GetGrainId()), StreamState.NoStream, new StreamMetadata(truncateBefore: StreamPosition.FromStreamRevision(10)), TestContext.Current.CancellationToken);
        Assert.Equal(powersOfTwo.Count * 2, (await account.GetEvents()).Count);

        Assert.Equal((powersOfTwo[^1] * 2) - 1, await account.GetConfirmedBalance());

        await account.RefreshNow(); // re-read state from storage

        Assert.Equal(0, await account.GetConfirmedBalance());

        // remove the events from all above iterations and check the last added event remains
        await account.Deposit(77);
        Assert.Equal(77, await account.GetConfirmedBalance());
        await kurrentClient.SetStreamMetadata(streamNameProvider.GetStreamName(account.GetGrainId()), StreamState.StreamRevision(0), new StreamMetadata(truncateBefore: StreamPosition.FromStreamRevision(30)), TestContext.Current.CancellationToken);
        Assert.Single(await account.GetEvents());
        await account.RefreshNow();
        Assert.Equal(77, await account.GetConfirmedBalance());
    }

    [Fact(Timeout = 10000)]
    public async Task CheckHandleExceptionTest()
    {
        var accountGrainId = Guid.NewGuid();

        var account = cluster.Client.GetGrain<IAccountGrain>(accountGrainId);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), new TimeoutException("Test read exception"));

        var ex1 = await Assert.ThrowsAsync<InconsistentStateException>(() => account.Deposit(11));
        Assert.IsType<TimeoutException>(ex1.InnerException);

        await account.Deposit(22);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), new RpcException(new Status(StatusCode.DeadlineExceeded, "RPC timeout blah blah")));

        await account.DepositWithoutConfirm(33); // exception will be seen on following write

        var ex2 = await Assert.ThrowsAsync<InconsistentStateException>(() => account.Deposit(5));
        Assert.IsType<RpcException>(ex2.InnerException);

        Assert.Equal(22, await account.GetConfirmedBalance());

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()), new StreamDeletedException("stream name"));

        var ex3 = await Assert.ThrowsAsync<InconsistentStateException>(() => account.Deposit(11));
        Assert.IsType<StreamDeletedException>(ex3.InnerException);
    }

    [Fact(Timeout = 5000)]
    public async Task StateGrainCRUD()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());

        Assert.False(await stateGrain.RecordExists());
        Assert.Equal(42, await stateGrain.GetValue());
        Assert.Equal(string.Empty, await stateGrain.GetEtag());

        await stateGrain.SetValue(99393);
        Assert.True(await stateGrain.RecordExists());
        Assert.Equal("0", await stateGrain.GetEtag());

        await stateGrain.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        Assert.Equal(99393, await stateGrain.GetValue());
        Assert.True(await stateGrain.RecordExists());
        Assert.Equal("0", await stateGrain.GetEtag());

        await stateGrain.ClearValue();
        Assert.Equal(string.Empty, await stateGrain.GetEtag());
        Assert.False(await stateGrain.RecordExists());
        Assert.Equal(42, await stateGrain.GetValue());

        await stateGrain.ClearValue();
        await stateGrain.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        // Check state after clear
        Assert.False(await stateGrain.RecordExists());
        Assert.Equal(string.Empty, await stateGrain.GetEtag());
        Assert.Equal(42, await stateGrain.GetValue());

        // Check reading and writing after clearing

        await stateGrain.SetValue(519235351);
        Assert.True(await stateGrain.RecordExists());
        Assert.Equal(519235351, await stateGrain.GetValue());
        Assert.Equal("1", await stateGrain.GetEtag());

        await stateGrain.ClearValue();
    }

    [Fact(Timeout = 5000)]
    public async Task TestDeleteOnNonExistantState()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());
        await stateGrain.ClearValue();
    }

    [Fact(Timeout = 5000)]
    public async Task StateGrainWriteETagTest()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());

        await stateGrain.SetValue(99393);

        await kurrentClient.DuplicateLastEventInStream(streamNameProvider.GetStreamName("test", stateGrain.GetGrainId()));

        await Assert.ThrowsAsync<InconsistentStateException>(() => stateGrain.SetValue(356));
    }

    [Fact(Timeout = 5000)]
    public async Task StateGrainClearETagTest()
    {
        var stateGrain = cluster.Client.GetGrain<IStateGrain>(Guid.NewGuid());

        await stateGrain.SetValue(99393);

        await kurrentClient.DuplicateLastEventInStream(streamNameProvider.GetStreamName("test", stateGrain.GetGrainId()));

        await Assert.ThrowsAsync<InconsistentStateException>(() => stateGrain.ClearValue());
    }

    [Fact(Timeout = 5000)]
    public async Task ProjectionAbstractBaseType()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<NotSupportedException>(() => grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent)], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = 5000)]
    public async Task ProjectionNoTypes()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<ArgumentException>(() => grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = 5000)]
    public async Task ProjectionCannotCastToBaseEvent()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<ArgumentException>(() => grainProjectionProvider.SubscribeToGrainEvents<string>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(object)], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = 5000)]
    public async Task ProjectionAggregation()
    {
        Guid[] idsForThisTest = [Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()];

        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);

        var projection = Task.Run<(decimal totalBalance, decimal averageCreditRating)>(async () =>
        {
            await Task.Yield();
            decimal total = 0;
            int expectedEvents = 4;
            int creditRatingSum = 0;
            int creditRatingCount = 0;

            await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited), typeof(UserEvent.UserCreditRatingChanged)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
            {
                if (update is not GrainEvent<object> grainEvent
                || !idsForThisTest.Contains(grainEvent.EventGrainId.GetGuidKey()))
                {
                    continue; // skip events from other unit tests
                }

                switch (grainEvent.Event)
                {
                    case AccountEvent.Deposited deposited:
                        total += deposited.Amount;
                        break;
                    case UserEvent.UserCreditRatingChanged userCreditRatingChanged:
                        creditRatingSum += userCreditRatingChanged.CreditRating;
                        creditRatingCount++;
                        break;
                }
                if (--expectedEvents == 0)
                {
                    break;
                }
            }
            return (total, creditRatingCount > 0 ? creditRatingSum / creditRatingCount : 0);
        });

        await cluster.Client.GetGrain<IAccountGrain>(idsForThisTest[0]).Deposit(45);
        await cluster.Client.GetGrain<IAccountGrain>(idsForThisTest[1]).Deposit(831);
        await cluster.Client.GetGrain<IAccountGrain>(idsForThisTest[2]).Deposit(1563);
        await cluster.Client.GetGrain<IUserGrain>(idsForThisTest[3]).UpdateCreditRating(5);

        var (totalBalance, average) = await projection;

        Assert.Equal(45 + 831 + 1563, totalBalance);
        Assert.Equal(5, average);
    }

    [Fact(Timeout = 5000)]
    public async Task ProjectionSubscribeReturnsEventsInOrder()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(100);
        await account.Deposit(200);
        await account.Withdraw(50);

        var receivedEvents = new List<object>();

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited), typeof(AccountEvent.Withdrawn)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            if (update is GrainEvent<object> grainEvent
                && grainEvent.EventGrainId.GetGuidKey() == accountId)
            {
                receivedEvents.Add(grainEvent.Event);
                if (receivedEvents.Count == 3)
                    break;
            }
        }

        Assert.Equal(100, Assert.IsType<AccountEvent.Deposited>(receivedEvents[0]).Amount);
        Assert.Equal(200, Assert.IsType<AccountEvent.Deposited>(receivedEvents[1]).Amount);
        Assert.Equal(50, Assert.IsType<AccountEvent.Withdrawn>(receivedEvents[2]).Amount);
    }



    [Fact(Timeout = 5000)]
    public async Task ProjectionSubscribeNotificationReturnsEventsInOrder()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(100);
        await account.Deposit(200);
        await account.Withdraw(50);

        var receivedEvents = new List<int>();

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEventNotifications(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited), typeof(AccountEvent.Withdrawn)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken).ConfigureAwait(false))
        {
            if (update is GrainEventNotification grainEvent
                && grainEvent.EventGrainId.GetGuidKey() == accountId)
            {
                receivedEvents.Add(grainEvent.EventGrainVersion);
                if (receivedEvents.Count == 3)
                    break;
            }
        }

        Assert.Equal(1, receivedEvents[0]);
        Assert.Equal(2, receivedEvents[1]);
        Assert.Equal(3, receivedEvents[2]);
    }


    [Fact(Timeout = 5000)]
    public async Task ProjectionEventNumbersMatchGrainVersionNumbers()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(100);

        var receivedEventVersions = new List<int>();

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited), typeof(AccountEvent.Withdrawn)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            if (update is GrainEvent<object> grainEvent
                && grainEvent.EventGrainId.GetGuidKey() == accountId)
            {
                receivedEventVersions.Add(grainEvent.EventGrainVersion);
                if (receivedEventVersions.Count == 1)
                    break;
            }
        }

        // Orleans event-sourcing starts with version 1 when the first event is written, version 0 means no events written. Kurrent starts with version 0 when first event is written
        Assert.Equal(1, receivedEventVersions[0]);
    }

    [Fact(Timeout = 5000)]
    public async Task ProjectionSubscribeSkipsUnrelatedEvents()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        var accountId = Guid.NewGuid();
        var userId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);
        var user = cluster.Client.GetGrain<IUserGrain>(userId);

        await account.Deposit(123);
        await user.UpdateCreditRating(7);

        var receivedEvents = new List<object>();

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            if (update is GrainEvent<object> grainEvent
                && grainEvent.EventGrainId.GetGuidKey() == accountId)
            {
                receivedEvents.Add(grainEvent.Event);
                break;
            }
        }

        Assert.Single(receivedEvents);
        Assert.Equal(123, Assert.IsType<AccountEvent.Deposited>(receivedEvents[0]).Amount);
    }

    [Fact(Timeout = 5000)]
    public async Task ProjectionThrowsOnNullTypeArray()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, null!, TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = 5000)]
    public async Task ProjectionThrowsOnDefaultSubscriber()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            grainProjectionProvider.SubscribeToGrainEvents<object>(default, GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited)], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask());
    }

    [Fact(Timeout = 5000)]
    public async Task ProjectionCaughtUp()
    {
        var grainProjectionProvider = cluster.GetSiloServiceProvider().GetRequiredKeyedService<IGrainEventProvider>(ProviderConstants.DEFAULT_LOG_CONSISTENCY_PROVIDER_NAME);

        await foreach (var update in grainProjectionProvider.SubscribeToGrainEvents<object>(GrainId.Parse($"Test/{Guid.NewGuid()}"), GlobalEventLogPosition.Start, [typeof(AccountEvent.Deposited)], TestContext.Current.CancellationToken).WithCancellation(TestContext.Current.CancellationToken))
        {
            if (update is CaughtUp)
            {
                break;
            }
        }
    }

    [Fact(Timeout = 5000)]
    public async Task ClearLog_OnEmptyStream_IsNoOp()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());

        await account.ClearLog();

        Assert.Equal(0, await account.GetConfirmedVersion());
        Assert.Equal(0, await account.GetConfirmedBalance());
        Assert.Empty(await account.GetEvents());
    }

    [Fact(Timeout = 5000)]
    public async Task ClearLog_AfterAppends_ResetsViewsAndAllowsReAppend()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());

        await account.Deposit(10);
        await account.Deposit(20);
        Assert.Equal(2, await account.GetConfirmedVersion());
        Assert.Equal(30, await account.GetConfirmedBalance());

        await account.ClearLog();

        Assert.Equal(0, await account.GetConfirmedVersion());
        Assert.Equal(0, await account.GetConfirmedBalance());
        Assert.Equal(0, await account.GetTentativeBalance());
        Assert.Empty(await account.GetEvents());

        // Re-appending after a soft delete should succeed and the new view should reflect only the new event.
        await account.Deposit(7);
        Assert.Equal(7, await account.GetConfirmedBalance());
        Assert.Single(await account.GetEvents());
    }

    [Fact(Timeout = 5000)]
    public async Task ClearLog_PersistsAcrossDeactivation()
    {
        var accountId = Guid.NewGuid();
        var account = cluster.Client.GetGrain<IAccountGrain>(accountId);

        await account.Deposit(50);
        await account.ClearLog();

        await account.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        Assert.Equal(0, await account.GetConfirmedVersion());
        Assert.Equal(0, await account.GetConfirmedBalance());
        Assert.Empty(await account.GetEvents());
    }

    [Fact(Timeout = 5000)]
    public async Task ClearLog_PropagatesKurrentExceptions()
    {
        var account = cluster.Client.GetGrain<IAccountGrain>(Guid.NewGuid());

        await account.Deposit(10);

        kurrentClient.AddExceptionToThrowOnceForTesting(streamNameProvider.GetStreamName(account.GetGrainId()),
                                                         new TimeoutException("Test delete exception"));

        var ex = await Assert.ThrowsAsync<InconsistentStateException>(() => account.ClearLog());
        Assert.IsType<TimeoutException>(ex.InnerException);
    }
}
