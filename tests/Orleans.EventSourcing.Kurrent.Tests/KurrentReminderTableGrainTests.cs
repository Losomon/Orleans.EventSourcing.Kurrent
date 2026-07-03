using KurrentDB.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Core.Internal;
using Orleans.EventSourcing.Kurrent.Reminders;
using Orleans.EventSourcing.Kurrent.Storage;
using Orleans.TestingHost;

namespace Orleans.EventSourcing.Kurrent.Tests;

public sealed class KurrentReminderTableGrainTests : IAsyncLifetime
{
    private const int Timeout = 30_000;

    static readonly KurrentDBClientSettings clientSettings =
        KurrentDBClientSettings.Create("esdb://kurrentemulator:2113?tls=false");

    private InProcessTestCluster cluster = null!;
    private IReminderTable table = null!;

    public async ValueTask InitializeAsync()
    {
        // Register a fresh InMemoryKurrentClient instance before the provider setup
        // so that TryAddKeyedSingleton inside AddKurrentReminderService does not replace it.
        // This keeps this class's Kurrent state isolated from other test classes.
        var inMemoryClient = new InMemoryKurrentClient();

        var builder = new InProcessTestClusterBuilder();
        builder.ConfigureClient(c => c.AddActivityPropagation());
        builder.ConfigureSilo((s, c) =>
        {
            c.Services.AddKeyedSingleton<IKurrentClient>(
                KurrentReminderServiceCollectionExtensions.LOG_PROVIDER_NAME,
                inMemoryClient);
            c.AddKurrentReminderService(o => o.ClientSettings = clientSettings);
        });
        builder.Options.ConfigureFileLogging = false;
        cluster = builder.Build();
        await cluster.DeployAsync();

        table = cluster.GetSiloServiceProvider().GetRequiredService<IReminderTable>();
    }

    public ValueTask DisposeAsync() => cluster.DisposeAsync();

    private static GrainId NewGrainId() =>
        GrainId.Create(GrainType.Create("test-grain"), IdSpan.Create(Guid.NewGuid().ToString("N")));

    private static ReminderEntry MakeEntry(
        GrainId grainId,
        string name,
        DateTime? startAt = null,
        TimeSpan? period = null,
        string eTag = "") =>
        new()
        {
            GrainId = grainId,
            ReminderName = name,
            StartAt = startAt ?? DateTime.UtcNow,
            Period = period ?? TimeSpan.FromMinutes(1),
            ETag = eTag
        };

    // ReadRow tests

    [Fact(Timeout = Timeout)]
    public async Task ReadRow_ReturnsNull_WhenReminderDoesNotExist()
    {
        await table.TestOnlyClearTable();

        var result = await table.ReadRow(NewGrainId(), "missing");

        Assert.Null(result);
    }

    [Fact(Timeout = Timeout)]
    public async Task ReadRow_ReturnsEntry_AfterUpsert()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();
        var startAt = DateTime.UtcNow;
        var period = TimeSpan.FromMinutes(5);

        var etag = await table.UpsertRow(MakeEntry(grainId, "r1", startAt, period));
        var result = await table.ReadRow(grainId, "r1");

        Assert.NotNull(result);
        Assert.Equal(grainId, result.GrainId);
        Assert.Equal("r1", result.ReminderName);
        Assert.Equal(startAt, result.StartAt);
        Assert.Equal(period, result.Period);
        Assert.Equal(etag, result.ETag);
    }

    // ReadRows(GrainId) tests

    [Fact(Timeout = Timeout)]
    public async Task ReadRows_ByGrainId_ReturnsEmptyTable_WhenNoReminders()
    {
        await table.TestOnlyClearTable();

        var result = await table.ReadRows(NewGrainId());

        Assert.Empty(result.Reminders);
    }

    [Fact(Timeout = Timeout)]
    public async Task ReadRows_ByGrainId_ReturnsOnlyEntriesForThatGrain()
    {
        await table.TestOnlyClearTable();
        var grainId1 = NewGrainId();
        var grainId2 = NewGrainId();

        await table.UpsertRow(MakeEntry(grainId1, "a"));
        await table.UpsertRow(MakeEntry(grainId1, "b"));
        await table.UpsertRow(MakeEntry(grainId2, "c"));

        var result1 = await table.ReadRows(grainId1);
        var result2 = await table.ReadRows(grainId2);

        Assert.Equal(2, result1.Reminders.Count);
        Assert.All(result1.Reminders, r => Assert.Equal(grainId1, r.GrainId));
        var single = Assert.Single(result2.Reminders);
        Assert.Equal(grainId2, single.GrainId);
    }

    // ReadRows(uint begin, uint end) tests

    [Fact(Timeout = Timeout)]
    public async Task ReadRows_ByHashRange_NormalRange_IncludesHashAtEndOfRange()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();
        await table.UpsertRow(MakeEntry(grainId, "r1"));
        var hash = grainId.GetUniformHashCode();

        // Normal range (begin < end): hash > begin && hash <= end
        // (hash-1, hash) → includes this grain (hash > hash-1 && hash <= hash)
        var included = await table.ReadRows(unchecked(hash - 1), hash);
        // (hash, hash+1) → excludes this grain (hash > hash is false)
        var excluded = await table.ReadRows(hash, unchecked(hash + 1));

        Assert.Contains(included.Reminders, r => r.GrainId == grainId);
        Assert.DoesNotContain(excluded.Reminders, r => r.GrainId == grainId);
    }

    [Fact(Timeout = Timeout)]
    public async Task ReadRows_ByHashRange_WrapAround_IncludesHashBelowOrEqualEnd()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();
        await table.UpsertRow(MakeEntry(grainId, "r1"));
        var hash = grainId.GetUniformHashCode();

        // Wrap-around range (begin >= end): hash > begin || hash <= end
        // (hash+1, hash) → begin > end → hash > hash+1 is false, hash <= hash is true → includes
        var included = await table.ReadRows(unchecked(hash + 1), hash);
        // (hash, hash-1) → begin > end → hash > hash is false, hash <= hash-1 is false → excludes
        var excluded = await table.ReadRows(hash, unchecked(hash - 1));

        Assert.Contains(included.Reminders, r => r.GrainId == grainId);
        Assert.DoesNotContain(excluded.Reminders, r => r.GrainId == grainId);
    }

    // UpsertRow tests

    [Fact(Timeout = Timeout)]
    public async Task UpsertRow_ReturnsNonEmptyETag()
    {
        await table.TestOnlyClearTable();

        var etag = await table.UpsertRow(MakeEntry(NewGrainId(), "r1"));

        Assert.NotEmpty(etag);
    }

    [Fact(Timeout = Timeout)]
    public async Task UpsertRow_UpdatesExistingEntry_AndReturnsNewETag()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();

        var etag1 = await table.UpsertRow(MakeEntry(grainId, "r1", period: TimeSpan.FromMinutes(1)));
        var etag2 = await table.UpsertRow(MakeEntry(grainId, "r1", period: TimeSpan.FromMinutes(10), eTag: etag1));

        Assert.NotEqual(etag1, etag2);
        var result = await table.ReadRow(grainId, "r1");
        Assert.NotNull(result);
        Assert.Equal(TimeSpan.FromMinutes(10), result.Period);
        Assert.Equal(etag2, result.ETag);
    }

    [Fact(Timeout = Timeout)]
    public async Task UpsertRow_Throws_WhenETagIsStale()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();

        var etag1 = await table.UpsertRow(MakeEntry(grainId, "r1"));
        await table.UpsertRow(MakeEntry(grainId, "r1", eTag: etag1)); // advance the etag

        // etag1 is now stale
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.UpsertRow(MakeEntry(grainId, "r1", eTag: etag1)));
    }

    // RemoveRow tests

    [Fact(Timeout = Timeout)]
    public async Task RemoveRow_ReturnsFalse_WhenReminderDoesNotExist()
    {
        await table.TestOnlyClearTable();

        var removed = await table.RemoveRow(NewGrainId(), "missing", "any-etag");

        Assert.False(removed);
    }

    [Fact(Timeout = Timeout)]
    public async Task RemoveRow_ReturnsTrue_AndDeletesEntry_WhenETagMatches()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();
        var etag = await table.UpsertRow(MakeEntry(grainId, "r1"));

        var removed = await table.RemoveRow(grainId, "r1", etag);

        Assert.True(removed);
        Assert.Null(await table.ReadRow(grainId, "r1"));
    }

    [Fact(Timeout = Timeout)]
    public async Task RemoveRow_Throws_WhenETagMismatch()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();
        await table.UpsertRow(MakeEntry(grainId, "r1"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            table.RemoveRow(grainId, "r1", "wrong-etag"));
    }

    [Fact(Timeout = Timeout)]
    public async Task RemoveRow_ReturnsFalse_AfterAlreadyRemoved()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();
        var etag = await table.UpsertRow(MakeEntry(grainId, "r1"));
        await table.RemoveRow(grainId, "r1", etag);

        var removed = await table.RemoveRow(grainId, "r1", etag);

        Assert.False(removed);
    }

    // TestOnlyClearTable test

    [Fact(Timeout = Timeout)]
    public async Task TestOnlyClearTable_RemovesAllReminders()
    {
        var grainId = NewGrainId();
        await table.UpsertRow(MakeEntry(grainId, "r1"));
        await table.UpsertRow(MakeEntry(grainId, "r2"));

        await table.TestOnlyClearTable();

        // ReadRows(0, 0): 0 >= 0 → wrap-around → hash > 0 || hash <= 0 covers all uint values
        var allReminders = await table.ReadRows(0, 0);
        Assert.Empty(allReminders.Reminders);
    }

    // Persistence tests

    [Fact(Timeout = Timeout)]
    public async Task StateIsPreserved_AfterGrainDeactivation()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();
        var etag = await table.UpsertRow(MakeEntry(grainId, "r1", period: TimeSpan.FromMinutes(3)));

        var grain = cluster.Client.GetGrain<IReminderTableGrain>(default(IdSpan));
        await grain.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        // The next read reactivates the grain and replays all committed events from Kurrent.
        var result = await table.ReadRow(grainId, "r1");

        Assert.NotNull(result);
        Assert.Equal(grainId, result.GrainId);
        Assert.Equal(TimeSpan.FromMinutes(3), result.Period);
        Assert.Equal(etag, result.ETag);
    }

    [Fact(Timeout = Timeout)]
    public async Task UpsertRow_AfterClearTable_Succeeds()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();
        await table.UpsertRow(MakeEntry(grainId, "before-clear"));

        // ClearedV1 carries [DiscardPriorEvents] which triggers a Kurrent TruncateBefore operation.
        // Subsequent appends must still use the correct ConfirmedVersion.
        await table.TestOnlyClearTable();

        var etag = await table.UpsertRow(MakeEntry(grainId, "after-clear", period: TimeSpan.FromMinutes(2)));
        var result = await table.ReadRow(grainId, "after-clear");

        Assert.NotNull(result);
        Assert.Equal(TimeSpan.FromMinutes(2), result.Period);
        Assert.Equal(etag, result.ETag);
        Assert.Null(await table.ReadRow(grainId, "before-clear"));
    }

    // Multi-reminder and multi-grain tests

    [Fact(Timeout = Timeout)]
    public async Task RemoveRow_OneOfMultiple_LeavesOtherIntact()
    {
        await table.TestOnlyClearTable();
        var grainId = NewGrainId();
        var etag1 = await table.UpsertRow(MakeEntry(grainId, "r1"));
        await table.UpsertRow(MakeEntry(grainId, "r2"));

        await table.RemoveRow(grainId, "r1", etag1);

        Assert.Null(await table.ReadRow(grainId, "r1"));
        Assert.NotNull(await table.ReadRow(grainId, "r2"));

        var remaining = await table.ReadRows(grainId);
        var single = Assert.Single(remaining.Reminders);
        Assert.Equal("r2", single.ReminderName);
    }

    [Fact(Timeout = Timeout)]
    public async Task ReadRows_ByHashRange_WithMultipleGrains_FiltersCorrectly()
    {
        await table.TestOnlyClearTable();
        var grainId1 = NewGrainId();
        var grainId2 = NewGrainId();
        await table.UpsertRow(MakeEntry(grainId1, "r1"));
        await table.UpsertRow(MakeEntry(grainId2, "r2"));

        var hash1 = grainId1.GetUniformHashCode();

        // (hash1-1, hash1] is a one-slot range: includes exactly grainId1 but not grainId2,
        // provided the two grains have different hashes (overwhelmingly likely for random GUIDs).
        var result = await table.ReadRows(unchecked(hash1 - 1), hash1);

        Assert.Contains(result.Reminders, r => r.GrainId == grainId1);
        Assert.DoesNotContain(result.Reminders, r => r.GrainId == grainId2);
    }
}
