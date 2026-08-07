using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.EventSourcing.Kurrent.Clustering;
using Orleans.EventSourcing.Kurrent.Configuration;
using Orleans.EventSourcing.Kurrent.Storage;

namespace Orleans.EventSourcing.Kurrent.Tests;

public sealed class MembershipTableTests
{
    private static KurrentMembershipTable CreateTable(out ClusterOptions clusterOptions)
        => CreateTable(out clusterOptions, out _);

    private static KurrentMembershipTable CreateTable(out ClusterOptions clusterOptions, out InMemoryKurrentClient client)
    {
        clusterOptions = new ClusterOptions
        {
            ClusterId = "test-cluster",
            ServiceId = Guid.NewGuid().ToString("N")
        };

        // A fresh InMemoryKurrentClient keeps each test isolated.
        var jsonSerializerOptions = new JsonSerializerOptions
        {
            Converters = { new SiloAddressJsonConverter() }
        };

        client = new InMemoryKurrentClient();
        return new KurrentMembershipTable(new KurrentMembershipEventStorage(client, Options.Create(new KurrentClusteringOptions()), Options.Create(clusterOptions), jsonSerializerOptions));
    }

    private static int nextPort = 11_111;

    private static SiloAddress CreateSiloAddress()
        => SiloAddress.New(IPAddress.Loopback, Interlocked.Increment(ref nextPort), generation: 0);

    private static MembershipEntry CreateEntry(SiloAddress address, SiloStatus status = SiloStatus.Active, DateTime? startTime = null)
        => new()
        {
            SiloAddress = address,
            Status = status,
            SiloName = $"Silo_{address.Endpoint.Port}",
            HostName = "localhost",
            RoleName = "test",
            ProxyPort = address.Endpoint.Port,
            StartTime = startTime ?? DateTime.UtcNow,
            IAmAliveTime = startTime ?? DateTime.UtcNow,
            SuspectTimes = []
        };

    [Fact]
    public async Task InsertRow_AddsSilo()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var initial = await table.ReadAll();
        var address = CreateSiloAddress();

        Assert.True(await table.InsertRow(CreateEntry(address), initial.Version));

        var data = await table.ReadAll();
        var member = Assert.Single(data.Members);
        Assert.Equal(address, member.Item1.SiloAddress);
        Assert.Equal(SiloStatus.Active, member.Item1.Status);
    }

    [Fact]
    public async Task InsertRow_IsRejected_WhenTableVersionIsStale()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var initial = await table.ReadAll();
        Assert.True(await table.InsertRow(CreateEntry(CreateSiloAddress()), initial.Version));

        // Re-using the stale version must be rejected.
        Assert.False(await table.InsertRow(CreateEntry(CreateSiloAddress()), initial.Version));
    }

    [Fact]
    public async Task InsertRow_IsRejected_ForDuplicateSilo()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var address = CreateSiloAddress();
        var version = (await table.ReadAll()).Version;
        Assert.True(await table.InsertRow(CreateEntry(address), version));

        version = (await table.ReadAll()).Version;
        Assert.False(await table.InsertRow(CreateEntry(address), version));
    }

    [Fact]
    public async Task InsertRow_AddsMultipleSilos()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var first = CreateSiloAddress();
        var second = CreateSiloAddress();

        Assert.True(await table.InsertRow(CreateEntry(first), (await table.ReadAll()).Version));
        Assert.True(await table.InsertRow(CreateEntry(second), (await table.ReadAll()).Version));

        var data = await table.ReadAll();
        Assert.Equal(2, data.Members.Count);
        Assert.Contains(data.Members, m => m.Item1.SiloAddress.Equals(first));
        Assert.Contains(data.Members, m => m.Item1.SiloAddress.Equals(second));
    }

    [Fact]
    public async Task ReadRow_ReturnsSingleSilo()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var address = CreateSiloAddress();
        await table.InsertRow(CreateEntry(address), (await table.ReadAll()).Version);

        var data = await table.ReadRow(address);
        var member = Assert.Single(data.Members);
        Assert.Equal(address, member.Item1.SiloAddress);
    }

    [Fact]
    public async Task ReadRow_ReturnsEmpty_ForUnknownSilo()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var data = await table.ReadRow(CreateSiloAddress());
        Assert.Empty(data.Members);
    }

    [Fact]
    public async Task UpdateRow_ChangesStatus()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var address = CreateSiloAddress();
        await table.InsertRow(CreateEntry(address), (await table.ReadAll()).Version);

        var row = await table.ReadRow(address);
        var (entry, etag) = row.Members.Single();
        entry.Status = SiloStatus.Dead;

        Assert.True(await table.UpdateRow(entry, etag, row.Version));

        var updated = await table.ReadRow(address);
        Assert.Equal(SiloStatus.Dead, updated.Members.Single().Item1.Status);
    }

    [Fact]
    public async Task UpdateRow_IsRejected_WithStaleEtag()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var address = CreateSiloAddress();
        await table.InsertRow(CreateEntry(address), (await table.ReadAll()).Version);

        var row = await table.ReadRow(address);
        var (entry, _) = row.Members.Single();
        entry.Status = SiloStatus.Dead;

        Assert.False(await table.UpdateRow(entry, "not-a-real-etag", row.Version));
    }

    [Fact]
    public async Task UpdateIAmAlive_UpdatesLivenessTimestamp()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var address = CreateSiloAddress();
        await table.InsertRow(CreateEntry(address), (await table.ReadAll()).Version);

        var newAliveTime = DateTime.UtcNow.AddMinutes(5);
        var aliveEntry = CreateEntry(address);
        aliveEntry.IAmAliveTime = newAliveTime;

        await table.UpdateIAmAlive(aliveEntry);

        var data = await table.ReadRow(address);
        Assert.Equal(newAliveTime, data.Members.Single().Item1.IAmAliveTime);
    }

    [Fact]
    public async Task CleanupDefunctSiloEntries_RemovesOldSilos()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var oldSilo = CreateSiloAddress();
        var recentSilo = CreateSiloAddress();
        var cutoff = DateTimeOffset.UtcNow;

        await table.InsertRow(CreateEntry(oldSilo, startTime: cutoff.AddHours(-1).UtcDateTime), (await table.ReadAll()).Version);
        await table.InsertRow(CreateEntry(recentSilo, startTime: cutoff.AddHours(1).UtcDateTime), (await table.ReadAll()).Version);

        // Mark the old silo as Dead so CleanupDefunctSiloEntries considers it eligible for removal.
        var oldSiloRow = await table.ReadRow(oldSilo);
        var (oldEntry, oldEtag) = oldSiloRow.Members.Single();
        oldEntry.Status = SiloStatus.Dead;
        await table.UpdateRow(oldEntry, oldEtag, oldSiloRow.Version);

        await table.CleanupDefunctSiloEntries(cutoff);

        var data = await table.ReadAll();
        var member = Assert.Single(data.Members);
        Assert.Equal(recentSilo, member.Item1.SiloAddress);
    }

    [Fact]
    public async Task DeleteMembershipTableEntries_RemovesAllSilos()
    {
        var table = CreateTable(out var clusterOptions);
        await table.InitializeMembershipTable(true);

        await table.InsertRow(CreateEntry(CreateSiloAddress()), (await table.ReadAll()).Version);
        await table.InsertRow(CreateEntry(CreateSiloAddress()), (await table.ReadAll()).Version);

        await table.DeleteMembershipTableEntries(clusterOptions.ClusterId);

        var data = await table.ReadAll();
        Assert.Empty(data.Members);
    }

    [Fact]
    public async Task DeleteMembershipTableEntries_IgnoresOtherClusters()
    {
        var table = CreateTable(out _);
        await table.InitializeMembershipTable(true);

        var address = CreateSiloAddress();
        await table.InsertRow(CreateEntry(address), (await table.ReadAll()).Version);

        await table.DeleteMembershipTableEntries("some-other-cluster");

        var data = await table.ReadAll();
        Assert.Single(data.Members);
    }
}
