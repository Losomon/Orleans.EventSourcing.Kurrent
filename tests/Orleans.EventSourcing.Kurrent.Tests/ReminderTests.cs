using KurrentDB.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.EventSourcing.Kurrent.Reminders;
using Orleans.TestingHost;

namespace Orleans.EventSourcing.Kurrent.Tests;

public sealed class ReminderTests : IAsyncLifetime
{
    private const int IntegrationTestTimeout = 30_000;

    static readonly KurrentDBClientSettings clientSettings = KurrentDBClientSettings.Create("esdb://kurrentemulator:2113?tls=false");

    private InProcessTestCluster cluster = null!;

    public async ValueTask InitializeAsync()
    {        
        var builder = new InProcessTestClusterBuilder();
        builder.ConfigureClient(c => { c.AddActivityPropagation(); });
        builder.ConfigureSilo((s, c) =>
        {
            c.AddKurrentReminderService(o => o.ClientSettings = clientSettings);
        });

        builder.Options.ConfigureFileLogging = false;
        cluster = builder.Build();
        await cluster.DeployAsync();
    }

    public ValueTask DisposeAsync()
        => cluster.DisposeAsync();
     
    [Fact]
    public void ReminderTableRegistration()
        => Assert.IsType<KurrentReminderTableGrainProxy>(cluster.GetSiloServiceProvider().GetService<IReminderTable>());

    [Fact(Timeout = IntegrationTestTimeout)]
    public async Task ReminderCrud()
    {
        var grain = cluster.Client.GetGrain<IReminderTestGrain>(Guid.NewGuid());
        await grain.RegisterReminder("test", TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        Assert.True(await grain.Get("test"));
        await grain.DeleteReminder("test");
        Assert.False(await grain.Get("test"));
    }
}
