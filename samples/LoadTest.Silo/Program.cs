using KurrentDB.Client;
using LoadTest.Silo;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Dashboard;
using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.EventSourcing.Kurrent.Reminders;

// Connection string for KurrentDB. The special host "kurrentemulator" selects the in-process,
// in-memory Kurrent implementation so the load test runs without external infrastructure.
// Point KURRENT_CONNECTION_STRING at a real KurrentDB instance to measure end-to-end performance.
var connectionString = Environment.GetEnvironmentVariable("KURRENT_CONNECTION_STRING")
                       ?? "esdb://kurrentemulator:2113?tls=false";

// GRAIN_SERIALIZER selects the event payload serializer for the log-consistency provider:
//   "orleans" (default) — Orleans binary serializer
//   "stj"               — System.Text.Json
// Note: streams written with one serializer cannot be read with the other, so wipe the
// database (docker compose down -v) when switching.
var serializerName = (Environment.GetEnvironmentVariable("GRAIN_SERIALIZER") ?? "default").ToLowerInvariant();

var clientSettings = KurrentDBClientSettings.Create(connectionString);

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.AddFilter("Orleans", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);

builder.UseOrleans(silo =>
{
    silo.AddDashboard();
    silo.UseKurrentClustering(o => { o.ClientSettings = clientSettings; o.EventCountBeforeSnapshots = 5; });
    //silo.UseLocalhostClustering();
    silo.AddKurrentBasedLogConsistencyProviderAsDefault(o =>
    {
        o.ClientSettings = clientSettings;
        if (serializerName == "stj")
        {
            o.GrainStorageSerializer = new SystemTextJsonGrainStorageSerializer();
        }
    });
    silo.AddKurrentReminderService(o => o.ClientSettings = clientSettings);
});

var host = builder.Build();

Console.WriteLine($"LoadTest silo starting (Kurrent: {connectionString}, serializer: {serializerName})...");
await host.StartAsync();
Console.WriteLine("LoadTest silo is running. Press Ctrl+C to shut down.");
await host.WaitForShutdownAsync();
