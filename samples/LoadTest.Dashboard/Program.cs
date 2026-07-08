using KurrentDB.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;
using Orleans.Dashboard;
using Orleans.EventSourcing.Kurrent.Hosting;

// Connection string for KurrentDB. The special host "kurrentemulator" selects the in-process,
// in-memory Kurrent implementation so the load test runs without external infrastructure.
// Point KURRENT_CONNECTION_STRING at a real KurrentDB instance to measure end-to-end performance.
var connectionString = Environment.GetEnvironmentVariable("KURRENT_CONNECTION_STRING")
                       ?? "esdb://localhost:2113?tls=false";

var clientSettings = KurrentDBClientSettings.Create(connectionString);

// Create a separate web application for the dashboard
var dashboardBuilder = WebApplication.CreateBuilder(args);

// Configure Orleans client
dashboardBuilder.UseOrleansClient(clientBuilder =>
{
    clientBuilder.UseKurrentClustering(options => options.ClientSettings = clientSettings);
        
    // Add dashboard services to the client
    clientBuilder.AddDashboard();
});

var dashboardApp = dashboardBuilder.Build();

// Map dashboard endpoints on the client
dashboardApp.MapOrleansDashboard();

await dashboardApp.RunAsync();
