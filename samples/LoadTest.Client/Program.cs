using KurrentDB.Client;
using LoadTest.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.EventSourcing.Kurrent.Hosting;
using System.Diagnostics;

// ---------------------------------------------------------------------------
// LoadTest.Client — drives load against the LoadTest.Silo to validate the
// performance and scalability of the Kurrent log-consistency provider.
//
// Usage: dotnet run --project samples\LoadTest.Client -- [options]
//   --grains <n>             number of event-sourced account grains   (default 100)
//   --workers <n>            number of concurrent client workers      (default 32)
//   --duration <seconds>     duration of the load phase               (default 90, minimum 90)
//   --reminder-fraction <f>  fraction of grains with an active reminder (default 0.1)
//   --observe-reminders <s>  seconds to wait for reminder ticks after the run (default 0)
// ---------------------------------------------------------------------------

var options = LoadTestOptions.Parse(args);

// Connection string for KurrentDB. The special host "kurrentemulator" selects the in-process,
// in-memory Kurrent implementation so the load test runs without external infrastructure.
// Point KURRENT_CONNECTION_STRING at a real KurrentDB instance to measure end-to-end performance.
var connectionString = Environment.GetEnvironmentVariable("KURRENT_CONNECTION_STRING")
                       ?? "esdb://kurrentemulator:2113?tls=false";

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.AddFilter("Orleans", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.UseOrleansClient(client => client.UseKurrentClustering(x => x.ClientSettings = KurrentDBClientSettings.Create(connectionString)));

using var host = builder.Build();
await host.StartAsync();
var clusterClient = host.Services.GetRequiredService<IClusterClient>();

Console.WriteLine($"Connected. grains={options.Grains} workers={options.Workers} duration={options.DurationSeconds}s reminder-fraction={options.ReminderFraction:P0}");

var grainIds = Enumerable.Range(0, options.Grains).Select(_ => Guid.NewGuid()).ToArray();
var expectedCents = new long[options.Grains];
var reminderGrainCount = (int)Math.Ceiling(options.Grains * options.ReminderFraction);
var reminderTicksBeforeLoad = new int[options.Grains];
var shouldValidateReminderTicks = reminderGrainCount > 0;

// --- Warm-up: activate every grain (first activation replays/creates its Kurrent stream).
var warmupWatch = Stopwatch.StartNew();
await Parallel.ForAsync(0, options.Grains, async (i, ct) =>
{
    var grain = clusterClient.GetGrain<IAccountGrain>(grainIds[i]);
    reminderTicksBeforeLoad[i] = (await grain.GetSummary()).InterestTicks;
    if (i < reminderGrainCount)
    {
        // Orleans reminders have a 1 minute minimum period. The load test enforces a minimum
        // duration of 90 seconds so reminder firing is part of every run.
        await grain.EnableInterestReminder(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }
});
warmupWatch.Stop();
Console.WriteLine($"Warm-up: activated {options.Grains} grains and registered {reminderGrainCount} reminders in {warmupWatch.Elapsed.TotalSeconds:F1}s");

// --- Load phase.
var stats = new WorkerStats[options.Workers];
var deadline = Stopwatch.StartNew();
var duration = TimeSpan.FromSeconds(options.DurationSeconds);

var workers = Enumerable.Range(0, options.Workers).Select(async workerIndex =>
{
    var random = new Random(unchecked(Environment.TickCount * 397) ^ workerIndex);
    var workerStats = stats[workerIndex] = new WorkerStats();
    var opWatch = new Stopwatch();

    while (deadline.Elapsed < duration)
    {
        var grainIndex = random.Next(options.Grains);
        var grain = clusterClient.GetGrain<IAccountGrain>(grainIds[grainIndex]);
        var roll = random.Next(100);

        try
        {
            opWatch.Restart();
            if (roll < 60)
            {
                // 60% deposits
                var amount = random.Next(1, 101);
                await grain.Deposit(amount);
                Interlocked.Add(ref expectedCents[grainIndex], amount * 100L);
                workerStats.Record(OpType.Deposit, opWatch.Elapsed);
            }
            else if (roll < 85)
            {
                // 25% withdrawals (grain rejects overdrafts)
                var amount = random.Next(1, 51);
                if (await grain.Withdraw(amount))
                {
                    Interlocked.Add(ref expectedCents[grainIndex], -amount * 100L);
                }
                workerStats.Record(OpType.Withdraw, opWatch.Elapsed);
            }
            else if (roll < 98)
            {
                // 13% reads
                await grain.GetBalance();
                workerStats.Record(OpType.Read, opWatch.Elapsed);
            }
            else
            {
                // 2% reminder churn — register/unregister exercises the Kurrent reminder table under load.
                if (reminderGrainCount < options.Grains)
                {
                    var churnGrain = clusterClient.GetGrain<IAccountGrain>(grainIds[random.Next(reminderGrainCount, options.Grains)]);
                    await churnGrain.EnableInterestReminder(TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(2));
                    await churnGrain.DisableInterestReminder();
                    workerStats.Record(OpType.ReminderChurn, opWatch.Elapsed);
                }
            }
        }
        catch (Exception ex)
        {
            workerStats.RecordError(ex);
        }
    }
}).ToArray();

await Task.WhenAll(workers);
var elapsed = deadline.Elapsed;

// --- Report throughput and latency.
var merged = WorkerStats.Merge(stats);
var totalOps = merged.Values.Sum(latencies => latencies.Count);
var totalErrors = stats.Sum(s => s.Errors);

Console.WriteLine();
Console.WriteLine($"=== Load phase complete: {totalOps:N0} ops in {elapsed.TotalSeconds:F1}s = {totalOps / elapsed.TotalSeconds:N0} ops/sec ({totalErrors} errors) ===");
Console.WriteLine($"{"operation",-14} {"count",10} {"mean",9} {"p50",9} {"p95",9} {"p99",9} {"max",9}");
foreach (var (op, latencies) in merged.OrderBy(kv => kv.Key))
{
    if (latencies.Count == 0)
    {
        continue;
    }

    latencies.Sort();
    Console.WriteLine($"{op,-14} {latencies.Count,10:N0} {latencies.Average(),7:F1}ms {Percentile(latencies, 0.50),7:F1}ms {Percentile(latencies, 0.95),7:F1}ms {Percentile(latencies, 0.99),7:F1}ms {latencies[^1],7:F1}ms");
}

if (totalErrors > 0)
{
    foreach (var error in stats.SelectMany(s => s.SampleErrors).Take(5))
    {
        Console.WriteLine($"  error: {error}");
    }
}

// --- Optionally wait so the 1-minute reminders get a chance to tick.
if (options.ObserveRemindersSeconds > 0)
{
    Console.WriteLine($"Waiting {options.ObserveRemindersSeconds}s for reminder ticks...");
    await Task.Delay(TimeSpan.FromSeconds(options.ObserveRemindersSeconds));
}

// --- Validation: replayed, confirmed grain state must match the client-side ledger exactly.
Console.WriteLine();
Console.WriteLine("Validating confirmed state against client-side ledger...");
var mismatches = 0;
var reminderMismatches = 0;
var totalEvents = 0L;
var totalInterestTicks = 0;
var remindersActive = 0;

for (var i = 0; i < options.Grains; i++)
{
    var summary = await clusterClient.GetGrain<IAccountGrain>(grainIds[i]).GetSummary();
    totalEvents += summary.ConfirmedVersion;
    totalInterestTicks += summary.InterestTicks;
    if (summary.ReminderActive)
    {
        remindersActive++;
    }

    var expected = expectedCents[i] / 100m;
    if (summary.Balance != expected)
    {
        mismatches++;
        Console.WriteLine($"  MISMATCH grain {grainIds[i]}: expected {expected}, confirmed {summary.Balance}");
    }

    if (shouldValidateReminderTicks && i < reminderGrainCount && summary.InterestTicks <= reminderTicksBeforeLoad[i])
    {
        reminderMismatches++;
        Console.WriteLine($"  REMINDER NOT OBSERVED grain {grainIds[i]}: expected interest ticks to increase from {reminderTicksBeforeLoad[i]}, observed {summary.InterestTicks}");
    }
}

Console.WriteLine($"Confirmed events across all grains: {totalEvents:N0}");
Console.WriteLine($"Reminders active: {remindersActive} (expected >= {reminderGrainCount}), interest ticks observed: {totalInterestTicks}");
Console.WriteLine(mismatches == 0 && reminderMismatches == 0
    ? $"VALIDATION PASSED: all {options.Grains} grain balances match the client-side ledger and reminder ticks were observed as expected."
    : $"VALIDATION FAILED: {mismatches} balance mismatch(es), {reminderMismatches} reminder grain(s) did not observe a tick.");

await host.StopAsync();
return mismatches == 0 && reminderMismatches == 0 && totalErrors == 0 ? 0 : 1;

static double Percentile(List<double> sorted, double percentile)
    => sorted[Math.Min(sorted.Count - 1, (int)Math.Ceiling(percentile * sorted.Count) - 1)];

internal enum OpType
{
    Deposit,
    Withdraw,
    Read,
    ReminderChurn,
}

internal sealed class WorkerStats
{
    private readonly Dictionary<OpType, List<double>> _latencies = new();
    private readonly List<string> _sampleErrors = [];

    public int Errors { get; private set; }

    public IReadOnlyList<string> SampleErrors => _sampleErrors;

    public void Record(OpType op, TimeSpan latency)
    {
        if (!_latencies.TryGetValue(op, out var list))
        {
            _latencies[op] = list = [];
        }

        list.Add(latency.TotalMilliseconds);
    }

    public void RecordError(Exception ex)
    {
        Errors++;
        if (_sampleErrors.Count < 5)
        {
            _sampleErrors.Add($"{ex.GetType().Name}: {ex.Message}");
        }
    }

    public static Dictionary<OpType, List<double>> Merge(IEnumerable<WorkerStats> all)
    {
        var merged = new Dictionary<OpType, List<double>>();
        foreach (var stats in all)
        {
            foreach (var (op, latencies) in stats._latencies)
            {
                if (!merged.TryGetValue(op, out var list))
                {
                    merged[op] = list = [];
                }

                list.AddRange(latencies);
            }
        }

        return merged;
    }
}

internal sealed record LoadTestOptions
{
    public int Grains { get; init; } = 100;
    public int Workers { get; init; } = 32;
    public int DurationSeconds { get; init; } = 90;
    public double ReminderFraction { get; init; } = 0.1;
    public int ObserveRemindersSeconds { get; init; }

    public static LoadTestOptions Parse(string[] args)
    {
        var options = new LoadTestOptions();
        for (var i = 0; i + 1 < args.Length; i += 2)
        {
            options = args[i] switch
            {
                "--grains" => options with { Grains = int.Parse(args[i + 1]) },
                "--workers" => options with { Workers = int.Parse(args[i + 1]) },
                "--duration" => options with { DurationSeconds = int.Parse(args[i + 1]) },
                "--reminder-fraction" => options with { ReminderFraction = double.Parse(args[i + 1]) },
                "--observe-reminders" => options with { ObserveRemindersSeconds = int.Parse(args[i + 1]) },
                _ => options,
            };
        }

        if (options.DurationSeconds < 90)
        {
            throw new ArgumentOutOfRangeException(nameof(options.DurationSeconds), options.DurationSeconds, "The load test duration must be at least 90 seconds so 1-minute reminders are exercised.");
        }

        return options;
    }
}
