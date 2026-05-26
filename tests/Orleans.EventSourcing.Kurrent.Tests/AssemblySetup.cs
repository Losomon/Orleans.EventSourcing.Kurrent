using System.Diagnostics;
using System.Runtime.CompilerServices;

// Tests depend on shared state and cluster
[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Orleans.EventSourcing.Kurrent.Tests;

internal static class AssemblySetup
{
    [ModuleInitializer]
    public static void Init() => Trace.Listeners.Add(new ConsoleTraceListener());
}
