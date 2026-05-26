using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Orleans.EventSourcing.Kurrent.Tests;

internal static class AssemblySetup
{
    [ModuleInitializer]
    public static void Init() => Trace.Listeners.Add(new ConsoleTraceListener());
}
