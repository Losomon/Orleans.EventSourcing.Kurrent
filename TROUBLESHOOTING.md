# Troubleshooting

## Windows: tests fail with `0x800711C7`

### Symptom

Most tests fail with:

```text
System.IO.FileLoadException: Could not load file or assembly 'KurrentDB.Client.dll'.
An Application Control policy has blocked this file. (0x800711C7)
```

The build may also fail earlier with an `MSB3073` error involving `MinVer.dll`.

### Cause

Windows Smart App Control (SAC) can block unsigned assemblies. On affected Windows installations, this can prevent `KurrentDB.Client.dll` from being loaded during test execution and `MinVer.dll` from running during the build.

### Fix

1. Open **Windows Security**
2. Go to **App & browser control** → **Smart App Control settings**
3. If Smart App Control is blocking the assemblies, set it to **Off**
4. Re-run the build and tests

For example:

```powershell
dotnet test tests\Orleans.EventSourcing.Kurrent.Tests\Orleans.EventSourcing.Kurrent.Tests.csproj -f net10.0
```

> **Note:** Disabling Smart App Control changes a Windows security setting. Consider the security implications before doing so on your development machine.

After disabling SAC, the test suite completed successfully on Windows 11 with .NET SDK 10.0.401, with **88/88 tests passing** for `net10.0`.

If testing the `net8.0` target, ensure the .NET 8 runtime/SDK is installed. Running `net8.0` tests on a newer runtime through roll-forward does not provide the same validation as running them on the actual .NET 8 runtime.
