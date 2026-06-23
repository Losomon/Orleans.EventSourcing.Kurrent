# Copilot instructions for Orleans.EventSourcing.Kurrent

## Build, test, and pack

Use the solution file for restore/build and the test project directly for test runs:

```powershell
dotnet restore .\Orleans.EventSourcing.Kurrent.slnx
dotnet build .\Orleans.EventSourcing.Kurrent.slnx --configuration Release --no-restore
dotnet test .\tests\Orleans.EventSourcing.Kurrent.Tests\Orleans.EventSourcing.Kurrent.Tests.csproj -f net8.0 --no-restore --logger "console;verbosity=minimal"
dotnet test .\tests\Orleans.EventSourcing.Kurrent.Tests\Orleans.EventSourcing.Kurrent.Tests.csproj -f net8.0 --no-restore --filter "FullyQualifiedName~Orleans.EventSourcing.Kurrent.Tests.BasicTests.DepositWithdraw" --logger "console;verbosity=minimal"
dotnet pack .\src\Orleans.EventSourcing.Kurrent\Orleans.EventSourcing.Kurrent.csproj --configuration Release --no-build --output .\artifacts
```

There is no separate lint command. Static analysis runs as part of normal builds because the packable projects set `AnalysisLevel=latest-all` and `TreatWarningsAsErrors=true`.

## High-level architecture

- `src\Orleans.EventSourcing.Kurrent` contains the main provider package. It exposes two persistence paths that share the same Kurrent abstractions:
  - the log-consistency provider for `JournaledGrain<TView, TEntry>`
  - the grain-storage provider for `[PersistentState]` / `IGrainStorage`
- Registration happens in `Hosting\KurrentStorageSiloBuilderExtensions.cs` and `Hosting\KurrentStorageServiceCollectionExtensions.cs`. Both providers are wired through keyed DI by provider name, and both read the same `KurrentStorageOptions` shape.
- The log-consistency path is centered on `Storage\KurrentLogViewAdapter.cs`. It appends grain events to one Kurrent stream per grain, rebuilds confirmed/tentative views by replaying the stream, and handles business-level truncation markers.
- The grain-storage path is `Storage\KurrentGrainStorageProvider.cs`. It stores snapshots as a stream that is forced back to `MaxCount = 1`, so it behaves like latest-state storage rather than an event log.
- `Projections\KurrentGrainEventProvider.cs` implements the experimental `IGrainEventProvider` by reading Kurrent's `$all` stream, filtering to this package's stream names or aliased event types, then yielding `GrainEvent`, `Checkpoint`, `CaughtUp`, and `FallenBehind` updates.
- `src\Orleans.EventSourcing.Kurrent.Reminders` is a separate package. It implements Orleans reminders as a single `JournaledGrain` (`KurrentReminderTableGrain`) behind an `IReminderTable` proxy, and it registers its own dedicated log-consistency provider name.

## Key conventions

- Stable event contracts rely on Orleans `[Alias]`. The default converters write the alias into Kurrent `EventType`, and projection filters use the same alias resolution. When adding persisted event types, add `[Alias]` and keep it stable across refactors.
- Stream names are not arbitrary strings. `KurrentStreamName` uses escaped structural formats:
  - event-sourced streams: `{GrainType}-{Key}`
  - grain-state streams: `{GrainType}-{stateName}|{Key}`
  Reserved characters are escaped, and `StreamNameTests` covers the required round-trips.
- State storage is intentionally snapshot-only. `WriteStateAsync` appends one event and then ensures stream metadata has `MaxCount = 1`; `ClearStateAsync` performs a soft delete instead of writing a tombstone event.
- Event-stream truncation is a first-class convention, not an ad hoc cleanup step. Applying `[DiscardPriorEvents]` to an event tells the log-view adapter to rebuild from that marker and issue a Kurrent truncate-before operation.
- Experimental APIs are used on purpose. `IGrainEventProvider` and `DiscardPriorEventsAttribute` emit `OEK0001` / `OEK0002`; existing code suppresses them explicitly in project files or localized pragmas rather than disabling broader warnings.
- Tests commonly use `InProcessTestCluster` plus the in-memory Kurrent implementation. `KurrentClientFactory` treats `esdb://kurrentemulator:2113?tls=false` as the trigger for the in-memory client, so many tests do not require a real KurrentDB instance.
- Versions and packages are tag-driven. Packable projects use MinVer with `v` tags, and CI/release workflows build, test, pack, then publish artifacts from the package project under `src\Orleans.EventSourcing.Kurrent`.
