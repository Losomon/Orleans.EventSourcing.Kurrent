# Orleans.EventSourcing.Kurrent

[![NuGet](https://img.shields.io/nuget/v/Orleans.EventSourcing.Kurrent.svg)](https://www.nuget.org/packages/Orleans.EventSourcing.Kurrent)

[Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/) providers backed by [KurrentDB](https://kurrent.io/) (formerly EventStoreDB)

This project provides Event Sourcing, State Storage and Reminders providers for Microsoft Orleans backed by KurrentDB.

In addition, it offers some experimental features that are not yet supported by Orleans.EventSourcing, such as event stream truncation and projection Grains.

Event Sourcing is a powerful pattern for modeling complex domains, capturing intent and deriving state. KurrentDB provides an event-native data platform for event-sourcing.

This library aims to provide seamless integration between Orleans and KurrentDB, allowing developers to leverage the strengths of both technologies in their applications.

This library is used in production and has some additional practical features 
* Using Orlean's Alias attribute to decouple KurrentDB's EventType and the .NET type
* Event stream truncation
* Grains that host projections and side-effects

Adding support for storing Grain persistent state and reminders simplifies infrastructure required to host Orleans (perhaps a membership provider is needed next?). Using one storage layer offers a single point-in-time backup of your entire system in KurrentDB, including the state of your Grains and their reminders.

| Provider | Orleans interface | What it persists | When to use |
| --- | --- | --- | --- |
| **Log-consistency provider** (`AddKurrentBasedLogConsistencyProvider*`) | `ILogViewAdaptorFactory` for `JournaledGrain<TView, TEntry>` | The full **event log** for the grain — every `RaiseEvent` is appended to the grain's Kurrent stream. The view is rebuilt by replaying events. | Event-sourced grains (`JournaledGrain`). Gives you full history, projections via `IGrainEventProvider`, and replay. |
| **Grain state storage provider** (`AddKurrentBasedGrainStorageProvider*`) | `IGrainStorage` for `Grain<TState>` / `[PersistentState]` | The **latest snapshot** only — each `WriteStateAsync` appends one event and Kurrent's `MaxCount = 1` stream metadata trims older revisions. | Regular Orleans state persistence when you want it backed by Kurrent (e.g. to keep all storage in one system) |
| **Reminders provider** (`AddKurrentBasedReminderProvider*`) | `IReminderTable` | Grain Reminders | Use this provider to persist and manage Orleans reminders in KurrentDB. |
| **Grain event subscription** (`IGrainEventProvider`) ⚠ experimental | n/a | Read-side subscription to the Kurrent `$all` stream, filtered to the streams produced by the log-consistency provider. | Projections / read models built from grain events emitted via the log-consistency provider. |

## Install

```sh
dotnet add package Orleans.EventSourcing.Kurrent
```

Targets `net8.0` and `net10.0`. Requires Microsoft Orleans 10.x and a KurrentDB / EventStoreDB server.

## Quick start

### Silo configuration

Pick whichever providers you need — they are configured separately and can use different options names if you want different `ClientSettings` per provider:

```csharp
using KurrentDB.Client;
using Orleans.EventSourcing.Kurrent.Hosting;

var settings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");

var builder = Host.CreateApplicationBuilder(args);
builder.UseOrleans(silo =>
{
    // Log-consistency provider — required for JournaledGrain<TView, TEntry>.
    silo.AddKurrentBasedLogConsistencyProviderAsDefault(o => o.ClientSettings = settings);

    // Grain state storage provider — required for Grain<TState> / [PersistentState] backed by Kurrent.
    silo.AddKurrentBasedGrainStorageProviderAsDefault(o => o.ClientSettings = settings);

    // Reminders provider — if you want to persist Orleans reminders in Kurrent.
    silo.AddKurrentBasedReminderProviderAsDefault(o => o.ClientSettings = settings);
});
```

Both extensions also have non-default overloads that take a provider `name` so you can register multiple instances side-by-side.

## Using the log-consistency provider (event-sourced grains)

Inherit from `JournaledGrain<TView, TEntry>`. Events are appended to a Kurrent stream per grain, and the view is rebuilt by replaying them.

```csharp
public sealed class AccountGrain : JournaledGrain<AccountState, AccountEvent>, IAccountGrain
{
    public Task Deposit(decimal amount)
    {
        RaiseEvent(new AccountEvent.Deposited(amount));
        return ConfirmEvents();
    }
}
```

### Subscribing to grain events (projections)

Resolve `IGrainEventProvider` from the silo container and call one of the `SubscribeToGrainEvents*` overloads. This reads the Kurrent `$all` stream filtered to streams written by the log-consistency provider, deserializes each event, and yields it together with the originating `GrainId` and version:

```csharp
await foreach (var update in eventProvider.SubscribeToGrainEvents<IAccountGrain, AccountEvent>(
    subscriber: this.GetGrainId(),
    startingPosition: GlobalEventLogPosition.Start,
    cancellationToken: ct))
{
    switch (update)
    {
        case EventStreamUpdate.GrainEvent<AccountEvent> e:
            // project e.Event for e.GrainId at e.Version
            break;
        case EventStreamUpdate.Checkpoint cp:
            // persist cp.Position so the next subscription resumes from here
            break;
        case EventStreamUpdate.CaughtUp:
            // subscription is now live
            break;
    }
}
```

`IGrainEventProvider` only sees streams produced by the **log-consistency** provider. The state-storage provider's snapshot writes are not surfaced through this API.

## Using the grain state storage provider

Use as a regular Orleans `IGrainStorage` — for example via `[PersistentState]`. Each `WriteStateAsync` appends one event to the grain's stream and Kurrent retains only the latest revision (stream metadata `MaxCount = 1`):

```csharp
public sealed class SettingsGrain(
    [PersistentState(stateName: "settings")] IPersistentState<Settings> state)
    : Grain, ISettingsGrain
{
    public Task<Settings> Get() => Task.FromResult(state.State);

    public Task Set(Settings value)
    {
        state.State = value;
        return state.WriteStateAsync();
    }
}
```

`ClearStateAsync` issues a Kurrent **soft-delete** of the stream (no tombstone event is written).

This provider does **not** keep event history — if you need history, use the log-consistency provider instead.

## Reminders provider

The reminders provider event-sources Orleans reminders to KurrentDB, using a single JournaledGrain implementation that implements the `IReminderTable` interface. 

The current implemenation is intended to offer support for reminders in clusters where the usage will be relatively light, as all reminder data for the cluster is materialised into grain state.

Clusters with heavy reminder usage should use a more traditional reminders provider implementation that persists directly to a database.

## Configuration

`KurrentStorageOptions` is configured per provider name. The same options type is used by both providers; configuring one named instance does not affect the other.

| Property | Description |
| --- | --- |
| `ClientSettings` | `KurrentDBClientSettings` used to build the underlying `KurrentClient`. |
| `GrainStorageSerializer` | Orleans' `IGrainStorageSerializer` used by the default event serializer. |
| `StreamNameProvider` | `IKurrentStreamNameProvider` controlling stream-name layout. Defaults to `KurrentStreamName.Default`. |

### Custom event stream naming

By default each grain's stream is named `{GrainType}-{Key}` (log-consistency) or `{GrainType}-{stateName}|{Key}` (state storage). The naming scheme is pluggable — see [Custom stream naming](#custom-stream-naming).

Both providers, and the projection subscription, route through `IKurrentStreamNameProvider`. The default produces:

- `{GrainType}-{Key}` for log-consistency streams.
- `{GrainType}-{stateName}|{Key}` for state-storage streams.

To change the scheme, implement `IKurrentStreamNameProvider` and assign it on the options for each provider you register:

```csharp
silo.AddKurrentBasedLogConsistencyProviderAsDefault(o =>
{
    o.ClientSettings = settings;
    o.StreamNameProvider = new MyStreamNameProvider();
});

silo.AddKurrentBasedGrainStorageProviderAsDefault(o =>
{
    o.ClientSettings = settings;
    o.StreamNameProvider = new MyStreamNameProvider();
});
```

### Event type names and `[Alias]`

The default serializers (`DefaultEventSerializer<T>` and `EventEnvelopeSerializer<T>`) write each event's CLR type into Kurrent's `EventType` field via Orleans' `TypeConverter.Format(Type)`. That converter honours `[Alias("...")]` from `Orleans.Metadata`:

- If the event type carries `[Alias("MyAlias")]`, the Kurrent `EventType` is `MyAlias`.
- Otherwise, the converter falls back to the Orleans assembly-qualified name format.

Implications:

- **`[Alias]` is the stable contract** between your app and Kurrent. Renaming or moving an event type without an `[Alias]` will break replay because previously written `EventType` strings will no longer resolve. Always add `[Alias]` to event records you persist.
- **The same alias is used on read.** `DeserializeEvent` calls `TypeConverter.Parse(EventType)` to resolve the alias back to a CLR type, so the type must still be reachable in the silo's loaded assemblies under that alias.
- **Projection subscriptions filter by alias.** `IGrainEventProvider.SubscribeToGrainEvents<T>(..., types, ...)` resolves each requested CLR type to its alias via the same `TypeConverter`, then asks Kurrent for events whose `EventType` matches. Event types without an `[Alias]` will still work but couple your subscribers to the assembly-qualified name and break on type rename or relocation.
- **Aliases must be unique** across the silo's serialization graph; this is an Orleans-wide constraint, not specific to this package.

```csharp
[GenerateSerializer]
[Alias("Account.Deposited")]
public sealed record Deposited(decimal Amount);
```

If you need a different encoding (e.g., a versioned namespace scheme, JSON envelope with a discriminator, or interop with a non-Orleans producer), implement `IEventSerializer<TLogEntry>` and register it via DI before the provider, or supply your own through `KurrentStorageOptions`.


## Accessing EventId and Event Metadata

If you need access to Kurrent's `EventId` or metadata fields in your grain, wrap your `TLogEvent` using `EventEnvelope<TLogEvent>` as your TLogEvent argument for JournaledGrain or IGrainEventProvider.

e.g. `IGrainEventProvider.SubscribeToGrainEvents<EventEnvelope<T>>` and `JournaledGrain<TLogView, EventEnvelope<TLogEvent>>`.

You can mix-and-match `EventEnvelope<T>` with `T` as needed within the same silo, as the serializer will handle both cases.

When you use `EventEnvelope<T>` a different `IEventConverter` is used which reads and writes the properites of the `EventEnvelope` into the appropriate Kurrent fields.

* EventId property, which you can set if you want control over the eventId written to KurrentDB, or read from the event when replaying.
* Metadata dictionary, which is persisted to KurrentDB and can be used to store additional information about the event that doesn't fit into the event payload. This can be useful for things like correlation ids, causation ids, or any other contextual information you want to associate with the event.

## Notes & limitations

- The storage provider's delete operations are mapped to Kurrent soft-deletes.
- `IGrainEventProvider` subscriptions only observe streams written by the log-consistency provider; state-storage snapshot writes are not exposed.
- Read-optimisation snapshots are not supported, an event sourced grain will load all events on activation. Business snapshots are supported using events marked with `DiscardPriorEventsAttribute`

## Versioning

Package versions are derived from git tags (`vMAJOR.MINOR.PATCH[-prerelease]`) using [MinVer](https://github.com/adamralph/minver). Tagged commits publish stable versions; non-tag commits produce pre-release packages.

## Experimental APIs

Some surface is annotated with `[Experimental("OEK…")]` and will produce a compiler diagnostic at every call site. The shape of these APIs may change in non-major releases. Acknowledge by suppressing the relevant diagnostic ID in your project (`<NoWarn>$(NoWarn);OEK0001;OEK0002</NoWarn>`) or with a localized `#pragma warning disable`.

| Diagnostic | API | Purpose |
| --- | --- | --- |
| `OEK0001` | `IGrainEventProvider` | An interface available via dependency-injection to for grains hosting projections or side-effects to read grain events |
| `OEK0002` | `DiscardPriorEventsAttribute`  | An attribute to tell the storage provider to delete events prior to this event |

## License

[License](https://github.com/OrleansContrib/Orleans.EventSourcing.Kurrent/blob/main/LICENSE.md)

