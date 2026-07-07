# Orleans.EventSourcing.Kurrent

[![NuGet](https://img.shields.io/nuget/v/Orleans.EventSourcing.Kurrent.svg)](https://www.nuget.org/packages/Orleans.EventSourcing.Kurrent)

[Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/) providers backed by [KurrentDB](https://kurrent.io/) (formerly EventStoreDB)

This project adds KurrentDB support to Microsoft Orleans.

| Provider | Orleans interface | What it persists | When to use |
| --- | --- | --- | --- |
| **Cluster membership provider** (`UseKurrentClustering`) | `IMembershipTable` `IGatewayListProvider` | Cluster membership information | Use this to maintain cluster membership in KurrentDB. | 
| **Log-consistency provider** (`AddKurrentBasedLogConsistencyProvider*`) | `ILogViewAdaptorFactory` for `JournaledGrain<TView, TEntry>` | The full **event log** for the grain — every `RaiseEvent` is appended to the grain's Kurrent stream. The view is rebuilt by replaying events. | Event-sourced grains (`JournaledGrain`). Gives you full history, projections via `IGrainEventProvider`, and replay. |
| **Grain state storage provider** (`AddKurrentBasedGrainStorageProvider*`) | `IGrainStorage` for `Grain<TState>` / `[PersistentState]` | The **latest snapshot** only — each `WriteStateAsync` appends one event and Kurrent's `MaxCount = 1` stream metadata trims older revisions. | Regular Orleans state persistence when you want it backed by Kurrent (e.g. to keep all storage in one system) |
| **Reminders provider** (`AddKurrentReminderService`) ⚠ experimental | `IReminderTable` | Grain Reminders | Use this provider to persist and manage Orleans reminders in KurrentDB. Installed from a separate package `Orleans.EventSourcing.Kurrent.Reminders`. |
| **Grain event subscription** (`IGrainEventProvider`) ⚠ experimental | n/a | Read-side subscription to the Kurrent `$all` stream, filtered to the streams produced by the log-consistency provider. | Projections / read models built from grain events emitted via the log-consistency provider. |

## Install

```sh
dotnet add package Orleans.EventSourcing.Kurrent
```

The reminders provider is shipped as a separate package:

```sh
dotnet add package Orleans.EventSourcing.Kurrent.Reminders
```

Targets `net8.0` and `net10.0`. Requires Microsoft Orleans 10.x and a KurrentDB / EventStoreDB server.

## Quick start

### Silo configuration

Pick whichever providers you need — they are configured separately and can use different options names if you want different `ClientSettings` per provider:

```csharp
using KurrentDB.Client;
using Orleans.EventSourcing.Kurrent.Hosting;
using Orleans.EventSourcing.Kurrent.Reminders;

var settings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");

var builder = Host.CreateApplicationBuilder(args);
builder.UseOrleans(silo =>
{
    // Clustering provider - supports the Orleans membership table implemented via events
    silo.UseKurrentClustering(o => o.ClientSettings = settings);

    // Log-consistency provider — required for JournaledGrain<TView, TEntry>.
    silo.AddKurrentBasedLogConsistencyProviderAsDefault(o => o.ClientSettings = settings);

    // Grain state storage provider — required for Grain<TState> / [PersistentState] backed by Kurrent.
    silo.AddKurrentBasedGrainStorageProviderAsDefault(o => o.ClientSettings = settings);

    // Reminders provider — if you want to persist Orleans reminders in Kurrent.
    silo.AddKurrentReminderService(o => o.ClientSettings = settings);
});
```

Configure the Orleans client to use the Kurrent Clustering provider:

```csharp
using KurrentDB.Client;
using Orleans.EventSourcing.Kurrent.Hosting;

var settings = KurrentDBClientSettings.Create("esdb://localhost:2113?tls=false");

var builder = Host.CreateApplicationBuilder(args);
builder.UseOrleansClient(client =>
{
    // Clustering provider - supports discovering silos by reading the cluster's event stream
    client.UseKurrentClustering(o => o.ClientSettings = settings);
});
```

The storage extensions also have non-default overloads that take a provider `name` so you can register multiple instances side-by-side.

To run KurrentDB locally (running **insecure** for testing only):
```sh
docker run --name kurrentdb-node -it -p 2113:2113 docker.kurrent.io/kurrent-latest/kurrentdb:latest --insecure
```

## Using the clustering provider

The clustering provider event-sources the orleans `IMembershipTable` and by writing membership events to the cluster's event stream.

The stream contains `MembershipUpdate` events written by silos as they join or when their status or suspect list changes. Silos also update their `IAmAlive` value every 30 seconds by default; these are recorded as `SiloAlive` events.

In addition to removing defunct silos from the membership table, the interval configured for [ClusteringMembershipOptions.DefunctSiloCleanupPeriod](https://learn.microsoft.com/en-us/dotnet/api/orleans.configuration.clustermembershipoptions.defunctsilocleanupperiod?view=orleans-10.0#orleans-configuration-clustermembershipoptions-defunctsilocleanupperiod)
may be used to write a snapshot of the membership table and truncate the cluster's event stream. This prevents the stream length growing indefinitely and ensures recovery and discovery complete in a reasonable amount of time.

### Configuring the clustering provider

The clustering provider can be configured using the delegate supplied to `UseKurrentClustering` 

Options available in `KurrentClusteringOptions` include:

| Name | Type | Default | Description |
|---|---|---|---|
| ClientSettings | KurrentDBClientSettings | null | The KurrentDB client settings used to connect to the KurrentDB server. |
| StreamPrefix | string | "Orleans.Cluster.Membership" | The prefix for the stream name used by the clustering provider. |
| EventCountBeforeSnapshot | int | 5000 | The number of events to write before taking a snapshot of the membership table. |

## Using the log-consistency provider (event-sourced grains)

Inherit from `JournaledGrain<TView, TEntry>`. Events are appended to a Kurrent stream per grain, and the view is rebuilt by replaying them.

> **Note:** Grains replay all events from the stream on activation. For long-running grains, use `[DiscardPriorEvents]` on summary or snapshot events to bound replay cost.

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

### Truncating the event stream - Experimental

The provider supports truncating the event stream by using the `DiscardPriorEvents` attribute on an event type (`OEK0002` — suppress with `#pragma warning disable OEK0002` or `<NoWarn>$(NoWarn);OEK0002</NoWarn>`).

When an event with this attribute is committed, all prior events in the stream will be discarded.

This can be used to write a tombstone event e.g. `AccountClosed` or a summary event e.g. `AccountBalance`:

```csharp
[DiscardPriorEvents]
[Alias("Account.Closed.V1")]
public record AccountClosed(DateTime ClosedAt) : AccountEvent;

public sealed class AccountGrain : JournaledGrain<AccountState, AccountEvent>, IAccountGrain
{
    public Task CloseAccount()
    {
        RaiseEvent(new AccountClosed(DateTime.UtcNow));
        return ConfirmEvents();
    }
}
```

### Deleting the event stream

The log-consistency provider [tombstones](https://docs.kurrent.io/clients/python/v1.2/delete-stream.html#tombstone-stream) the event stream using `JournaledGrain.ClearLogAsync`.

> **Note:** Once tombstoned a stream cannot be reused in Kurrent. To truncate the stream use the `DiscardPriorEvents` attribute instead.

The state-based provider [soft deletes](https://docs.kurrent.io/clients/python/v1.2/delete-stream.html#delete-stream) the stream using `Grain<TState>.ClearStateAsync`, which will delete the stream and allow it to be reused in Kurrent.

### Subscribing to grain events (projections) - Experimental

Resolve `IGrainEventProvider` from the silo container (`OEK0001` — suppress with `#pragma warning disable OEK0001` or `<NoWarn>$(NoWarn);OEK0001</NoWarn>`) and call one of the `SubscribeToGrainEvents*` overloads. This reads the Kurrent `$all` stream filtered to streams written by the log-consistency provider, deserializes each event, and yields it together with the originating `GrainId` and version:

```csharp
await foreach (var update in eventProvider.SubscribeToGrainEvents<IAccountGrain, AccountEvent>(
    subscriber: this.GetGrainId(),
    startingPosition: GlobalEventLogPosition.Start,
    cancellationToken: ct))
{
    switch (update)
    {
        case GrainEvent<AccountEvent> e:
            // project e.Event for e.EventGrainId at e.EventGrainVersion
            break;
        case Checkpoint cp:
            // persist cp.Position so the next subscription resumes from here
            break;
        case CaughtUp:
            // subscription is now live
            break;
        case FallenBehind:
            // subscription is lagging behind the live tail
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

## Reminders provider

The reminders provider is shipped as a **separate package**:

```sh
dotnet add package Orleans.EventSourcing.Kurrent.Reminders
```

It records Orleans reminder upserts and removals as discrete events in KurrentDB via a `JournaledGrain` implementing `IReminderTable`.

Register it on the silo:

```csharp
using Orleans.EventSourcing.Kurrent.Reminders;

builder.UseOrleans(silo =>
{
    silo.AddKurrentReminderService(o => o.ClientSettings = settings);
});
```

Reminders are persisted to a single stream. If you have high reminder churn, the stream will become large and it will take time to read the entire stream on startup.

## Configuration

`KurrentStorageOptions` is configured per provider name. The same options type is used by both storage providers; configuring one named instance does not affect the other.

| Property | Description |
| --- | --- |
| `ClientSettings` | `KurrentDBClientSettings` used to build the underlying `KurrentClient`. |
| `GrainStorageSerializer` | Orleans' `IGrainStorageSerializer` used by the default event serializer. |
| `StreamNameProvider` | `IKurrentStreamNameProvider` controlling stream-name layout. Defaults to `KurrentStreamName.Default`. |

### Custom event stream naming

The naming scheme is pluggable — see [Custom event stream naming](#custom-event-stream-naming).

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

When you use `EventEnvelope<T>` a different `IEventConverter` is used which reads and writes the properties of the `EventEnvelope` into the appropriate Kurrent fields.

* EventId property, which you can set if you want control over the eventId written to KurrentDB, or read from the event when replaying.
* Metadata dictionary, which is persisted to KurrentDB and can be used to store additional information about the event that doesn't fit into the event payload. This can be useful for things like correlation ids, causation ids, or any other contextual information you want to associate with the event.

## Notes & limitations

- Read-optimisation snapshots are not yet supported; an event-sourced grain will replay all events on activation. Business snapshots are supported using events marked with `[DiscardPriorEvents]`.

## Versioning

Package versions are derived from git tags (`vMAJOR.MINOR.PATCH[-prerelease]`) using [MinVer](https://github.com/adamralph/minver). Tagged commits publish stable versions; non-tag commits produce pre-release packages.

## Experimental APIs

Some surface is annotated with `[Experimental("OEK…")]` and will produce a compiler diagnostic at every call site. The shape of these APIs may change in non-major releases. Acknowledge by suppressing the relevant diagnostic ID in your project (`<NoWarn>$(NoWarn);OEK0001;OEK0002</NoWarn>`) or with a localized `#pragma warning disable`.

| Diagnostic | API | Purpose |
| --- | --- | --- |
| `OEK0001` | `IGrainEventProvider` | An interface available via dependency injection for grains hosting projections or side-effects to read grain events |
| `OEK0002` | `DiscardPriorEventsAttribute`  | An attribute to tell the storage provider to delete events prior to this event |

## License

[License](https://github.com/OrleansContrib/Orleans.EventSourcing.Kurrent/blob/main/LICENSE.md)

