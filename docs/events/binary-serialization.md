# Binary Event Serialization <Badge type="tip" text="5.17" />

Polecat can store the body of an individual event type in a compact binary format —
[MessagePack](https://msgpack.org/), [MemoryPack](https://github.com/Cysharp/MemoryPack),
compressed JSON, anything you can express as `byte[]` — instead of the default JSON.

The opt-in is **per event type**. Binary-serialized and JSON-serialized events coexist in the same
`pc_events` table, so switching one hot event type to binary is an in-place change with **no
migration of existing event data** — and switching it back off is equally safe.

::: tip Why bother
This is a pure storage and IO win with no application-visible behaviour change. The motivating
measurement, from CritterWatch's telemetry ingest, was a representative fleet going from
**904 KB/min to 19 KB/min** of event storage by moving a single high-volume event type off JSON.
That is the difference between retaining a month and retaining a day at the same disk budget.
:::

## The contract

The seam is [`JasperFx.Events.IEventBinarySerializer`](https://github.com/JasperFx/jasperfx), shared
by every Critter Stack store:

```cs
public interface IEventBinarySerializer
{
    byte[] Serialize(Type type, object data);
    object Deserialize(Type type, byte[] data);
}
```

Two methods, deliberately. Everything else about the feature — which column carries the bytes, how a
row records that it is binary, how the serializer for a type is resolved — is a storage concern that
differs per store and stays there.

::: warning It is shared on purpose
The interface and the `[BinaryEvent]` attribute both live in `JasperFx.Events`, not in Polecat. An
application that compiles one body of source against Marten *and* Polecat *and* Fisher writes **one**
serializer and registers it three times, rather than carrying three identical two-method classes that
differ only in which namespace the interface came from. See
[polecat#475](https://github.com/JasperFx/polecat/issues/475).

Polecat 5.17 **removed** the earlier `Polecat.Events.IEventBinarySerializer` and
`Polecat.Events.BinaryEventAttribute` (added in 5.12 by
[#388](https://github.com/JasperFx/polecat/issues/388)) rather than keeping them as aliases: any file
with both `JasperFx.Events` and `Polecat.Events` in scope — which is exactly the code this feature
exists for — got `CS0104` on the bare name. Upgrading means re-pointing the `using` at
`JasperFx.Events`. It is a compile error, never a silent behaviour change, and no stored data is
affected.
:::

## How it works on SQL Server

`pc_events` carries a nullable binary column alongside the JSON one:

| Column | Type | |
| :--- | :--- | :--- |
| `data` | `json` (SQL Server 2025) / `nvarchar(max)` | the JSON payload, `NOT NULL` |
| `bdata` | `varbinary(max)` | the binary payload, `NULL` |

The discriminator is **per row**: `bdata IS NULL`.

| When | `data` | `bdata` |
| :--- | :--- | :--- |
| The event's type stays on the JSON path | the full JSON payload | `NULL` |
| The event's type has a binary serializer | the placeholder `{}` | the serialized bytes |

`data` holds `{}` rather than `NULL` for a binary event because the column is `NOT NULL` and, on SQL
Server 2025, typed `json` — the row still has to hold something the engine will parse. No schema
relaxation was needed.

On read, Polecat inspects `bdata`:

- `NULL` → the ordinary JSON deserialization path. Rows written before the feature existed keep
  working, forever, with no conversion.
- non-null → `IEventBinarySerializer.Deserialize(eventType, bytes)`.

Because the discriminator is on the row and the serializer is resolved per event type, one stream can
carry rows of either format with no special handling at the call site. This holds across every read
path: `FetchStreamAsync`, live aggregation, inline projections, the async daemon, the event LINQ
provider, and event data masking.

## Opting an event type in

Two equivalent routes.

**By attribute**, resolved against the store-wide default serializer:

```cs
using JasperFx.Events;

[BinaryEvent]
public record TelemetrySampled(string Channel, int[] Samples);
```

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection(connectionString);

    // Every [BinaryEvent]-marked type with no explicit registration uses this.
    opts.Events.DefaultBinarySerializer = new MessagePackEventSerializer();
});
```

**By explicit registration**, which wins over the attribute and needs no attribute at all — useful
when the event type lives in an assembly you do not own:

```cs
opts.Events.UseBinarySerializer<TelemetrySampled>(new MessagePackEventSerializer());
```

Resolution order for an event type:

1. An explicit `opts.Events.UseBinarySerializer<TEvent>(...)` registration.
2. `[BinaryEvent]` on the type, plus `opts.Events.DefaultBinarySerializer`.
3. Otherwise plain JSON — the existing path, and the default for everything.

A type marked `[BinaryEvent]` with **neither** configured is a configuration error: Polecat throws
when it first resolves that event type, naming both registration entry points. It deliberately does
not fall back to JSON — a silent fallback would give you a store whose write amplification quietly
does not match the configuration you believe you are running.

## Bring your own serializer

The interface is small enough to implement directly against any binary format. The serializer is a
singleton — one instance serves every session in the store — so **it must be thread-safe**:

```cs
using JasperFx.Events;

public sealed class MessagePackEventSerializer : IEventBinarySerializer
{
    private static readonly MessagePackSerializerOptions _options =
        MessagePackSerializerOptions.Standard
            .WithCompression(MessagePackCompression.Lz4BlockArray)
            .WithResolver(ContractlessStandardResolver.Instance);

    public byte[] Serialize(Type type, object data)
        => MessagePackSerializer.Serialize(type, data, _options);

    public object Deserialize(Type type, byte[] data)
        => MessagePackSerializer.Deserialize(type, data, _options);
}
```

::: danger A serializer is part of your read path forever
A serializer stays load-bearing for as long as any row it wrote still exists. Removing a registration
does not restore those rows to the JSON path — it makes them **unreadable**, and the read fails with
an exception naming the type and both registration entry points. Keep the registration after the
event type stops being written as binary; retire it only once the rows are archived or compacted away.
:::

## Schema migration

Purely additive. The only schema change is the nullable `bdata varbinary(max)` column on
`pc_events`, and Polecat's ordinary schema migration adds it on the next apply — Weasel produces an
additive delta, so `ApplyAllConfiguredChangesToDatabaseAsync()` or the `polecat` CLI's
`db-apply` handles an existing store with no downtime and no data conversion.

The column is created **unconditionally**, whether or not a binary serializer is configured. That
keeps the read projection one fixed shape rather than two, and it means turning the feature on later
is a code change only — the column is already there.

Existing rows have `bdata = NULL` and keep reading through the JSON path. There is nothing to
backfill.

## On-disk shape

```sql
-- a binary-serialized event
select [type], cast([data] as nvarchar(max)) as data, iif([bdata] is null, 1, 0) as is_json
from [polecat].[pc_events] where seq_id = 42;
--  type                | data | is_json
-- ---------------------|------|--------
--  telemetry_sampled   | {}   | 0

-- a JSON-serialized event on the same stream
select [type], cast([data] as nvarchar(max)) as data, iif([bdata] is null, 1, 0) as is_json
from [polecat].[pc_events] where seq_id = 43;
--  type                | data                          | is_json
-- ---------------------|-------------------------------|--------
--  trip_comment_added  | {"comment":"looking good",…}  | 1
```

## Schema evolution — use versioned event types

Polecat's event upcasters operate on the JSON wire form and do not generalize to a `byte[]` payload,
so they do not apply to binary events. The recommended pattern for evolving a binary event's shape is
to **introduce a new event type per version** rather than transforming in place:

```cs
// Original
[BinaryEvent]
public record TripStarted(Guid TripId, string DriverName);

// Schema change. Don't edit TripStarted — add a new type.
[BinaryEvent]
public record TripStartedV2(Guid TripId, string DriverName, DateTimeOffset StartedAt);
```

Have the aggregate handle both versions, and the coexistence design carries old rows and new rows on
the same stream with no migration:

```cs
public class Trip
{
    public Guid Id { get; set; }
    public string DriverName { get; set; } = "";
    public DateTimeOffset? StartedAt { get; set; }

    public void Apply(TripStarted e) { Id = e.TripId; DriverName = e.DriverName; }

    public void Apply(TripStartedV2 e)
    {
        Id = e.TripId;
        DriverName = e.DriverName;
        StartedAt = e.StartedAt;
    }
}
```

You *can* instead lean on your serializer's own version tolerance — MessagePack and MemoryPack both
have backward-compatible field evolution for additive-only changes. That works right up to the edge of
the serializer's tolerance rules; renames, type changes and field splits have no JSON-style upcaster
to fall back on. Versioned event types work for every shape of change and stay explicit about which
version each row was written with.

The same pattern covers going binary for an event type that is already JSON: define a new
`[BinaryEvent]`-marked type for the new version, leave the old type and its upcasters alone, and let
the per-row dispatch cope with both formats on one stream.

## What this is not

- **Not** a store-wide or per-stream setting. There is no flag on the store, the stream or the table —
  only per-event-type registration and the per-row `bdata` discriminator. That is deliberate; it is
  what makes adoption and rollback both free.
- **Not** a change to querying event *metadata*. Sequence, version, stream, type name, timestamp,
  correlation, causation and headers are all still ordinary columns and still queryable.
- ⚠️ **Not** queryable *content*. Anything that reaches into the event body through LINQ resolves
  against the `data` column, which holds `{}` for a binary event — so a `Where` on a body member
  never matches a binary row, and `QueryRawEventDataOnly<T>()` (which deserializes straight from
  `data`) hands back an empty instance. Both fail *silently*: no exception, just no results. Do not
  make an event type binary if you query its body.

  `QueryAllRawEvents()` is the exception and is safe — it materializes `IEvent` wrappers through the
  same per-row dispatch as `FetchStreamAsync`, so binary payloads hydrate correctly. Only the
  *filtering* is blind to them.

## Cross-store parity

The behaviour on this page is held to a shared definition by
`BinaryEventSerializationCompliance` in `JasperFx.Events.ComplianceTests`, which Marten and Polecat
both run against their own storage. The fact that pins the headline promise is
`json_and_binary_events_coexist_in_one_stream` — a store that got everything else right but kept a
per-store or per-stream format flag fails exactly there.

## See also

- [Event Storage](/events/storage) — the `pc_events` table in full
- [Appending Events](/events/appending)
