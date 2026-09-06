# Event Upcasting <Badge type="tip" text="5.23" />

Event schemas change; the events already in `pc_events` do not. **Upcasting** transforms a stored
event into the current CLR type **on read**, so aggregates, projections and subscriptions only ever
see the new shape — with no migration of existing event data, and no `if (old) … else …` branches
scattered through your handlers.

## The contract

The seam is [`JasperFx.Events.Upcasting`](https://github.com/JasperFx/jasperfx), shared across the
Critter Stack, so an upcaster you write compiles once against Marten, Polecat or Fisher.

Registration is on `StoreOptions.Events`, and it comes in three shapes.

### Typed: the old CLR type is still in the codebase

```csharp
// The event as it was originally written, and as rows in pc_events still record it.
public record RoomBooked(Guid RoomId, string Guest);

// The event this deployment wants to see.
public record RoomReserved(Guid RoomId, string Guest, string Source);

opts.Events.Upcast<RoomBooked, RoomReserved>(
    old => new RoomReserved(old.RoomId, old.Guest, "legacy"));
```

The stored event type name defaults to `RoomBooked`'s conventional alias, which is what makes rows
that were written as `RoomBooked` come back as `RoomReserved`.

### Raw JSON: the old CLR type is gone

This is the shape that lets you **delete the old event class**. Polecat is System.Text.Json only and
stores event bodies in a SQL Server 2025 native `json` column, so the raw form is always available:

```csharp
opts.Events.Upcast<RoomReserved>("room_booked", document =>
{
    var root = document.RootElement;
    return new RoomReserved(
        root.GetProperty("roomId").GetGuid(),
        root.GetProperty("guest").GetString()!,
        "legacy");
});
```

::: warning Property casing is your serializer's, not the contract's
`JsonElement.GetProperty` is case-sensitive. Polecat's default `PropertyNamingPolicy` is
`JsonNamingPolicy.CamelCase`, so the stored names are `roomId` / `guest` — **not** `RoomId` /
`Guest`. A raw-JSON upcaster reads exactly what your serializer wrote; the typed shapes get
case-insensitivity for free from `PropertyNameCaseInsensitive`.
:::

### Class-based

Derive from the shared bases when the transformation deserves a name and a test of its own:

```csharp
public class RoomBookedUpcaster : EventUpcaster<RoomBooked, RoomReserved>
{
    protected override RoomReserved Upcast(RoomBooked old)
        => new(old.RoomId, old.Guest, "legacy");
}

opts.Events.Upcast<RoomBookedUpcaster>();
```

`JasperFx.Events.Upcasting.SystemTextJson` carries the raw-JSON equivalents. The namespaces
deliberately mirror Marten's, so migrating an existing Marten upcaster is a using-directive change.

## Async upcasters

Every registration shape has an async-only twin, for a transformation that genuinely has to await:

```csharp
opts.Events.Upcast<RoomBooked, RoomReserved>(
    async (old, token) => new RoomReserved(old.RoomId, await LookupGuestAsync(old.Guest, token), "legacy"));
```

::: tip Prefer the synchronous form
An async transformation runs **once per stored event**, so it invites N+1 behaviour on a long
stream. Polecat's read paths are all asynchronous, so an async-only registration works everywhere
except the batched DCB tag query, which materializes synchronously and throws `UpcastingException`.
:::

## What upcasting does and does not touch

| | |
|---|---|
| **Applies to** | `FetchStreamAsync`, live aggregation, `FetchForWriting` / `FetchLatest`, the async daemon and every subscription, `QueryAllRawEvents()`, batched stream fetches, DCB tag queries, projection replay |
| **Does not apply to** | `QueryRawEventDataOnly<T>()` — it never builds an `IEvent`, and by naming `T` the caller has already made the decision an upcaster would make |
| **Never touches** | the **write** path. Upcasting is a read-time transformation; nothing rewrites `pc_events`. |

An upcast event keeps reporting the **stored** name in `IEvent.EventTypeName` while
`IEvent.EventType` and `IEvent.Data` are the new type. That is deliberate: the alias is a fact about
the database, and rewriting it on read would make an upcast indistinguishable from a native append
of the new type — exactly the distinction an operator needs mid-migration.

## Two rules worth knowing

**A registered transformation wins over the stored `dotnet_type`.** Polecat records an
assembly-qualified CLR type hint on every event row and normally resolves the type from it. A
registered transformation is the *authoritative* reading of its source event type name, so the hint
does not get a vote — otherwise appending the **old** CLR type into the store that carries the
upcaster would read straight back as the old type, and the upcaster would silently do nothing for
exactly the rows most likely to exist during a migration. (Marten pins the same rule; see
marten#4680.)

**Registration is last-wins per stored event type name.** Registering the same source name twice
replaces the earlier transformation rather than chaining onto it.

## Binary events

Upcasting does not apply to rows written by an
[`IEventBinarySerializer`](/events/binary-serialization). The shared payload contract is a JSON one —
both accessors presuppose a JSON body — so a binary row has nothing a transformation can read and
keeps its ordinary binary read path.
