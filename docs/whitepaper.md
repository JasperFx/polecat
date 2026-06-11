# Polecat: A SQL Server-Native Event Store and Document Database

*Building on a decade of Marten heritage — for the .NET teams that are already on SQL Server.*

<div align="center">
    <img src="./polecat-logo.png" alt="Polecat" width="25%">
</div>

## Executive summary

Polecat is an event store and lightweight document database for .NET, backed by SQL Server 2025. It gives SQL Server-on-.NET shops the same productive development model that [Marten](https://martendb.io) brings to PostgreSQL — same `IDocumentStore` API, same projection model, same async daemon, same Critter Stack ecosystem — without a database migration.

If you're a SQL Server shop considering event sourcing, CQRS, or a document-store productivity layer, Polecat lets you adopt those patterns inside your existing database, with infrastructure your operations team already runs.

It's open source, MIT-licensed, shipped 1.0 in March 2026, and integrates natively with [Wolverine](https://wolverinefx.io) for end-to-end CQRS.

---

## The state of the .NET data layer

Most production .NET applications run on SQL Server. That's not nostalgia — it's procurement, licensing, ops tooling, monitoring, backups, DR, DBA expertise, and ten years of internal applications already built around it. SQL Server is the path of least resistance for the majority of .NET teams, and it isn't going anywhere.

Meanwhile, the productivity story for event sourcing and document-style persistence in .NET has been overwhelmingly PostgreSQL-shaped. Marten — the open-source library that turned PostgreSQL into a credible event store and document database for .NET — has been mature for years and shipped its 10-year line. EventStoreDB / KurrentDB is its own product. Building event sourcing or a document store on SQL Server has historically meant either (a) writing the plumbing yourself, (b) bolting on Cosmos DB or MongoDB, or (c) adopting PostgreSQL purely for the data-layer features.

Each of those costs something real. Hand-rolled event sourcing skips the boring-but-important problems (projection rebuilds, async daemon coordination, optimistic concurrency, schema migrations, multi-tenancy). Adding a second database doubles ops complexity. Switching to PostgreSQL means a team retraining, ops re-tooling, and an extended migration window.

**SQL Server 2025 changes the calculus.** It introduces a native `JSON` column type with `JSON_VALUE` / `JSON_QUERY` / `JSON_MODIFY` and modern T-SQL features (window functions, MERGE, partition functions, sequences) — closing the storage-engine gap that made PostgreSQL uniquely suited to this style of work.

Polecat is the library that takes the Marten development model and points it at SQL Server 2025. If you'd adopt Marten on PostgreSQL, you can now adopt Polecat on SQL Server.

---

## What Polecat actually is

Polecat is one library that gives you two things under a single `IDocumentStore` API: a **lightweight document database** and a **full event store**.

### As a document database

Store and query .NET objects as JSON documents in SQL Server. Sessions are explicit (Lightweight or IdentityMap — no surprise dirty-tracking). LINQ is the query surface. Schema is managed for you.

```csharp
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;
    opts.UseNativeJsonType = true;     // SQL Server 2025 JSON column type
});

await using var session = store.LightweightSession();
session.Store(new Customer { Id = id, Name = "Acme", Region = "EMEA" });
await session.SaveChangesAsync();

await using var query = store.QuerySession();
var emea = await query.Query<Customer>()
    .Where(x => x.Region == "EMEA")
    .OrderBy(x => x.Name)
    .ToListAsync();
```

Polecat ships the productivity features you'd expect from a mature document layer: LINQ querying with paging and projections, optimistic concurrency (`IRevisioned` for numeric versions, `IVersioned` for GUID versions), soft deletes, document patching (`JSON_MODIFY` under the hood), bulk insert, HiLo and strongly-typed IDs, document metadata (created/modified/tenant), session listeners, multi-tenancy (conjoined, separate-database, or single-tenant), and an admin/diagnostics surface for migrations and dead-letter handling.

### As an event store

Append events to streams, project them into read models, and run the projections async without leaving the same store:

```csharp
session.Events.StartStream<Order>(orderId,
    new OrderPlaced(customerId, lineItems),
    new PaymentAuthorized(amount));
await session.SaveChangesAsync();

// Live aggregation (no read model required)
var order = await query.Events.AggregateStreamAsync<Order>(orderId);

// Or build a read model with a projection registered on the store
opts.Projections.Add<OrderSummaryProjection>(ProjectionLifecycle.Async);
```

Under the hood: a single-table append (QuickAppend — direct `INSERT` with `OUTPUT` for version capture, no stored procedures); inline, async, or live projection lifecycles; `SingleStreamProjection`, `MultiStreamProjection`, `EventProjection`, `FlatTableProjection`, and composite projections; inline snapshots for fast aggregate fetch; per-tenant event partitioning for very large multi-tenant stores; an async projection daemon that coordinates across nodes with high-water-mark tracking, extended progression diagnostics, and a resilience pipeline on every command; dead-letter persistence under `SkipApplyErrors`; subscriptions; and `FetchForWriting` for exclusive-lock command handlers.

### Built on SQL Server 2025

The storage layer uses SQL Server 2025's native `JSON` data type for document bodies, event data, headers, and snapshots; `bigint IDENTITY` (or per-tenant `CREATE SEQUENCE` under the per-tenant flag) for the global event sequence; `MERGE` for upserts and progression; `sp_getapplock` for advisory locks during exclusive writes and sequence reservation; `datetimeoffset` + `SYSDATETIMEOFFSET()` for timestamps; and partition functions for archive-aware and per-tenant physical partitioning. Schema migrations are managed declaratively by `Weasel.SqlServer`.

---

## The Marten heritage

The most important thing to know about Polecat is that it isn't a from-scratch reinvention. It's a SQL Server storage adapter for an architecture that has been deployed in production behind Marten for ten years.

Marten launched in 2016 and has shipped continuously since. The non-storage logic — projection lifecycles, the async daemon coordinator, the source generators that emit aggregate appliers at compile time, the LINQ provider's compositional design, the dead-letter mechanism, the resilience pipeline contract, the extended progression-tracking surface, projection rebuilds, multi-tenancy, the strongly-typed-ID conventions, the `IDocumentStore` / `IDocumentSession` / `IQuerySession` shape — lives in shared upstream libraries:

- **`JasperFx`** — core utilities, dependency-injection wiring (`IConfigureStore<T>`), strongly-typed-ID detection, source-generator scaffolding.
- **`JasperFx.Events`** — the storage-agnostic event-sourcing primitives. Projection types, the async daemon base (`JasperFxAsyncDaemon`), the projection coordinator, `ShardName` / `ShardState` / `HighWaterStatistics`, `IEventStoreInstrumentation`, `IMessageOutbox`, the resilience contract.
- **`Weasel.SqlServer`** — declarative schema management, `DatabaseResource` integration, migration logic, partition support.

Marten is one consumer of that stack. Polecat is another. The two stores intentionally share the same projection model, the same source generators, the same daemon coordinator, the same DI conventions, the same Critter Stack tooling integrations (CritterWatch monitoring, JasperFx aspirations). The delta is **purely the SQL storage layer** — the SQL dialect, the table layouts (`pc_streams` / `pc_events` / `pc_event_progression` instead of `mt_*`), the JSON storage choice (SQL Server 2025 native `JSON` instead of Postgres `jsonb`), the upsert syntax (`MERGE` instead of `ON CONFLICT`), and the locking primitives (`sp_getapplock` instead of `pg_advisory_lock`).

What this means in practice: when a useful pattern lands in Marten — extended progression tracking with heartbeat/agent_status/pause_reason columns, per-tenant physical partitioning of the events table, the dedupe + lift wave that consolidated shared types into JasperFx, the `IEventStoreInstrumentation` DI mechanic for CritterWatch — Polecat picks it up too, because most of the work happened upstream of either store. The API surface is intentionally identical so Marten experience translates directly: a developer who knows `session.Events.StartStream` and `SingleStreamProjection<T>` in Marten knows them in Polecat.

You aren't betting on a new library. You're betting on the SQL Server adapter for an architecture that's been pressure-tested for a decade.

---

## CQRS with Polecat and Wolverine

The canonical Critter Stack pattern is Wolverine on the front edge, Polecat on the storage edge, with the two wired together so command handlers stay small and explicit:

- **Wolverine** — the message bus and HTTP/handler runtime from the same author. Handlers are plain methods. Code generation produces the wiring.
- **Polecat** — the unit-of-work for state changes. Documents, events, and outgoing Wolverine messages all commit in one SQL Server transaction.

`WolverineFx.Polecat` (the NuGet package — the namespace and library identity stays `Wolverine.Polecat`, historical naming quirk; structurally identical to `WolverineFx.Marten`) gives you:

- **A transactional outbox** (`IPolecatOutbox`) — Wolverine messages emitted during a handler are written to the Polecat outbox in the same transaction as your document/event changes. Either everything commits, or nothing does. There's no "wrote to the database but the message wasn't published" failure mode.
- **Aggregate handler codegen** — annotate a handler with `[WriteAggregate]` or `[ReadAggregate]` and Wolverine.Polecat generates the `FetchForWriting`/`SaveChangesAsync` plumbing. The handler stays a pure command-to-events function.
- **Event subscription** — `SubscribeToEvents` publishes selected event types from the Polecat event store as Wolverine messages, so async projection-driven workflows are first-class.
- **Concurrency model selection** — `[ConsistentAggregate]` picks optimistic vs revision-based concurrency without leaking SQL into the handler.

A typical command handler:

```csharp
public class PlaceOrderHandler
{
    [AggregateHandler]
    public static OrderPlaced Handle(PlaceOrder cmd, [WriteAggregate] Order order)
    {
        if (order.Status != OrderStatus.Draft)
            throw new InvalidOperationException("Order is not in draft state.");

        return new OrderPlaced(cmd.CustomerId, cmd.LineItems);
    }
}
```

There is no `IDocumentSession` parameter, no `session.SaveChangesAsync()`, no `Events.Append(...)`. Wolverine.Polecat's code generation handles the fetch-aggregate / apply-events / save-changes / publish-outgoing-messages dance. The handler is a function from command to event(s).

Wired up at composition time, after `dotnet add package WolverineFx.Polecat`:

```csharp
builder.Services.AddPolecat(opts =>
{
    opts.ConnectionString = connectionString;
    opts.Projections.LiveStreamAggregation<Order>();
})
.IntegrateWithWolverine();   // transactional outbox + handler codegen
```

The combined pattern — Wolverine for the entry-point and orchestration concerns, Polecat for state — is well-suited to **modular monolith** architectures: complex business workflows expressed as event-driven command handlers, all running in one process against one SQL Server, without the operational cost of premature microservices.

---

## SQL Server 2025 — the moment

The features that made PostgreSQL the obvious choice for this style of library — `jsonb`, expressive indexing, partition functions, mature window functions, declarative-ish schema management — were the gap that made hand-rolling event sourcing on SQL Server painful. SQL Server 2025 closes the gap:

- A **native `JSON` data type** with first-class `JSON_VALUE` / `JSON_QUERY` / `JSON_MODIFY` operators, suitable for document storage and event-data persistence.
- Mature **`MERGE`**, **`OUTPUT`**, and **window function** support that the QuickAppend path and progression upserts depend on.
- **Partition functions** and **`CREATE SEQUENCE`** — used by Polecat's archive-aware partitioning and per-tenant event sequencing.
- **`sp_getapplock`** / **`sp_releaseapplock`** for advisory locks, used during exclusive writes and tenant-sequence reservation.

Polecat targets SQL Server 2025 specifically because the JSON column type is non-negotiable for the projection performance and storage characteristics this kind of library needs. Older SQL Server versions can run the test matrix in `(edge)` mode (no native JSON) for compatibility checks, but production targets are 2025 forward.

---

## Honest limits

A whitepaper that doesn't say "here's where we're younger than the alternative" isn't worth reading. So:

- **SQL Server 2025 and .NET 10 required.** Older SQL Server versions are out of scope for production deployments — the native JSON type is core to how Polecat stores documents, event bodies, and snapshots.
- **The community is younger than Marten's.** Marten has ten years of conference talks, blog posts, and field reports. Polecat is months into 1.0. The codebase is mature because the patterns are mature, but the community knowledge base is still catching up.
- **Polecat is System.Text.Json only.** Marten supports both Newtonsoft.Json and STJ for historical reasons; Polecat skipped that fork.
- **Polecat doesn't ship dynamic ancillary-store proxies.** `AddPolecatStore<T>()` works for `T == IDocumentStore`; arbitrary marker interfaces are not yet supported the way Marten emits them.
- **JasperFx Software offers paid support** for Marten today. Polecat is covered as part of the same ecosystem — the same team builds and supports both.

Polecat is production-ready (1.0 shipped March 2026), but you should pick it because the SQL Server alignment matters to you, not because every Marten feature has a like-for-like Polecat copy on day one.

---

## Get started

```bash
dotnet add package Polecat
```

Then:

```csharp
builder.Services.AddPolecat(connectionString);
```

That's the minimum viable setup. From there the [Getting Started guide](/getting-started) walks you through documents, events, projections, and the async daemon in about an hour.

- **Docs:** [polecat.jasperfx.net](https://polecat.jasperfx.net/)
- **GitHub:** [JasperFx/polecat](https://github.com/JasperFx/polecat)
- **Discord:** [Critter Stack chat](https://discord.gg/WMxrvegf8H)
- **Wolverine integration:** [wolverinefx.io](https://wolverinefx.io)
- **Support:** [JasperFx Software](https://jasperfx.net/support-plans/)

If you're already running SQL Server in production and you've been watching the Critter Stack from across the PostgreSQL fence — this is your invitation.
