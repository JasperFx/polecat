# Event Multi-Tenancy

Polecat supports multi-tenancy in the event store through conjoined tenancy (shared tables) or separate database tenancy.

## Conjoined Tenancy

Enable tenant isolation within shared event tables:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    opts.Events.TenancyStyle = TenancyStyle.Conjoined;
});
```

With conjoined tenancy:

- All events include a `tenant_id` column
- Stream queries automatically filter by tenant
- Event appending records the session's tenant ID
- The async daemon processes events per-tenant

### Using Tenanted Events

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    TenantId = "tenant-abc"
});

// Events are stored with tenant_id = "tenant-abc"
session.Events.StartStream<Order>(
    new OrderCreated(100m, "Widget")
);
await session.SaveChangesAsync();

// Only loads events for "tenant-abc"
var order = await session.Events.AggregateStreamAsync<Order>(streamId);
```

### Several Tenants in One Save

`ForTenant` gives you a view of another tenant in the same unit of work, so its writes commit with the
session's in one transaction:

```cs
await using var session = store.LightweightSession();

// Two streams, because a stream id is only unique within a tenant
session.ForTenant("red").Events.Append(streamId, new OrderShipped());
session.ForTenant("blue").Events.Append(streamId, new OrderShipped());

await session.SaveChangesAsync();
```

Everything on `ForTenant(...).Events` runs as that tenant, reads included: `FetchStreamAsync`,
`AggregateStreamAsync`, `FetchLatest`, `FetchForWriting`, `FetchForWritingByTags`, `QueryAllRawEvents` and
`CompactStreamAsync`. The session's aggregate identity map belongs to the session's own tenant, so a view
of another tenant doesn't use it.

Without conjoined tenancy the id identifies the stream on its own, so two tenants naming the same id in
one save are appending to one stream.

## Separate Database Tenancy

Each tenant gets its own database with independent event stores:

```cs
var store = DocumentStore.For(opts =>
{
    opts.MultiTenantedDatabases(databases =>
    {
        databases.AddTenant("tenant-a", "Server=localhost;Database=events_tenant_a;...");
        databases.AddTenant("tenant-b", "Server=localhost;Database=events_tenant_b;...");
    });
});
```

With separate database tenancy:

- Each tenant has completely isolated event data
- The async daemon runs one daemon per tenant database, each with its own high-water mark and
  projection progress
- Schema management is independent per database; `ApplyAllDatabaseChangesOnStartup()` migrates
  every tenant database

::: warning No default tenant — and no top level connection string
`MultiTenantedDatabases()` (and `MultiTenantedMasterTable()`) sets
`StoreOptions.DefaultTenantUsageEnabled` to `false`. Do **not** register a placeholder
`*DEFAULT*` tenant, and you do **not** need to set `StoreOptions.ConnectionString` — the tenancy
supplies one. Opening a session or building a daemon without a tenant throws
`DefaultTenantUsageDisabledException` rather than quietly using whichever database happened to be
first.

The async daemon starts one daemon per tenant database with the default tenant disabled, and
`ApplyAllDatabaseChangesOnStartup()` migrates every tenant database. See
[No default tenant is required](/configuration/multitenancy#no-default-tenant-is-required).
:::

## Default Tenant

When no tenant ID is specified, events are stored with `tenant_id = 'DEFAULT'`. In single-tenant
mode, all queries use this default value.

Under **separate database** tenancy the default tenant is disabled, so there is no `'DEFAULT'`
fallback — every session must name its tenant. See the warning above.

## Per-Tenant Event Partitioning <Badge type="tip" text="4.2" />

::: tip
This is an advanced, opt-in option for large multi-tenanted event stores where a single shared
event sequence becomes a scalability bottleneck. It builds on conjoined event tenancy by giving each
tenant its own event numbering, so a tenant-scoped projection rebuild doesn't have to pay for a
database-wide event-sequence scan. Tracked in
[polecat#163](https://github.com/JasperFx/polecat/issues/163).
:::

By default every tenant shares the global `seq_id BIGINT IDENTITY` on `pc_events`. With
**per-tenant event partitioning**, each tenant instead gets its own event sequence:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // Per-tenant partitioning builds on conjoined event tenancy
    opts.Events.TenancyStyle = TenancyStyle.Conjoined;

    // Opt into per-tenant event numbering
    opts.EventGraph.UseTenantPartitionedEvents = true;
});
```

When enabled, Polecat:

* **Maintains a tenant registry** — `pc_tenant_partitions` maps each `tenant_id` to a compact integer
  `ordinal`, populated the first time a tenant appends events (via Weasel.SqlServer's managed tenant
  partitioning).
* **Gives each tenant its own sequence** — `seq_id` is drawn from a per-tenant
  `pc_events_sequence_{ordinal}` object (created on demand) via `NEXT VALUE FOR`, rather than a single
  global `IDENTITY`. `seq_id` is therefore unique only *within* a tenant, so the `pc_events` primary
  key becomes composite `(tenant_ordinal, seq_id)`.
* **Physically partitions `pc_events` and `pc_streams` by tenant** — both tables are `RANGE RIGHT`
  partitioned on the tenant `ordinal`, and a new partition is split in as each tenant registers. A
  tenant's events and stream rows live in their own physical partitions, so per-tenant scans and
  rebuilds touch only those partitions (Marten parity: `mt_streams` rides `mt_events`' tenant
  partitioning).

```cs
// Each tenant's seq_id starts at 1 and advances independently
await using var red = store.LightweightSession(new SessionOptions { TenantId = "Red" });
red.Events.StartStream(redStream, new QuestStarted("Red"));   // Red seq_id 1
await red.SaveChangesAsync();

await using var blue = store.LightweightSession(new SessionOptions { TenantId = "Blue" });
blue.Events.StartStream(blueStream, new QuestStarted("Blue")); // Blue seq_id 1 — independent
await blue.SaveChangesAsync();
```

A tenant's first write provisions it before the save opens its transaction, because splitting a
partition needs a schema-modification lock on `pc_streams` that it could not get behind a lock the same
session holds. `FetchForExclusiveWriting` provisions before it takes its lock, for the same reason, and
so do `FetchForWriting` and `FetchManyForWriting` in a Serializable, Repeatable Read or Snapshot session.

Two things can't be reordered that way, because the lock is taken before Polecat knows the tenant will
be written to: an exclusive fetch or `AppendExclusive` for one tenant followed by a write through
`ForTenant` to a new one, and a plain event read in a Serializable or Repeatable Read session followed by
a tenant's first append. Either one waits for the command timeout and leaves the tenant's partitions
unsplit, which is the failure described below. A plain read in a Snapshot session followed by a tenant's
first append fails instead, with SQL Server's error 3961. Onboard those tenants first with
`store.Advanced.AddPolecatManagedTenantsAsync`.

Provisioning takes the same schema lock as migrations and the async daemon, so a split doesn't collide with
them, and new tenants are provisioned one at a time across all your processes. A write waits for that lock
for up to `StoreOptions.StartupMigrationLockTimeout`, 30 seconds by default, and a session with a transaction
open doesn't wait at all. Either way, a write that can't get the lock goes ahead without it, because it can't
happen without the tenant's ordinal, and a busy schema doesn't fail a write. A retried split is the
exception: it's skipped if the lock is taken, and comes round again later.

Each process keeps its own copy of the `pc_tenant_partitions` map. When two processes pick the same ordinal
for different new tenants, the second one reloads the map and takes the next ordinal, so both writes succeed.

#### When a split fails

Splitting a partition needs a schema-modification lock, so another session's long transaction on
`pc_streams` or `pc_events` can make a new tenant's split time out. That does not fail the save. The
write goes ahead, and until the split succeeds the tenant's rows on that table sit in a neighbouring
tenant's partition:

- **Reads are correct.** Every read filters on the tenant, so only the physical placement is wrong.
- **A warning is logged** naming the tenant and the tables, through the host's logging when the store was
  registered with `AddPolecat`. A store built with `DocumentStore.For` has no logger to write to.
- **A later write retries the split**, and SQL Server moves the misplaced rows when it succeeds. So does
  the next new tenant's split, which fills in every boundary that is missing.

A retry never runs behind a lock the session holds on the table, so a session with a transaction already
open leaves it to a later one. At most one retry for a tenant starts every 30 seconds in each process,
and it gives up after three seconds rather than the command timeout. The limits are there because
while a split waits for its lock, every tenant's reads and writes of that table queue behind it.

Moving rows counts against those three seconds, and was measured at about 60,000 small rows a second on
a development container, so a tenant that wrote a great many rows before its split could run may never
get through on a retry. It keeps logging the warning. `store.Advanced.AddPolecatManagedTenantsAsync` repeats the split on demand under the
connection's own command timeout, and reports the outcome for each table. Moving rows holds that lock
for as long as it takes, so run it at a quiet moment and raise `Command Timeout` in the connection string
if it needs longer.

### Tenant-aware async daemon

The asynchronous projection daemon is per-tenant aware under this flag. Rather than one database-wide
high-water scan, it polls a **per-tenant high-water vector** in a single round-trip (joining
`pc_tenant_partitions` → each tenant's `pc_events_sequence` value → `pc_event_progression`), and tracks
each tenant's projection progress independently. The headline benefit is **bounded, isolated
per-tenant rebuilds**:

```cs
using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();

// Rebuild a projection for ONE tenant — replays only that tenant's bounded sequence range and
// resets only that tenant's (projection, tenant) progression. Other tenants keep running untouched.
await daemon.RebuildProjectionAsync("QuestParty", "Red", CancellationToken.None);

// Or fan out an isolated rebuild across every registered tenant:
await CrossTenantRebuild.RebuildEverywhereAsync(
    daemon, "QuestParty", timeout: 1.Minutes(), CancellationToken.None);
```

A tenant-scoped rebuild never pays for a database-wide event scan and never disturbs other tenants —
exactly the win this design exists for in very large
multi-tenanted stores.

::: warning
`UseTenantPartitionedEvents` defaults to `false`; existing stores keep the global `IDENTITY` append
path byte-for-byte. The flag **requires** `TenancyStyle.Conjoined` (there is nothing to partition by
otherwise) and is currently incompatible with `UseArchivedStreamPartitioning` — a SQL Server table
supports only one partition scheme; both raise an error at store construction.

Physical partitioning applies to `pc_events` and (since #335) `pc_streams`. The partition
function/scheme are database-global objects, so a single database should host one tenant-partitioned
event store.

Document tables can join the same managed per-tenant partitioning — including runtime tenant
onboarding/removal via `store.Advanced.AddPolecatManagedTenantsAsync` /
`RemovePolecatManagedTenantsAsync` — see
[Document multi-tenancy partitioning](/documents/partitioning#managed-per-tenant-partitioning-335).
:::

## Erasing one tenant's data

`DeleteAllTenantDataAsync` deletes every row belonging to one tenant and leaves the others intact:
documents of every conjoined type, `pc_events` and `pc_streams` (with the inline snapshot each stream
row carries), DCB tag tables, natural-key lookups, full-text token tables, flat-table projections, and
that tenant's own `pc_event_progression` rows.

```cs
var deleted = await store.Advanced.DeleteAllTenantDataAsync("acme", cancellationToken);

// Rows removed per table, for the tables that had any
foreach (var (table, rows) in deleted)
{
    logger.LogInformation("Deleted {Rows} rows from {Table}", rows, table);
}
```

It works on **any** conjoined store, partitioned or not, which is the difference from
`RemovePolecatManagedTenantsAsync(..., TenantDropBehavior.DeleteData)` — that one requires managed
partitioning and drops a partition rather than deleting rows. Under `UseTenantPartitionedEvents` it also
takes the tenant's DCB tag rows, `pc_dcb_tag_version` rows and natural-key lookups. Whichever
`TenantDropBehavior` you pass, a removed tenant loses its `pc_event_progression` rows, because its event
sequence restarts at 1 if it comes back. A tenant still sharing an ordinal with another keeps all of this,
as it keeps its events.

Three things worth knowing before you call it:

- **It deletes data, not registration.** The tenant's partition, ordinal and per-tenant event sequence
  are schema objects and are left alone. Use `RemovePolecatManagedTenantsAsync` when the tenant is
  going away for good; use this when its data should go and its registration should stay.
- **It runs in one transaction**, so a failure leaves the tenant wholly present rather than half
  erased.
- **On a store with no multi-tenancy it deletes nothing** and returns an empty report. With no tenant
  column to filter on, "delete this tenant" and "delete everything" would be the same statement, so it
  declines rather than guessing — and the empty report says so instead of failing silently.

Under database-per-tenant tenancy the tenant's own database is resolved from the tenant id, so the
call reaches the right database without the caller having to know which shape the store has.
