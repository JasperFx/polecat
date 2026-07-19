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

## Separate Database Tenancy

Each tenant gets its own database with independent event stores:

```cs
var store = DocumentStore.For(opts =>
{
    opts.MultiTenantedDatabases(databases =>
    {
        databases.AddSingleTenantDatabase("Server=localhost;Database=events_tenant_a;...", "tenant-a");
        databases.AddSingleTenantDatabase("Server=localhost;Database=events_tenant_b;...", "tenant-b");
    });
});
```

With separate database tenancy:

- Each tenant has completely isolated event data
- The async daemon runs independently per database
- Schema management is independent per database

## Default Tenant

When no tenant ID is specified, events are stored with `tenant_id = 'DEFAULT'`. In single-tenant mode, all queries use this default value.

## Per-Tenant Event Partitioning <Badge type="tip" text="4.2" />

::: tip
This is an advanced, opt-in option for large multi-tenanted event stores where a single shared
event sequence becomes a scalability bottleneck. It builds on conjoined event tenancy by giving each
tenant its own event numbering, so a tenant-scoped projection rebuild doesn't have to pay for a
database-wide event-sequence scan. Tracked in
[polecat#163](https://github.com/JasperFx/polecat/issues/163) /
[CritterStack #209](https://github.com/JasperFx/CritterWatch/issues/209).
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
exactly the [#209](https://github.com/JasperFx/CritterWatch/issues/209) win for very large
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
