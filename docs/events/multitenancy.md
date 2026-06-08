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

* **Maintains a tenant registry** — `pc_tenant_partitions` maps each `tenant_id` to a compact
  `partition_id`, populated the first time a tenant appends events.
* **Gives each tenant its own sequence** — `seq_id` is drawn from a per-tenant
  `pc_events_sequence_{partition_id}` object (created on demand) via `NEXT VALUE FOR`, rather than a
  single global `IDENTITY`. `seq_id` is therefore unique only *within* a tenant, so the `pc_events`
  primary key becomes composite `(tenant_id, seq_id)`.

```cs
// Each tenant's seq_id starts at 1 and advances independently
await using var red = store.LightweightSession(new SessionOptions { TenantId = "Red" });
red.Events.StartStream(redStream, new QuestStarted("Red"));   // Red seq_id 1
await red.SaveChangesAsync();

await using var blue = store.LightweightSession(new SessionOptions { TenantId = "Blue" });
blue.Events.StartStream(blueStream, new QuestStarted("Blue")); // Blue seq_id 1 — independent
await blue.SaveChangesAsync();
```

::: warning
`UseTenantPartitionedEvents` defaults to `false`; existing stores keep the global `IDENTITY` append
path byte-for-byte. The flag **requires** `TenancyStyle.Conjoined` (there is nothing to partition by
otherwise) and is currently incompatible with `UseArchivedStreamPartitioning` — a SQL Server table
supports only one partition scheme; both raise an error at store construction.

This first phase delivers per-tenant **sequencing** (the bounded-scan win). The tenant-aware async
daemon (per-tenant high-water + per-tenant rebuild) and physical per-tenant table partitioning land
in follow-ups — [polecat#163](https://github.com/JasperFx/polecat/issues/163) and
[polecat#171](https://github.com/JasperFx/polecat/issues/171). Until the daemon phase ships, treat
the flag as experimental for asynchronous projections.
:::
