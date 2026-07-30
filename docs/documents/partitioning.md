# Table Partitioning

Polecat can **RANGE-partition a document table** on a member you choose — the SQL Server companion to
Marten's `PartitionOn`. The classic use is a time-series retention table partitioned by month, so that old
data is reclaimed by retiring a partition instead of issuing a large `DELETE`. Three strategies are
available:

| Strategy                                                       | Who owns the partitions                                                       |
| -------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `ByRange(...)` / `PartitionByRange(...)`                       | you declare a fixed boundary list; Polecat rolls additions forward in place    |
| [`ByRollingRange(...)`](#rolling-time-windows)                  | Polecat, from a rolling-window policy — provisioning *and* retention           |
| [`ByExternallyManagedRange(...)`](#externally-managed-range-partitions) | something outside Polecat, which Polecat then never touches           |

::: tip
This is built on SQL Server partition **functions** and **schemes** rather than the child-table model
PostgreSQL/Marten uses, so the migration story differs. Declarative range partitioning requires
`Weasel.SqlServer` 9.3.0 or later; rolling time windows require 9.21.0 or later.
:::

## Partitioning by a date member

Use `PartitionByRange` in `Schema.For<T>()`, passing the member and the RANGE RIGHT boundary values
(`N` boundaries produce `N + 1` partitions):

```csharp
var store = DocumentStore.For(opts =>
{
    opts.Connection(connectionString);

    opts.Schema.For<MetricsSample>()
        .PartitionByRange(x => x.BucketEnd,
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2026, 2, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero));
});
```

This creates a partition function and scheme for `pc_doc_metricssample` and places the table on the
scheme. Supported member types are dates (`DateTimeOffset`, `DateTime`, `DateOnly`) and integers
(`int`, `long`, `short`), plus `Guid`.

### The promoted partition column

Unless you partition directly on the identity, the member's value is promoted into a real column
(`bucket_end` for `BucketEnd`) that Polecat writes on every upsert. SQL Server requires the partitioning
column to be part of the table's unique (clustered) index, so this column is **added to the primary
key** — meaning the document `Id` is unique together with the partition value. For the typical
time-series case the partition value is derived from immutable document data, so this is transparent.

## Rolling partitions forward

Adding new boundaries over time is an in-place, online operation: Polecat (via Weasel) issues
`ALTER PARTITION FUNCTION ... SPLIT RANGE` rather than rebuilding the table. Extend the boundary list and
re-activate the schema (for example at application start-up):

```csharp
opts.Schema.For<MetricsSample>()
    .PartitionByRange(x => x.BucketEnd,
        /* existing months... */
        new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero),
        new DateTimeOffset(2026, 4, 1, 0, 0, 0, TimeSpan.Zero)); // new
```

Schema migration adds the new partition with no data movement. Removing a boundary or changing the
column/type is reported as a rebuild rather than performed silently.

## Rolling time windows

Declaring every boundary up front only works while the set of partitions is *fixed*. Real time-series
storage needs it to **move**: provision next month, retire last year. Rather than hand-writing that DDL on
a schedule forever, describe the window and let Polecat own it:

```csharp
var store = DocumentStore.For(opts =>
{
    opts.Connection(connectionString);

    // Keep 12 months of history, provision 3 months ahead. Polecat splits in the partitions at the
    // leading edge and retires the aged ones at the trailing edge — no application-authored DDL.
    opts.Schema.For<MetricsSample>()
        .PartitionOn(x => x.BucketEnd)
        .ByRollingRange(PartitionPeriod.Month, periodsAhead: 3, periodsBehind: 12);
});
```

`PartitionPeriod` (from `Weasel.Core.Partitioning`) supports `Hour`, `Day`, `Week`, `Month`, and `Year`, and
the whole window is computed in UTC. The partition member must be a `DateTime` or `DateTimeOffset` — a rolling window is a function of the
clock, so anything else is rejected at *configuration* time with a message that names the member, rather
than surfacing as an opaque partition-function error during the first migration.

There are also overloads taking a `RollingWindowPolicy` directly, or a pre-built `ManagedRangePartitions`.
Pass the *same* manager instance to several document types to roll all of their tables forward in one pass:

```csharp
using Weasel.SqlServer.Tables.Partitioning; // ManagedRangePartitions
using Weasel.Core.Partitioning;             // RollingWindowPolicy, PartitionPeriod

var manager = new ManagedRangePartitions(
    RollingWindowPolicy.Monthly(periodsAhead: 3, periodsBehind: 12),
    column: "bucket_end", sqlDataType: "datetimeoffset");

opts.Schema.For<MetricsSample>().PartitionOn(x => x.BucketEnd).ByRollingRange(manager);
opts.Schema.For<TraceSample>().PartitionOn(x => x.BucketEnd).ByRollingRange(manager);
```

The manager is also where you set `Filegroup` if the partitions should not go to `PRIMARY`, and it takes a
`TimeProvider` so the window can be rolled forward deterministically in tests instead of waiting on the
calendar.

### How the two halves are driven

|                                  | Driven by                        | Why                                                                                                                       |
| -------------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **Provision** the leading edge   | ordinary schema migration        | with a rolling-window manager attached the partition delta is purely additive, so a rolled-forward window is a `SPLIT` |
| **Retire** the trailing edge     | startup pass + `Advanced.*`      | migration never removes data, so the retention half has to be driven separately                                            |

The window is a pure function of the policy and the clock, which is what makes this safe: a window that has
rolled forward differs from the database by exactly one new boundary at the leading edge and one aged
boundary at the trailing edge. A boundary the declaration no longer names is a period that has **aged out** —
the normal steady state of a rolling window, not drift — so `CreateDelta` reports `Additive` or `None`, never
`Rebuild`. (A column or type change still rebuilds, as it must.)

Polecat runs the maintenance pass — roll forward, then retire everything below the retention floor — at
startup, alongside the schema changes it already applies:

```csharp
builder.Services.AddPolecat(opts =>
{
    // ... the ByRollingRange() configuration above
}).ApplyAllDatabaseChangesOnStartup();
```

Applying changes on startup is how a host says "Polecat owns this schema", and retiring a partition is
emphatically a schema change — so the pass is gated on the same opt-in as the migration itself.

### Retention is a partition operation, not a `DELETE`

Retiring a period is `TRUNCATE TABLE ... WITH (PARTITIONS (n))` followed by
`ALTER PARTITION FUNCTION ... MERGE RANGE`, **in that order**. `MERGE RANGE` on a partition that still holds
rows does not reclaim anything — it *moves* those rows into the neighbouring partition, which is the
opposite of the point. Truncating first deallocates the partition's pages in O(1), and the merge that
follows is then metadata-only against an empty partition. If the truncate fails, the boundary is
deliberately left in place.

Only boundaries the policy itself would have produced are ever retired, so a hand-added boundary — or one
left over from a different period size — is left strictly alone.

::: warning
Retiring a period removes its rows. That is the point — it is what makes reclaim O(1) instead of a mass
`DELETE` — but choose `periodsBehind` to match the retention policy you actually want.
:::

If a process is long-lived enough to outrun the number of periods you provision ahead — an hourly window
especially — run the pass yourself on whatever cadence the period size demands:

```csharp
// Roll every rolling-window table forward to its current window and retire the partitions that have
// aged past their retention floor. Idempotent, and safe to run from several nodes at once.
await store.Advanced.ApplyRollingPartitionsAsync(token);

// ...or run just one half
await store.Advanced.RollPartitionsForwardAsync(token);        // additive only, never removes data
await store.Advanced.DropAgedRollingPartitionsAsync(token);    // retention only
```

Each returns Weasel `TablePartitionStatus[]` — one entry per managed table, so a partial failure surfaces
per table rather than taking the whole pass down.

::: tip
A SQL Server RANGE function always spans `(-infinity, +infinity)`, so the outermost partitions absorb any
row written outside the provisioned window. Unlike PostgreSQL there is no "no partition of relation" error
to guard against and no `DEFAULT` overflow partition to declare.
:::

## Externally-managed range partitions

If something genuinely outside Polecat owns the partitions, use the externally-managed variant. Polecat
creates the partition function, scheme, and table once with the supplied initial boundaries and then never
reconciles the partitioning again, so runtime `SPLIT`/`SWITCH`/`MERGE` from elsewhere survives a later
schema apply:

```csharp
opts.Schema.For<MetricsSample>()
    .PartitionOn(x => x.BucketEnd)
    .ByExternallyManagedRange(
        new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
        new DateTimeOffset(2026, 2, 1, 0, 0, 0, TimeSpan.Zero));
```

For an ordinary time-series retention table, prefer [rolling time windows](#rolling-time-windows). Opting
out of Weasel means opting out of its **ordering and dependency management**, not just its DDL generation —
a hand-rolled rebuild that re-creates a partitioned table can easily do so before a helper object its
indexes depend on exists, and the resulting failure looks like a broken index rather than what it is.

## Limitations

- Supported for **single-tenant** document tables only; combining member RANGE partitioning with
  conjoined multi-tenancy throws at start-up. Conjoined document tables can instead use the managed
  per-tenant partitioning below.
- One partition scheme per table is a SQL Server constraint, so a document type cannot combine member
  RANGE partitioning (declared, rolling, or externally managed) with the store's managed per-tenant
  partitioning.

## Managed per-tenant partitioning (#335)

Conjoined multi-tenanted document tables can be physically partitioned **per tenant** through the
store's shared managed tenant partitioning — the SQL Server counterpart of Marten's
`AllDocumentsAreMultiTenantedWithPartitioning` + `PartitionMultiTenantedDocumentsUsingMartenManagement`:

```csharp
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // Make every document conjoined multi-tenanted AND tenant-partitioned:
    opts.Policies.AllDocumentsAreMultiTenantedWithPartitioning();

    // — or, when the store is already conjoined, enable just the partitioning:
    // opts.Events.TenancyStyle = TenancyStyle.Conjoined;
    // opts.Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement();

    // Per-type escape hatch (the [SingleTenanted]/DisablePartitioningIfAny analogue):
    opts.Policies.ForDocument<AuditRecord>(p => p.DisableTenantPartitioning = true);
});
```

Every document table then carries a `tenant_ordinal int` primary-key column and is `RANGE RIGHT`
partitioned on it, driven by the **one `pc_tenant_partitions` registry per database** — the same
registry, ordinals, and physical layout as
[per-tenant event partitioning](/events/multitenancy#per-tenant-event-partitioning), so a store using
both keeps a single coherent tenant → ordinal map across `pc_events`, `pc_streams`, and every
document table. The ordinal is resolved server-side from the registry on every write (upsert, insert,
update, bulk insert, and projection writes), so cross-process ordinal drift cannot mis-route rows.

Tenants are onboarded **lazily on first write** (matching the event-append behavior), or explicitly
with per-table status reporting:

```csharp
// Onboard tenants up front — returns Weasel TablePartitionStatus[] per managed table:
var statuses = await store.Advanced.AddPolecatManagedTenantsAsync(ct, "tenant-a", "tenant-b");

// Tenant bucketing (Weasel 9.18.0): map many small tenants onto one shared partition ordinal.
// Requires ManagedTenantPartitions.AllowOrdinalSharing:
await store.Advanced.AddPolecatManagedTenantsAsync(
    new Dictionary<string, int> { ["small-1"] = 1, ["small-2"] = 1 }, ct);

// Remove a tenant. SQL Server's MERGE RANGE alone would retain the rows —
// TenantDropBehavior.DeleteData physically purges the tenant's rows from every managed
// table first (PostgreSQL managed-drop parity):
await store.Advanced.RemovePolecatManagedTenantsAsync(
    ["tenant-b"], TenantDropBehavior.DeleteData, ct);
```

Notes:

- Requires `TenancyStyle.Conjoined` (asserted at store construction) and cannot be combined with
  member `PartitionByRange` on the same document type — a SQL Server table supports only one
  partition scheme.
- The registry and partition function/scheme are database-global objects: one tenant-partitioned
  store per database.
- `AddPolecatManagedTenantsAsync` splits the tables of document types **registered** with the store
  (`opts.Schema.For<T>()` or prior use); a table created later bakes the full boundary set at
  creation.
- The daemon's dead-letter document (`pc_doc_deadletterevent`) is always excluded, mirroring Marten.
