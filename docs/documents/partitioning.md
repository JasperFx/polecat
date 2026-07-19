# Table Partitioning

Polecat can declaratively **RANGE-partition a document table** on a member you choose — the SQL Server
companion to Marten's `PartitionOn`. The classic use is a time-series retention table partitioned by
month, so that old data can eventually be pruned by dropping a partition instead of issuing a large
`DELETE`.

::: tip
This is built on SQL Server partition **functions** and **schemes** rather than the child-table model
PostgreSQL/Marten uses, so the migration story differs. It requires `Weasel.SqlServer` 9.3.0 or later.
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

## Limitations

- Supported for **single-tenant** document tables only; combining member RANGE partitioning with
  conjoined multi-tenancy throws at start-up. Conjoined document tables can instead use the managed
  per-tenant partitioning below.
- Dropping aged partitions for retention (`SWITCH`/`MERGE RANGE`) and externally-managed partition
  rolling are not yet wired into the document API — for now, manage those out of band, or keep pruning
  with a predicate delete.

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
