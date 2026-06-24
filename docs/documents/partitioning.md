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

- Supported for **single-tenant** document tables only; combining partitioning with conjoined
  multi-tenancy throws at start-up.
- Dropping aged partitions for retention (`SWITCH`/`MERGE RANGE`) and externally-managed partition
  rolling are not yet wired into the document API — for now, manage those out of band, or keep pruning
  with a predicate delete.
