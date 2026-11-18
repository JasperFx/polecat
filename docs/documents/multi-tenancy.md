# Multi-Tenanted Documents

Polecat supports isolating document data by tenant using conjoined tenancy (shared tables with a `tenant_id` column) or separate database tenancy.

## Conjoined Tenancy

When conjoined tenancy is enabled, all document tables include a `tenant_id` column and use a composite primary key of `(tenant_id, id)`:

Conjoined document tenancy can be turned on three ways, from narrowest to widest:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // 1. One document type at a time. Mirrors Marten's Schema.For<T>().MultiTenanted().
    opts.Schema.For<Order>().MultiTenanted();

    // 2. ...or through a policy, which is the same switch in the policy's spelling.
    opts.Policies.ForDocument<Invoice>(p => p.MultiTenanted = true);

    // 3. ...or every document type in the store.
    opts.Policies.AllDocumentsAreMultiTenanted();
});
```

A conjoined event store still makes **every** document conjoined, which is how document tenancy
worked before the per-type opt-in existed:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    opts.Events.TenancyStyle = TenancyStyle.Conjoined;
});
```

`Events.TenancyStyle` is the *fallback*, so the two combine the way you would expect: the opt-ins
above can make a document conjoined in a store whose events are not, and nothing you have already
configured changes shape. The opt-in only ever widens tenancy — there is deliberately no per-type way
to make a document single-tenanted inside a conjoined event store, because that would retype a
primary key under stores that never asked for it.

::: warning Tenancy is a schema decision
Turning `MultiTenanted()` on for a document type whose table already exists changes its primary key
from `(id)` to `(tenant_id, id)`. That is a migration, not a setting — plan it like one.
:::

### Querying

All queries automatically filter by the session's tenant ID:

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    TenantId = "tenant-a"
});

// Only returns documents belonging to "tenant-a"
var orders = await session.Query<Order>().ToListAsync();
```

### Loading by ID

```cs
// Only loads if the document belongs to the session's tenant
var order = await session.LoadAsync<Order>(orderId);
```

## Separate Database Tenancy

Each tenant gets a completely separate SQL Server database:

```cs
var store = DocumentStore.For(opts =>
{
    opts.MultiTenantedDatabases(databases =>
    {
        databases.AddTenant("tenant-a", "Server=localhost;Database=tenant_a;...");
        databases.AddTenant("tenant-b", "Server=localhost;Database=tenant_b;...");
    });
});
```

Sessions are automatically routed to the correct database. A session stays on that database, so
`session.ForTenant("tenant-b")` from a `tenant-a` session throws `InvalidTenantForDatabaseException`. Open
a separate session for that tenant.

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

## Partitioning your own tables by tenant <Badge type="tip" text="6.0" />

If you add your own tables to the store through `opts.Storage.ExtendedSchemaObjects` and want them partitioned per
tenant the same way as the document and event tables, use `opts.Storage.PartitionPerTenant(table)`:

<!-- snippet: sample_partition_extended_table_per_tenant -->
<a id='snippet-sample_partition_extended_table_per_tenant'></a>
```cs
var table = new Table(new DbObjectName(Schema, "ledger_entries"));
table.AddColumn<Guid>("id").AsPrimaryKey();
table.AddColumn("tenant_id", "varchar(250)").NotNull();
table.AddColumn<int>("amount");

// One partition per tenant on the store's tenant partition scheme, by the tenant_ordinal
// column Polecat adds, and left alone by later schema migrations
opts.Storage.PartitionPerTenant(table);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/MultiTenancy/partition_extended_table_per_tenant.cs#L92-L103' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_partition_extended_table_per_tenant' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The store has to use Polecat-managed tenant partitioning (`Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement()`
or `Events.UseTenantPartitionedEvents`). If it doesn't, building the store throws.

SQL Server partitions by a number, not by the tenant id, so the table is partitioned on the store's tenant partition
scheme by a `tenant_ordinal int NOT NULL` column. Polecat adds the column if the table doesn't have one, adds it to the
table's primary key, because SQL Server requires the partition column in the clustered index, and adds the table to
`ExtendedSchemaObjects` if you haven't already. `AddPolecatManagedTenantsAsync()`, or a tenant's first write, then adds
each tenant's partition to your table along with Polecat's own tables. A later schema migration leaves those
partitions alone, so re-applying the schema to a database that already has tenants doesn't rebuild the table.

Your inserts have to write the tenant's ordinal into `tenant_ordinal`. In SQL, the store's `pc_tenant_ordinal()`
function answers it for a registered tenant, and `NULL` for one that isn't, so the insert fails on the `NOT NULL`
column:

<!-- snippet: sample_insert_into_a_table_partitioned_per_tenant -->
<a id='snippet-sample_insert_into_a_table_partitioned_per_tenant'></a>
```cs
// tenant_ordinal comes from the store's pc_tenant_ordinal() function
var sql = $"""
    INSERT INTO {Schema}.ledger_entries (id, tenant_id, tenant_ordinal, amount)
    VALUES (NEWID(), @tenant_id, {Schema}.pc_tenant_ordinal(@tenant_id), @amount);
    """;
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/MultiTenancy/partition_extended_table_per_tenant.cs#L109-L117' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_insert_into_a_table_partitioned_per_tenant' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

From .NET, `store.Advanced.TenantOrdinalAsync()` answers it, and registers the tenant first if it's new:

<!-- snippet: sample_tenant_ordinal_from_the_store -->
<a id='snippet-sample_tenant_ordinal_from_the_store'></a>
```cs
var ordinal = await store.Advanced.TenantOrdinalAsync("acme", Ct);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/MultiTenancy/partition_extended_table_per_tenant.cs#L245-L247' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_tenant_ordinal_from_the_store' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## ITenanted Interface

Documents implementing `ITenanted` have their `TenantId` property automatically synced:

```cs
public class Order : ITenanted
{
    public Guid Id { get; set; }
    public string TenantId { get; set; } = "";
    public string Description { get; set; } = "";
}

// When stored, order.TenantId is automatically set to the session's tenant
```

See [Multi-Tenancy Configuration](/configuration/multitenancy) for complete setup details.
