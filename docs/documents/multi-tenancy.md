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

Sessions are automatically routed to the correct database.

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
