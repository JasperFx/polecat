# Multi-Tenanted Documents

Polecat supports isolating document data by tenant using conjoined tenancy (shared tables with a `tenant_id` column) or separate database tenancy.

## Conjoined Tenancy

When conjoined tenancy is enabled, all document tables include a `tenant_id` column and use a composite primary key of `(tenant_id, id)`:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    opts.Events.TenancyStyle = TenancyStyle.Conjoined;
});
```

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
