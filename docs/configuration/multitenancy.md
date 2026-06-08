# Multi-Tenancy with Database per Tenant

Polecat supports multiple multi-tenancy strategies for isolating data between tenants.

## Tenancy Styles

### Single Tenant (Default)

All data lives in one set of tables with no tenant isolation:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    // This is the default -- no tenant isolation
});
```

### Conjoined Tenancy

All tenants share the same database and tables, but data is isolated by a `tenant_id` column:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // Enable conjoined tenancy for events
    opts.Events.TenancyStyle = TenancyStyle.Conjoined;
});
```

With conjoined tenancy:

- All document tables get a `tenant_id` column
- Document primary keys become composite: `(tenant_id, id)`
- All queries automatically filter by the session's tenant ID
- Event streams are isolated per tenant

Specify the tenant when creating a session:

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    TenantId = "tenant-abc"
});
```

See [Multi-Tenanted Documents](/documents/multi-tenancy) and [Event Multi-Tenancy](/events/multitenancy) for more details.

### Separate Database Tenancy

Each tenant gets their own isolated SQL Server database:

```cs
var store = DocumentStore.For(opts =>
{
    opts.MultiTenantedDatabases(databases =>
    {
        databases.AddSingleTenantDatabase("Server=localhost;Database=tenant_a;...", "tenant-a");
        databases.AddSingleTenantDatabase("Server=localhost;Database=tenant_b;...", "tenant-b");
    });
});
```

With separate database tenancy:

- Each tenant has completely isolated data
- Schema management runs independently per database
- Sessions automatically route to the correct database based on tenant ID
- The async daemon runs independently per tenant database

### Dynamic Tenant Management (Master Table Tenancy)

`MultiTenantedDatabases` above is **static** — the full tenant list is fixed when the store is
configured. When you need to add, remove, enable, or disable tenants **at runtime** without
restarting the service, use the **master table** strategy. A control-plane table (`pc_tenants`)
maps each `tenant_id` to its connection string, and Polecat reads from it dynamically:

```cs
var store = DocumentStore.For(opts =>
{
    // Default/fallback connection
    opts.Connection("...");

    // The control-plane database that holds the pc_tenants registry
    opts.MultiTenantedMasterTable("Server=localhost;Database=control_plane;...");
});
```

`MultiTenantedMasterTable` returns a `MasterTableTenancy` you can drive from operational code (for
example, a CritterWatch tenant-management handler). The master table is created automatically on
first use:

```cs
var tenancy = (MasterTableTenancy)store.Options.Tenancy!;

// Register a tenant -> connection string mapping at runtime (idempotent upsert)
await tenancy.AddDatabaseRecordAsync("tenant-a", "Server=localhost;Database=tenant_a;...");

// Temporarily take a tenant offline without losing its record...
await tenancy.DisableTenantAsync("tenant-a");
// ...and bring it back
await tenancy.EnableTenantAsync("tenant-a");

// Inspect which tenants are currently disabled
IReadOnlyList<string> disabled = await tenancy.AllDisabledAsync();

// Remove a tenant record entirely (the tenant database itself is left untouched)
await tenancy.DeleteDatabaseRecordAsync("tenant-a");

// Materialize the full set of currently-enabled tenant databases
// (e.g. to apply schema to each)
foreach (var db in await tenancy.BuildDatabasesAsync())
{
    await db.ApplyAllConfiguredChangesToDatabaseAsync();
}
```

Notes:

- The master table is `pc_tenants` (`tenant_id`, `connection_string`, `is_disabled`) and lives in the
  schema you pass to `MultiTenantedMasterTable` (defaults to `StoreOptions.DatabaseSchemaName`).
- `AddDatabaseRecordAsync` records the mapping and re-enables a previously-disabled tenant; it does
  **not** create the tenant database — provision that separately (the connection string must point at
  an existing database).
- Disabled or unknown tenants raise `UnknownTenantIdException` when a session is opened for them,
  exactly like static separate-database tenancy.
- All master-table access flows through `StoreOptions.ResiliencePipeline`.

This is the Polecat (SQL Server) equivalent of Marten's `MultiTenantedDatabasesViaMasterTable` /
`MasterTableTenancy`.

## Setting the Tenant ID

The tenant ID is set when opening a session:

```cs
// Via SessionOptions
await using var session = store.LightweightSession(new SessionOptions
{
    TenantId = "my-tenant"
});
```

::: warning
If no tenant ID is specified, Polecat uses `"DEFAULT"` as the tenant ID. In conjoined tenancy mode, this means documents and events will be stored with `tenant_id = 'DEFAULT'`.
:::

## ITenanted Interface

Documents that implement `ITenanted` will have their `TenantId` property automatically synced from the session:

```cs
public class Order : ITenanted
{
    public Guid Id { get; set; }
    public string TenantId { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
}
```
