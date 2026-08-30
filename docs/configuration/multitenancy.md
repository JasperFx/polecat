# Multi-Tenancy

Polecat supports several multi-tenancy strategies for isolating data between tenants — from a single
shared table to a database per tenant. They differ in **where** a tenant's data lives, **how** the
tenant set is managed, and **how** the async projection daemon scales.

## Choosing a Tenancy Strategy

| Strategy | Isolation | Tenant set | Best for |
| --- | --- | --- | --- |
| [Single tenant](#single-tenant-default) | None (shared tables) | n/a | Single-tenant apps |
| [Conjoined](#conjoined-tenancy) | `tenant_id` column, shared tables | Open (any id) | Many tenants, shared schema, simplest ops |
| [Separate database (static)](#separate-database-tenancy) | One database per tenant | Fixed at startup (`AddTenant`) | Strong isolation, known tenant list |
| [Master-table (dynamic)](#dynamic-tenant-management-master-table-tenancy) | One database per tenant | Managed at runtime via a control table | Strong isolation + add/remove/enable/disable tenants without a restart |
| [Per-tenant event partitioning](/events/multitenancy#per-tenant-event-partitioning) | Conjoined + per-tenant event sequence | Open (any id) | Very large multi-tenant **event** stores needing bounded, isolated per-tenant projection rebuilds |

Conjoined and the two separate-database strategies are mutually exclusive choices for **where data
lives**. Per-tenant event partitioning is an **opt-in optimization layered on conjoined event
tenancy** — see [Per-Tenant Event Partitioning](/events/multitenancy#per-tenant-event-partitioning).
For event-store specifics (tenanted streams, the default tenant), see
[Event Multi-Tenancy](/events/multitenancy).

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
        databases.AddTenant("tenant-a", "Server=localhost;Database=tenant_a;...");
        databases.AddTenant("tenant-b", "Server=localhost;Database=tenant_b;...");
    });
});
```

With separate database tenancy:

- Each tenant has completely isolated data
- Schema management runs independently per database — `ApplyAllDatabaseChangesOnStartup()` migrates
  every tenant database, not just the one behind `StoreOptions.ConnectionString`
- Sessions automatically route to the correct database based on tenant ID
- The async daemon runs independently per tenant database: `AddAsyncDaemon(...)` starts one daemon
  per tenant database, each tracking its own high-water mark and projection progress

### No default tenant is required

Calling `MultiTenantedDatabases()` (or `MultiTenantedMasterTable()`) sets
`StoreOptions.DefaultTenantUsageEnabled` to `false`. Every tenant has its own database, so there is
no database the default tenant could coherently mean — and you should **not** register a
placeholder `*DEFAULT*` tenant to satisfy startup.

With the default tenant disabled, opening a session or building a projection daemon without a
tenant throws `DefaultTenantUsageDisabledException` rather than quietly landing on whichever
database happens to back `StoreOptions.ConnectionString`:

```cs
// throws DefaultTenantUsageDisabledException
await using var session = store.LightweightSession();

// correct — always name the tenant
await using var session = store.LightweightSession(new SessionOptions { TenantId = "tenant-a" });
```

Infrastructure that has already resolved a database — the async daemon, for instance — opts out
with `SessionOptions.AllowAnyTenant`, which `SessionOptions.ForDatabase(database)` sets for you.
You can also re-enable the default tenant explicitly if your application genuinely wants it:

```cs
opts.MultiTenantedDatabases(databases => { /* ... */ });
opts.DefaultTenantUsageEnabled = true; // opt back in, after configuring the tenancy
```

This mirrors Marten's `StoreOptions.Advanced.DefaultTenantUsageEnabled`; Polecat has no `Advanced`
sub-object and carries the setting directly on `StoreOptions`.

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

### Store-Agnostic Runtime Tenant Management <Badge type="tip" text="5.8" />

The `MasterTableTenancy` methods above are Polecat-specific. `MasterTableTenancy` also implements
`JasperFx.MultiTenancy.IDynamicTenantSource<string>` — the same store-agnostic abstraction Marten's
`MasterTableTenancy` and `ShardedTenancy` implement — so tooling can drive the runtime tenant
lifecycle without taking a dependency on Polecat's concrete tenancy types:

```cs
var source = (IDynamicTenantSource<string>)store.Options.Tenancy!;

// Always DatabaseCardinality.DynamicMultiple -- the tenant list is read from the
// master table and free to change while the store is running
var cardinality = source.Cardinality;

// Add a tenant with a caller-supplied connection string
await source.AddTenantAsync("tenant-a", "Server=localhost;Database=tenant_a;...");

// Resolve a tenant's connection string; throws UnknownTenantIdException for an
// unknown *or* disabled tenant
var connectionString = await source.FindAsync("tenant-a");

// Soft delete / restore
await source.DisableTenantAsync("tenant-a");
IReadOnlyList<string> disabled = await source.AllDisabledAsync();
await source.EnableTenantAsync("tenant-a");

// Re-read the master table, dropping cached entries for tenants that have since
// been removed or disabled elsewhere
await source.RefreshAsync();

// The currently active tenants. AllActive() returns the tenant *database
// identifiers* rather than raw connection strings, so credentials never reach an
// admin dashboard
IReadOnlyList<string> databases = source.AllActive();
IReadOnlyList<Assignment<string>> byTenant = source.AllActiveByTenant();

// Remove the registry record entirely. The tenant database itself is untouched
await source.RemoveTenantAsync("tenant-a");
```

When the store is configured with `MultiTenantedMasterTable()`, `AddPolecat()` also registers the
tenancy in the container as `IDynamicTenantSource<string>`:

```cs
builder.Services.AddPolecat(opts =>
{
    opts.Connection(connectionString);
    opts.MultiTenantedMasterTable(controlPlaneConnectionString);
});

// ...elsewhere
var source = provider.GetServices<IDynamicTenantSource<string>>().Single();
```

This is what makes a Polecat-backed service's **Tenants** tab editable in CritterWatch — add,
disable, enable, and remove all round-trip against SQL Server with no CritterWatch release required.
Consumers resolve the source with `GetServices<IDynamicTenantSource<string>>()` and degrade to a
read-only tenant list when the collection is empty.

::: tip
The registration is deliberately **conditional**: it happens only when the configured tenancy is a
dynamic source. Single-database stores and static `MultiTenantedDatabases()` stores leave
`GetServices<IDynamicTenantSource<string>>()` empty, which is the signal consumers use to fall back
to a read-only tenant list.
:::

Two caveats:

- The auto-assign overload, `Task<string> AddTenantAsync(string tenantId, CancellationToken)`, throws
  `NotSupportedException`. Database-per-tenant has no pool for Polecat to assign from, so the caller
  must supply a connection string via `AddTenantAsync(tenantId, connectionValue)`. CritterWatch treats
  an empty connection string as "auto-assign" and skips sources that throw.
- The `AddPolecat(Func<IServiceProvider, StoreOptions>)` overload cannot inspect the tenancy while the
  container is still being assembled, so it does not auto-register the source. Configure the store with
  one of the other `AddPolecat` overloads, or register it yourself:

  ```cs
  services.AddSingleton<IDynamicTenantSource<string>>(sp =>
      (IDynamicTenantSource<string>)((DocumentStore)sp.GetRequiredService<IDocumentStore>()).Options.Tenancy!);
  ```

Hard-deleting the physical tenant database is out of scope for this abstraction — SQL Server needs
`ALTER DATABASE ... SET SINGLE_USER WITH ROLLBACK IMMEDIATE` before `DROP DATABASE`, and that is the
consumer's job. Add, disable, enable, and remove all work without it.

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

Under **separate database** tenancy this fallback is switched off — see
[No default tenant is required](#no-default-tenant-is-required) — and a tenantless session throws
`DefaultTenantUsageDisabledException` instead.
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
