# Configuring Document Storage

The `StoreOptions` class is the central configuration object for Polecat. It controls connection settings, schema management, serialization, and more.

## Connection String

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("Server=localhost;Database=myapp;User Id=sa;Password=YourStrong!Password;TrustServerCertificate=True");
});
```

## Schema Name

By default, Polecat uses the `dbo` schema. You can change this:

```cs
opts.DatabaseSchemaName = "myschema";
```

## Auto-Create Schema Objects

Control how Polecat manages database schema:

```cs
// Default: CreateOrUpdate - auto-creates and updates tables
opts.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;

// CreateOnly - only creates new tables, never modifies existing ones
opts.AutoCreateSchemaObjects = AutoCreate.CreateOnly;

// None - never creates or modifies schema objects
opts.AutoCreateSchemaObjects = AutoCreate.None;
```

::: warning
In production environments, consider setting `AutoCreate.None` and managing schema migrations separately.
:::

## Table Prefix

All Polecat tables use the `pc_` prefix:

- `pc_events` -- Event log
- `pc_streams` -- Stream metadata
- `pc_event_progression` -- Async daemon progression
- `pc_hilo` -- HiLo sequence storage
- `pc_doc_{typename}` -- Document tables

## Native JSON Column Type

By default, Polecat uses SQL Server 2025's native `json` data type for document bodies, event data, and headers. To fall back to `nvarchar(max)` for pre-2025 SQL Server instances:

```cs
opts.UseNativeJsonType = false;
```

See [JSON Serialization](/configuration/json#falling-back-to-nvarcharmax) for more details.

## Store Policies

Apply policies across all document types:

```cs
opts.Policies.AllDocumentsSoftDeleted();

opts.Policies.ForDocument<Order>(mapping =>
{
    mapping.DeleteStyle = DeleteStyle.SoftDelete;
});
```

## Listeners

Register global session listeners:

```cs
opts.Listeners.Add(new MySessionListener());
```

See [Session Listeners](/documents/sessions#session-listeners) for more details.

## Logging

Configure store-level logging:

```cs
opts.Logger(new MyPolecatLogger());
```

## HiLo Sequence Defaults

Configure default HiLo sequence settings for numeric identity generation:

```cs
opts.HiloSequenceDefaults.MaxLo = 500; // default is 1000
```

## Initial Data Seeding

Register data to be seeded on application startup:

```cs
opts.InitialData.Add(async (store, ct) =>
{
    await using var session = store.LightweightSession();
    session.Store(new User { FirstName = "Admin", LastName = "User" });
    await session.SaveChangesAsync(ct);
});
```

See [Initial Baseline Data](/documents/initial-data) for more details.
