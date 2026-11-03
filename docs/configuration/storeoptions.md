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

## Translating Database Errors into Your Own Exceptions

If SQL you queue yourself raises errors of its own, such as a guard queued with `QueueSqlCommand` that refuses a write
with `THROW`, register a transform so your callers receive a typed exception instead of picking through a `SqlException`:

```cs
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = connectionString;

    opts.ExceptionTransforms.IfExceptionIs<SqlException>()
        .If(e => e.Number == 51000)
        .ThenTransformTo(e => new GuardRefusedException(e));
});
```

Your transforms are consulted for what `SaveChangesAsync` sends, and for what the async daemon sends when it flushes a
batch, after the failing operation's own translation and before Polecat's other ones. An error none of them claims is reported as it would have been. A transform that matches an error
Polecat also translates, such as a duplicate key (`2627`) that would have been a `DocumentAlreadyExistsException`, replaces
that translation for this store. Transforms belong to the store they are registered on, never to the whole process.

They do not run for reads. Polecat translates nothing there, so a failed query or raw SQL read reports the `SqlException`
it was, and the lock failures of `FetchForExclusiveWriting` and `AppendExclusive` stay `StreamLockedException`. This is
narrower than Marten, whose `Advanced.ExceptionTransforms` also covers raw SQL and LINQ queries.
