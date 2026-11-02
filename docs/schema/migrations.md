# Schema Migrations

Polecat uses Weasel.SqlServer for diff-based schema migrations.

## Auto-Create Behavior

By default (`AutoCreate.CreateOrUpdate`), Polecat automatically creates and updates database schema on application startup:

```cs
opts.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
```

### What Auto-Create Does

- Creates document tables on first use
- Adds new columns when document configuration changes (e.g., enabling soft deletes)
- Creates event store tables on startup, including the tables flat table projections write to
- Creates the HiLo table on first use
- **Never drops columns or tables**

### What Auto-Create Does NOT Do

- Drop existing columns
- Rename columns
- Modify column types
- Delete data

## Disabling Auto-Create

For production environments:

```cs
opts.AutoCreateSchemaObjects = AutoCreate.None;
```

When auto-create is disabled, you're responsible for ensuring the database schema matches your configuration. Use the [schema export](/schema/exporting) feature to generate DDL scripts.

## Migration Flow

On startup, Polecat's migration flow:

1. Compare desired schema (from configuration) against actual database schema
2. Generate DDL for differences (new tables, new columns)
3. Execute DDL within a transaction
4. Run initial data seeding if configured

## Concurrent Migrations

Everything that changes a store's schema takes the same application lock first, so two replicas starting together, or a daemon and a session's first write, cannot collide on a fresh schema. That covers startup migrations, `ApplyAllConfiguredChangesToDatabaseAsync()`, `db-apply`, `resources setup`, the async daemon, a session's first use of the event store or of a document table, the HiLo table and `pc_tenants`. The lock is per schema within a database, so tenants' databases and stores in other schemas do not block each other.

Whoever finds the lock taken waits up to `StoreOptions.StartupMigrationLockTimeout`, 30 seconds by default, and then finds the schema already there:

```cs
opts.StartupMigrationLockTimeout = TimeSpan.FromMinutes(5);
```

If the wait runs out, a session's first use fails and the next use tries again, and an apply fails or carries on according to `ResourceMigrationFailureMode`.

A session with a transaction open never waits, because a migration waiting on that transaction's locks would then wait on the session forever, with nothing SQL Server can detect. If the lock is taken it skips the check, so a table that exists is used as normal and a table that does not fails the statement straight away. A HiLo sequence used outside a session behaves the same way, because it can't tell whether its caller has a transaction open.


## Weasel.SqlServer

All schema management is delegated to Weasel.SqlServer, which provides:

- **Diff-based migrations** -- Only applies changes that are needed
- **Safe operations** -- Never drops columns or tables
- **Idempotent** -- Safe to run multiple times
- **Transaction safety** -- Schema changes are transactional

## Command Line Schema Management

With `RunJasperFxCommands(args)` on a host that calls `AddPolecat(...)`, both families of schema
commands see the Polecat database:

```bash
dotnet run -- db-apply
```

| Command | Does |
|---|---|
| `db-apply` | Apply every outstanding migration |
| `db-assert` | Fail if the database is out of step with the configuration — useful as a deployment gate |
| `db-dump` | Write the DDL to a file instead of executing it |
| `resources setup` | Provision every registered resource, Polecat's schema among them |
| `resources list` / `resources check` | List or verify registered resources |

The `db-*` commands operate on Weasel databases specifically; `resources *` covers every JasperFx
resource in the application, so on a Wolverine host it also provisions envelope storage and broker
topology alongside the Polecat schema. Both work against a single database and against every tenant
database under separate-database or master-table tenancy, and ancillary stores registered with
`AddPolecatStore<T>()` are included.
