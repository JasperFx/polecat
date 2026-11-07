# Database Management

Polecat uses [Weasel.SqlServer](https://weasel.jasperfx.net/) for all database schema management. Tables are automatically created and updated as needed.

Because every Polecat table is a Weasel schema object, you get the same set of choices every Critter Stack store has: let Polecat migrate
lazily at runtime, apply the schema once at startup, run the migration as a deployment step with `db-apply` or `resources setup`, or hand
`db-patch` scripts to a DBA. See [Schema Migrations](/schema/migrations#choosing-an-approach) for the comparison, and the Weasel documentation
for the [migration engine and `AutoCreate` policy](https://weasel.jasperfx.net/core/schema-migrations.html) and the
[command line tools](https://weasel.jasperfx.net/cli/).

## Auto-Create Modes

Control schema management behavior via `StoreOptions.AutoCreateSchemaObjects`:

| Mode | Behavior |
| :--- | :--- |
| `CreateOrUpdate` (default) | Creates new tables, and alters existing tables to match the configuration: adds new columns, and drops columns and indexes the configuration no longer declares |
| `CreateOnly` | Only creates new tables, never modifies existing ones |
| `None` | No automatic schema management at runtime. An explicit `db-apply` or `resources setup` still migrates |

These are Weasel's [`AutoCreate` values](https://weasel.jasperfx.net/core/schema-migrations.html#autocreate-policy), shared with Marten, Fisher and Wolverine.

```cs
opts.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
```

## Schema Objects

Polecat manages these SQL Server objects:

### Document Tables

- `pc_doc_{typename}` -- One table per document type
- Created on first use (first `Store()`, `Query()`, or `LoadAsync()`)

### Event Store Tables

- `pc_events` -- Global event log
- `pc_streams` -- Stream metadata
- `pc_event_progression` -- Async daemon progress
- `pc_event_projection_floors` -- Persisted projection start floors (only with `Events.EnableProjectionFloors`)

### Support Tables

- `pc_hilo` -- HiLo sequence values for numeric IDs

## Default Schema

All tables are created in the `dbo` schema by default. Change this with:

```cs
opts.DatabaseSchemaName = "myschema";
```

## Weasel Integration

Polecat delegates all DDL generation and execution to Weasel.SqlServer. This provides:

- Diff-based migrations (compares desired vs actual schema)
- Column additions, and under `CreateOrUpdate` the removal of columns the configuration no longer declares; a whole table is never dropped except under `AutoCreate.All`
- Index management
- Foreign key management
- The `db-apply`, `db-assert`, `db-patch`, `db-dump` and `db-list` [command line tools](https://weasel.jasperfx.net/cli/)
- Registration of the Polecat database as a JasperFx [stateful resource](https://shared-libs.jasperfx.net/cli/resources.html), so `resources setup` and `AddResourceSetupOnStartup()` cover it

## PolecatActivator

On application startup, Polecat runs the `PolecatActivator` which:

1. Ensures all configured schema objects exist
2. Runs schema migrations if `AutoCreate` is enabled
3. Executes [initial data seeding](/documents/initial-data) if configured
4. Starts the async daemon if async projections are registered
