# Database Management

Polecat uses [Weasel.SqlServer](https://github.com/JasperFx/weasel) for all database schema management. Tables are automatically created and updated as needed.

## Auto-Create Modes

Control schema management behavior via `StoreOptions.AutoCreateSchemaObjects`:

| Mode | Behavior |
| :--- | :--- |
| `CreateOrUpdate` (default) | Creates new tables, adds new columns to existing tables |
| `CreateOnly` | Only creates new tables, never modifies existing ones |
| `None` | No automatic schema management |

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
- Safe column additions (never drops columns)
- Index management
- Foreign key management

## PolecatActivator

On application startup, Polecat runs the `PolecatActivator` which:

1. Ensures all configured schema objects exist
2. Runs schema migrations if `AutoCreate` is enabled
3. Executes [initial data seeding](/documents/initial-data) if configured
4. Starts the async daemon if async projections are registered
