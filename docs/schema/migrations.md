# Schema Migrations

Polecat uses [Weasel.SqlServer](https://weasel.jasperfx.net/core/schema-migrations.html) for diff-based schema migrations.

## Choosing an Approach

Polecat always computes a migration the same way: Weasel compares the schema objects your `StoreOptions` describe against the live
database and produces the DDL for the difference. What you choose is *when* that runs and *who* runs it:

| Approach | How | Typical use |
|---|---|---|
| Lazily, on first use | `AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate` (the default), described below | Local development |
| Once at application startup | `.ApplyAllDatabaseChangesOnStartup()` on `AddPolecat()`, or `services.AddResourceSetupOnStartup()` to set up every JasperFx [stateful resource](https://shared-libs.jasperfx.net/cli/resources.html) in the application, Wolverine's message storage and broker objects included | Development, integration tests, and production deployments where the application owns its schema |
| As an explicit deployment step | `dotnet run -- `[`db-apply`](https://weasel.jasperfx.net/cli/db-apply.html), or `dotnet run -- resources setup` for every resource at once, with the application itself on `AutoCreate.None` | Production, when the deployment identity may run DDL |
| From reviewed SQL scripts | `dotnet run -- `[`db-patch`](https://weasel.jasperfx.net/cli/db-patch.html) and [`db-dump`](https://weasel.jasperfx.net/cli/db-dump.html), applied by a DBA or a migration tool, with the application on `AutoCreate.None` | Production, when the application's credentials cannot run DDL |

In every case, `dotnet run -- `[`db-assert`](https://weasel.jasperfx.net/cli/db-assert.html) (or `resources check`) reports whether the database
matches the configuration without changing anything, and is the right gate for a deployment pipeline.

## Auto-Create Behavior

By default (`AutoCreate.CreateOrUpdate`), Polecat automatically creates and updates database schema on application startup:

```cs
opts.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
```

### What Auto-Create Does

- Creates document tables on first use
- Adds new columns when document configuration changes (e.g., enabling soft deletes)
- Drops columns and indexes that the configuration no longer declares on a table it knows about (removing a duplicated field drops that column and its data)
- Creates event store tables on startup, including the tables flat table projections write to
- Creates the HiLo table on first use
- **Never drops a whole table**

### What Auto-Create Does NOT Do

- Drop or recreate a table (only `AutoCreate.All` does that, and only when a change cannot be expressed as an `ALTER`)
- Rename columns
- Touch objects the configuration does not know about

::: warning `CreateOrUpdate` is not "additive only"
For a table it manages, the update delta brings the table in line with the configuration in both directions, so a column that is in the
database and no longer in the configuration is dropped. If a deployment must never lose a schema object, use `AutoCreate.CreateOnly`, which
refuses the migration instead. This is Weasel behaviour shared by every Critter Stack store; see the
[AutoCreate policy](https://weasel.jasperfx.net/core/schema-migrations.html#autocreate-policy).
:::

## Disabling Auto-Create

For production environments:

```cs
opts.AutoCreateSchemaObjects = AutoCreate.None;
```

When auto-create is disabled, you're responsible for ensuring the database schema matches your configuration. There are three ways to meet that:

- Run `dotnet run -- db-apply` or `dotnet run -- resources setup` as a deployment step. Both are an *explicit* instruction to migrate, so both apply the
  changes as `CreateOrUpdate` even when the store is configured with `None`, and `CreateOrUpdate` never drops a table.
- Generate scripts for a DBA with [`db-patch`](https://weasel.jasperfx.net/cli/db-patch.html) (the delta against the live database, plus a rollback
  file) or [`db-dump`](https://weasel.jasperfx.net/cli/db-dump.html) (the whole schema), or programmatically with the [schema export](/schema/exporting) feature.
- Keep `.ApplyAllDatabaseChangesOnStartup()` or `AddResourceSetupOnStartup()` registered. They respect `AutoCreate.None` and do nothing under it, so
  the application can carry the same registration in every environment with the policy deciding whether it acts.

Then gate the deployment with `dotnet run -- `[`db-assert`](https://weasel.jasperfx.net/cli/db-assert.html), which exits non-zero when the database differs
from the configuration. `None` on its own does not report drift: a missing table surfaces as SQL Server's own `Invalid object name` error.

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
- **Bounded operations** -- Never drops a whole table except under `AutoCreate.All`; see the warning above for what `CreateOrUpdate` does drop
- **Idempotent** -- Safe to run multiple times
- **Transaction safety** -- Schema changes are transactional

## Command Line Schema Management

::: tip
The command line itself comes from JasperFx. Its [command line documentation](https://shared-libs.jasperfx.net/cli/)
covers wiring up `RunJasperFxCommands`, [writing your own commands](https://shared-libs.jasperfx.net/cli/writing-commands),
[arguments and flags](https://shared-libs.jasperfx.net/cli/arguments-flags),
[environment checks](https://shared-libs.jasperfx.net/cli/environment-checks) and `describe`. Since JasperFx 2.101.0
the commands also run from a Native AOT binary; see [Native AOT](https://shared-libs.jasperfx.net/cli/#native-aot)
there and [the command line](/configuration/native-aot#the-command-line) on Polecat's Native AOT page.
:::

With `RunJasperFxCommands(args)` on a host that calls `AddPolecat(...)`, both families of schema
commands see the Polecat database:

```bash
dotnet run -- db-apply
```

| Command | Does |
|---|---|
| [`db-apply`](https://weasel.jasperfx.net/cli/db-apply.html) | Apply every outstanding migration. `--parallel <n>` walks a fleet of tenant databases concurrently |
| [`db-assert`](https://weasel.jasperfx.net/cli/db-assert.html) | Fail if the database is out of step with the configuration — useful as a deployment gate |
| [`db-patch`](https://weasel.jasperfx.net/cli/db-patch.html) | Write the delta against the live database as a SQL file, with a rollback file beside it |
| [`db-dump`](https://weasel.jasperfx.net/cli/db-dump.html) | Write the whole DDL to a file instead of executing it |
| [`db-list`](https://weasel.jasperfx.net/cli/db-list.html) | List the databases the host manages, with the identifiers the other commands' `-d` flag takes |
| [`resources setup`](https://shared-libs.jasperfx.net/cli/resources.html) | Provision every registered resource, Polecat's schema among them |
| `resources list` / `resources check` | List or verify registered resources |

The `db-*` commands are [Weasel's](https://weasel.jasperfx.net/cli/) and operate on Weasel databases specifically; `resources *` is JasperFx's
[stateful resource](https://shared-libs.jasperfx.net/cli/resources.html) model and covers every JasperFx
resource in the application, so on a Wolverine host it also provisions envelope storage and broker
topology alongside the Polecat schema. Both work against a single database and against every tenant
database under separate-database or master-table tenancy, and ancillary stores registered with
`AddPolecatStore<T>()` are included.

The same `Setup()` that `resources setup` runs can run at application startup instead, with
`services.AddResourceSetupOnStartup()`. For a Polecat-only host that is equivalent to `.ApplyAllDatabaseChangesOnStartup()`; on a host with
Wolverine it also builds the message storage and any broker objects before the first message moves.
