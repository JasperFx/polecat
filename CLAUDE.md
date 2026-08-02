# Polecat - CLAUDE.md

## What is Polecat?

SQL Server 2025-backed Event Store and lightweight Document Database in the Critter Stack ecosystem. Think "Marten for SQL Server" — same API patterns, different database engine.

## Architecture & Key Decisions

- **Table prefix**: `pc_` (pc_events, pc_streams, pc_event_progression, pc_doc_{typename})
- **API naming**: Mirrors Marten — IDocumentStore, IDocumentSession, IQuerySession, IDocumentOperations
- **Stream IDs**: Both Guid and string, configurable via StreamIdentity (like Marten)
- **Session model**: Lightweight (no tracking) and IdentityMap only — **no dirty tracking**
- **Event appending**: QuickAppend only — direct INSERT statements, no stored procedures
- **JSON storage**: SQL Server 2025 native `JSON` type for event data, document bodies, headers, snapshots
- **Serialization**: System.Text.Json only — no Newtonsoft.Json support
- **Default schema**: `dbo`, configurable via StoreOptions
- **Code generation**: C# source generators (compile-time), NOT runtime code generation
- **Target framework**: .NET 10 only
- **Target database**: SQL Server 2025 (v17) only

## Dependencies

- **JasperFx** — core Critter Stack framework (NuGet)
- **JasperFx.Events** — event sourcing abstractions, projection base types, daemon abstractions (NuGet)
- **Weasel.SqlServer** — SQL Server schema management, table definitions, migrations (NuGet)
- **Microsoft.Data.SqlClient** — SQL Server connectivity
- **System.Text.Json** — serialization

Switch to local project references from ~/code/jasperfx and ~/code/weasel if unreleased features are needed.

## Related Codebases

| Codebase | Local Path | Purpose |
|----------|-----------|---------|
| Marten | ~/code/marten | PostgreSQL reference implementation — mirror its patterns |
| Weasel | ~/code/weasel | Schema management framework — use Weasel.SqlServer |
| JasperFx | ~/code/jasperfx | Core + Events framework — implement its interfaces |

## Key Patterns from Marten to Follow

- **DocumentStore** (singleton) creates sessions; `DocumentStore.For(opts => { ... })` factory
- **Sessions** wrap a connection + unit of work; `SaveChangesAsync()` flushes all pending operations
- **EventGraph** manages event store configuration, event type registry, schema table definitions
- **QuickEventAppender** translates stream actions into SQL operations during SaveChanges
- **Projection registration** via `StoreOptions.Projections.Add<T>(lifecycle)`
- **IntegrationContext** test base class pattern for integration tests

## SQL Server vs PostgreSQL Differences

| Feature | PostgreSQL (Marten) | SQL Server (Polecat) |
|---------|-------------------|---------------------|
| JSON storage | `jsonb` type | `json` type (SQL Server 2025) |
| Sequence | `bigserial` / sequences | `bigint IDENTITY(1,1)` |
| Upsert | `INSERT ... ON CONFLICT` | `MERGE` statement |
| Notify | `LISTEN/NOTIFY` | Polling (configurable interval, default 500ms) |
| Advisory locks | `pg_advisory_lock` | `sp_getapplock` / `sp_releaseapplock` |
| Timestamps | `timestamptz` + `now()` | `datetimeoffset` + `SYSDATETIMEOFFSET()` |
| Quick append | PostgreSQL function | Direct INSERT with UPDATE...OUTPUT for version |

## Event Store Schema (pc_ prefix)

**pc_streams**: id, type, version, timestamp, created, snapshot, snapshot_version, tenant_id, is_archived
**pc_events**: seq_id (IDENTITY PK), id, stream_id, version, data (JSON), type, timestamp, tenant_id, dotnet_type, correlation_id, causation_id, headers (JSON), is_archived
**pc_event_progression**: name (PK), last_seq_id, last_updated

## Project Structure

```
src/Polecat/                    — main library
src/Polecat.Tests/              — xUnit integration/unit tests
src/Polecat.CodeGeneration/     — source generator (netstandard2.0)
```

## Development Environment

- Docker Compose (`docker-compose.yml`) provides SQL Server 2025 (`mcr.microsoft.com/mssql/server:2025-latest`) on port **11433**
- SA password: `P@55w0rd`
- Connection (see `Polecat.TestUtils/ConnectionSource.cs`): `Server=localhost,11433;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False`
- Override the connection string via the `POLECAT_TESTING_DATABASE` environment variable (used in CI)
- Tests run against the `master` database, isolated per-test by `DatabaseSchemaName` — there is no dedicated `polecat_testing` database

## Development Stages (ordered by priority)

1. Project infrastructure & configuration (StoreOptions, serialization, connection factory)
2. Schema management with Weasel.SqlServer (table definitions, auto-creation)
3. Test infrastructure (IntegrationContext, fixtures)
4. IDocumentStore/IDocumentSession + basic document ops (Store, Insert, Update, Delete, Load)
5. Event store core (Append, StartStream, FetchStream)
6. DI registration (AddPolecat extension methods)
7. Inline projections (SingleStreamProjection)
8. Live aggregation (AggregateStreamAsync)
9. Conjoined multi-tenancy
10. Async daemon — high water mark & event loader
11. Async daemon — ProjectionDaemon
12. Additional projection types (Multi, Event, FlatTable)
13. FetchForWriting & advanced event operations
14. Separate database multi-tenancy
15. Subscriptions
16. Source generator optimization

Critical path for MVP: Stages 1–5, 7–8, 10–11

## Testing

- **Framework**: xUnit v3 on Microsoft Testing Platform (no VSTest bridge). Each test project is its
  own MTP executable, so `OutputType=Exe` is required — `Microsoft.NET.Test.Sdk` used to set it and is
  no longer referenced, along with `xunit.runner.visualstudio` and `coverlet.collector`.
- **Pattern**: Mirror Marten's IntegrationContext base class
- **Database**: Dockerized SQL Server 2025 on localhost:11433
- **Never run two test runs at once.** Tests share one SQL Server instance and isolate by
  `DatabaseSchemaName` inside `master`, not by database, so concurrent runs — a second `dotnet test`,
  a run started before an earlier one finished, or a run left alive after you killed its parent shell
  — step on each other's schemas and produce large, scattered, misleading failure sets across
  unrelated areas (query plans, flat tables, partitioning, subscriptions). Let a run finish, and
  confirm with `pgrep -f Polecat.Tests` before starting another. A killed run in particular can leave
  its test host alive and its schemas half-torn-down; drop the leftovers before re-running.
- **Test naming**: snake_case file names (e.g., `start_stream_tests.cs`)
- **Assertions**: Shouldly (or similar fluent assertions)
- **Lifecycle**: `IAsyncLifetime` is ValueTask-based and inherits `IAsyncDisposable`. If a class
  implements both `IAsyncDisposable` and `IDisposable`, v3 calls **only** `DisposeAsync` — cleanup put
  in `Dispose()` silently never runs.
- **Cancellation**: pass `TestContext.Current.CancellationToken` to async calls. xUnit1051 enforces
  this and the analyzers are fully enabled; a hung test is this suite's characteristic failure.
- **No entry points in test projects.** A v3 test assembly launches as a process and must own `Main`.
  Top-level statements in a test project make discovery time out and report zero tests. An app under
  test belongs in a factory (see `Polecat.AspNetCore.Testing/TestApp.cs`) driven by
  `AlbaHost.For(builder, configure)`, not in a `Program.cs`.
- **TRX for CI**: `dotnet test` cannot emit TRX under MTP — `--logger "trx;..."` and `-- --report-trx`
  both run, exit 0, and silently write nothing. CI runs the test executable directly with
  `--report-trx`; see `.github/workflows/test-template.yml`.
- **MTP extension packages must stay on the 1.x line** (`TrxReport 1.9.1`, `CodeCoverage 18.0.6`).
  xunit.v3 is built against Microsoft.Testing.Platform 1.x; anything 2.x makes every test run die at
  startup with a `TypeLoadException`. See the comment in `Directory.Packages.props`.

### Writing tests that survive being run in parallel processes

The suite is a candidate for being run across several worker processes at once (Bobcat's supervisor
drives the MTP executable directly; measured **15.9 min → 5.7 min at four workers**). Each worker is
pointed at its own catalog through `POLECAT_TESTING_DATABASE`. Two rules follow, and both were
learned by watching tests break:

**Never rewrite a connection string by text.** This was in three files and was silently wrong:

```csharp
// WRONG — only rewrites while the catalog happens to be "master"
ConnectionSource.ConnectionString.Replace("Initial Catalog=master", $"Database={name}")
```

Point `POLECAT_TESTING_DATABASE` at anything else and the literal is absent, so the replace matches
nothing, the "other" database quietly resolves to the *current* one, and the test asserts against
itself — passing or failing for reasons unrelated to what it is testing. Use the helpers on
`ConnectionSource` instead:

```csharp
ConnectionSource.ConnectionStringFor(name)   // another database on the same server
ConnectionSource.MasterConnectionString      // master, for DDL
ConnectionSource.DatabaseName                // the catalog this process is using
```

**Name every database a test creates with `ConnectionSource.Scoped(...)`.** A database a test
creates is a *sibling* of the process's own database, not a child of it — so giving each worker its
own catalog does **not** isolate them. A hardcoded `"polecat_tenant_a"` is one database shared by
every worker on the box, and they will race to create and drop it:

```csharp
// WRONG — every worker fights over the same database
private const string DbA = "polecat_tenant_a";

// RIGHT — "master_tenant_a" locally, "polecat_w3_tenant_a" under a parallel runner
private static readonly string DbA = ConnectionSource.Scoped("tenant_a");
```

The same rule applies to anything else that lives at server scope rather than inside the catalog:
logins, linked servers, Agent jobs.

Schema names do **not** need scoping — they live inside the catalog, so per-worker databases already
separate them. That is why `SchemaName = "doc_usage"` appearing in nine files is fine.

## Engineering Principles

- Mirror Marten's public API surface where possible for user familiarity
- Use Weasel.SqlServer for ALL schema management — no hand-written DDL scripts
- Implement JasperFx.Events interfaces — don't reinvent the event/projection abstractions
- Opt into the Critter Stack stateful resource model via Weasel's DatabaseResource
- Keep it simple: QuickAppend only, no dirty tracking, STJ only
- Lean on SQL Server 2025 features (JSON type, modern T-SQL) rather than workarounds
- **All database command execution must be covered by `StoreOptions.ResiliencePipeline` (Polly).** Prefer executing through a session (`QuerySession` / `IDocumentSession`), whose `Execute*` methods wrap every command in the pipeline automatically. A component that legitimately owns its own connection/transaction (e.g. the async daemon's high-water detector, projection batch, event loader, bulk insert, HiLo sequence) must wrap its work in `Options.ResiliencePipeline.ExecuteAsync(...)` directly. Do **not** open ad-hoc `SqlConnection`s that run commands outside the pipeline.
  - Documented exceptions: one-time schema/DDL via `DocumentTableEnsurer` and Weasel migration (runs at startup), and test-support helpers.
