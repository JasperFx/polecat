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

- Docker Compose provides SQL Server 2025 on port **11433**
- Connection: `Server=localhost,11433;User Id=sa;Password=Polecat#Dev2025;TrustServerCertificate=True`
- Test database: `polecat_testing`

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

- **Framework**: xUnit
- **Pattern**: Mirror Marten's IntegrationContext base class
- **Database**: Dockerized SQL Server 2025 on localhost:11433
- **Test naming**: snake_case file names (e.g., `start_stream_tests.cs`)
- **Assertions**: Shouldly (or similar fluent assertions)

## Engineering Principles

- Mirror Marten's public API surface where possible for user familiarity
- Use Weasel.SqlServer for ALL schema management — no hand-written DDL scripts
- Implement JasperFx.Events interfaces — don't reinvent the event/projection abstractions
- Opt into the Critter Stack stateful resource model via Weasel's DatabaseResource
- Keep it simple: QuickAppend only, no dirty tracking, STJ only
- Lean on SQL Server 2025 features (JSON type, modern T-SQL) rather than workarounds
