# Marten Features Not Yet in Polecat

A summary of APIs and features available in [Marten](https://github.com/JasperFx/marten) (PostgreSQL) that are not yet implemented in Polecat (SQL Server).

---

## Session Management

| Feature | Description |
|---------|-------------|
| ~~`IDocumentSessionListener`~~ | ~~Hook into SaveChanges lifecycle events~~ **DONE** |
| `IChangeListener` | Listen to async projection changes |
| ~~`session.Eject<T>()`~~ | ~~Remove document from session tracking~~ **DONE** |
| ~~`session.EjectAllOfType(Type)`~~ | ~~Bulk eject by type~~ **DONE** |
| ~~`session.EjectAllPendingChanges()`~~ | ~~Clear pending operations without affecting identity map~~ **DONE** |
| `session.SetHeader() / GetHeader()` | User-defined metadata on sessions |
| ~~`OpenSerializableSessionAsync()`~~ | ~~Session with Serializable isolation level~~ **DONE** |

## Metadata & Tracking Interfaces

| Feature | Description |
|---------|-------------|
| `CreatedSince()` / `CreatedBefore()` | LINQ query helpers for created_at column (no created_at column in schema) |
| ~~`ModifiedSince()` / `ModifiedBefore()`~~ | ~~LINQ query helpers for last_modified column~~ **DONE** |
| `[IndexedCreatedAt]` | Index the created_at column |
| `[IndexedLastModified]` | Index the last_modified column |

## LINQ & Querying

| Feature | Description |
|---------|-------------|
| **Compiled Queries** | `ICompiledQuery<TDoc, TOut>` — parameterized, cached query plans |
| **Query Plans** | `IQueryPlan<T>` — specification pattern for complex queries |
| **Full-Text Search** | `SearchAsync()`, `PlainTextSearchAsync()`, `PhraseSearchAsync()`, `WebStyleSearchAsync()` |
| **Advanced SQL** | `IAdvancedSql` — typed raw SQL queries with tuple result support |
| **MatchesSql** | Raw SQL fragment filters in LINQ (with parameterization) |
| **Stream JSON** | `StreamJson<T>()` — stream results as JSON directly to output |

## Enhanced CRUD

| Feature | Description |
|---------|-------------|
| `TryUpdateRevision<T>(entity, revision)` | Conditional revision update (no-throw variant) |
| `UseIdentityMapFor<T>()` | Opt into identity map for specific type in lightweight session |

## Event Store Enhancements

| Feature | Description |
|---------|-------------|
| ~~**Tombstone Streams**~~ | ~~Mark streams as permanently deleted~~ **DONE** |
| **Event Snapshots** | `Snapshot<T>(SnapshotLifecycle)` — automatic snapshot storage |
| Optimized projection rebuilds | `UseOptimizedProjectionRebuilds` |

## Advanced Projections

| Feature | Description |
|---------|-------------|
| ~~`CompositeProjection`~~ | ~~Multi-stage projection pipelines~~ **DONE** |
| Snapshot management | Automatic snapshot storage and retrieval |
| Projection rebuild/reset | Administrative rebuild of projection data |

## Schema & Index Management

| Feature | Description |
|---------|-------------|
| **Duplicated Fields** | Store JSON path in relational column for efficient indexing |
| **Foreign Keys** | `[ForeignKey]` — cross-document foreign key constraints |
| **Partitioning** | Table partitioning support |
| **Custom DDL Templates** | `[DdlTemplate]` — templated DDL generation |
| **Schema per Document** | `[DatabaseSchemaName]` — custom schema per document type |
| Table name customization | Configure table naming conventions |

## Document Policies & Conventions

| Feature | Description |
|---------|-------------|
| `IDocumentPolicy` | Convention-based document configuration |
| Property searching configuration | `[PropertySearching]` attribute |
| Structural typed storage | `[StructuralTyped]` attribute |
| Custom identity configuration | `[Identity]` attribute |

## Diagnostics & Administration

| Feature | Description |
|---------|-------------|
| `IDiagnostics` | Query analysis tools |
| `PreviewCommand()` | Preview SQL that would be executed |
| `ExplainPlanAsync()` | Get database query execution plan |
| `AllSchemaNames()` | List all schemas |
| `AllObjects()` | List schema objects in dependency order |
| `ToDatabaseScript()` | Generate full SQL creation script |
| `WriteCreationScriptToFile()` | Export schema scripts |
| `CreateMigrationAsync()` | Generate migration scripts |

## Logging & Observability

| Feature | Description |
|---------|-------------|
| `IMartenSessionLogger` | Custom session logging |
| `IMartenLogger` | Document store-level logging |
| OpenTelemetry integration | Tracing for commands and connections |
| `RequestCount` property | Track DB requests per session |

## Initialization & Seeding

| Feature | Description |
|---------|-------------|
| `IInitialData` | Seeding interface for initial data |
| `StoreOptions.InitialData` | Register seed data collections |

---

## Already Implemented in Polecat

The following Marten features have been implemented:

- **Soft deletes** — `[SoftDeleted]` attribute, `ISoftDeleted` interface, `StorePolicies`, `HardDelete()`, `UndoDeleteWhere()`, `MaybeDeleted()`, `IsDeleted()`, `DeletedSince()`, `DeletedBefore()`
- **DeleteWhere / HardDeleteWhere** — bulk delete by predicate expression (respects soft-delete configuration)
- **IVersioned / IRevisioned** — optimistic concurrency with `IVersioned` (Guid-based) and `IRevisioned` (int-based), `ConcurrencyException`, `UpdateExpectedVersion()`, `UpdateRevision()`, version synced on Load and LINQ queries
- **LINQ querying** — Where, OrderBy, Take, Skip, First, Single, Count, Any, Sum, Min, Max, Average, Select, Distinct
- **LINQ extensions** — IsOneOf, In, IsEmpty, AnyTenant, TenantIsOneOf
- **HiLo identity** — int/long ID auto-generation with `[HiloSequence]` attribute
- **JSON serialization config** — EnumStorage, Casing, CollectionStorage, NonPublicMembersStorage
- **Batch querying** — `IBatchedQuery` with `Load`, `LoadMany`, and `Query` (Where, Count, Any, FirstOrDefault) in a single DB roundtrip
- **Metadata interfaces** — `ITracked` (CorrelationId, CausationId, LastModifiedBy) and `ITenanted` (TenantId) synced on save/load
- **Diagnostics** — `CleanAllDocumentsAsync()`, `CleanAsync<T>()`, `CleanAllEventDataAsync()`, `ToSql()` for SQL preview
- **Stream JSON** — `LoadJsonAsync<T>()` and `ToJsonArrayAsync()` for raw JSON without deserialization
- **QueryForNonStaleData** — LINQ extension to wait for async projections before query execution
- **Document patching** — `IPatchExpression<T>` with Set, Increment, Append, AppendIfNotExists, Insert, InsertIfNotExists, Remove, Rename, Delete, Duplicate — all via SQL Server `JSON_MODIFY()`
- **Bulk insert** — `BulkInsertAsync<T>()` with InsertsOnly, IgnoreDuplicates, OverwriteExisting modes
- **Pagination** — `IPagedList<T>` with `ToPagedListAsync()` — two-query approach (COUNT + OFFSET/FETCH)
- **Event archiving** — `ArchiveStream()`, `UnArchiveStream()`, archived events excluded from `FetchStreamAsync()` and daemon loading, append-to-archived-stream prevention
- **FetchLatest** — `FetchLatest<T>(id)` for quick aggregate state retrieval
- **Metadata LINQ helpers** — `ModifiedSince()` / `ModifiedBefore()` for filtering by `last_modified` column
- **Eject methods** — `Eject<T>()`, `EjectAllOfType()`, `EjectAllPendingChanges()` for removing documents from session tracking and pending operations
- **Session listeners** — `IDocumentSessionListener` with `BeforeSaveChangesAsync` / `AfterCommitAsync` hooks, registered globally or per-session
- **Tombstone streams** — `TombstoneStream(Guid/string)` for permanent hard DELETE of stream and all events
- **OpenSessionAsync** — `OpenSessionAsync(SessionOptions)` with configurable `IsolationLevel` (eagerly opens connection + begins transaction for non-ReadCommitted)
- **Composite projections** — `CompositeProjectionFor()` for multi-stage async projection pipelines with parallel execution within stages

---

## Will Not Implement

The following Marten features are out of scope for Polecat by design:

- **Dirty Tracking** — `DirtyTrackedSession()` is not supported. Polecat only supports Lightweight and IdentityMap sessions per CLAUDE.md architecture decisions.
- **GIN Indexes** — `[GinIndexed]` is a PostgreSQL-specific feature with no SQL Server equivalent.
- **Includes/Joins** — `IMartenQueryable<T>.Include<TInclude>()` for eager-loading related documents is not planned.

---

## Priority Assessment

**High priority for production use:**
1. Compiled queries — query caching for performance

**Medium priority:**
2. Event snapshots — automatic snapshot storage
3. Advanced SQL — typed raw SQL queries

**Lower priority (PostgreSQL-specific or niche):**
5. Full-text search (needs SQL Server alternative approach)
6. Advanced SQL / MatchesSql
