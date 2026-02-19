# Marten Features Not Yet in Polecat

A summary of APIs and features available in [Marten](https://github.com/JasperFx/marten) (PostgreSQL) that are not yet implemented in Polecat (SQL Server).

---

## Session Management & Dirty Tracking

| Feature | Description |
|---------|-------------|
| `DirtyTrackedSession()` | Full session with automatic change detection on Store/Update |
| `IDocumentSession.Concurrency` | Optimistic concurrency control property |
| `IDocumentSessionListener` | Hook into SaveChanges lifecycle events |
| `IChangeListener` | Listen to async projection changes |
| `session.Eject<T>()` | Remove document from session tracking |
| `session.EjectAllOfType(Type)` | Bulk eject by type |
| `session.EjectAllPendingChanges()` | Clear pending operations without affecting identity map |
| `session.LastModifiedBy` | Track who modified documents |
| `session.SetHeader() / GetHeader()` | User-defined metadata on sessions |
| `OpenSerializableSessionAsync()` | Session with Serializable isolation level |
| `SessionOptions` class | Fine-grained session configuration (isolation levels, etc.) |

## Document Patching

| Feature | Description |
|---------|-------------|
| `session.Patch<T>(id)` | Fluent JSON patching API |
| `.Set<TValue>()` | Set a field value |
| `.Increment()` | Increment numeric fields (int, long, double, float, decimal) |
| `.Append<T>()` | Append to collections |
| `.AppendIfNotExists<T>()` | Conditional append |
| `.Insert<T>()` / `.InsertIfNotExists<T>()` | Insert at index in collection |
| `.Remove<T>()` | Remove from collection (first/all) |
| `.Rename()` | Rename a field |
| `.Delete()` | Delete a field |
| `.Duplicate<T>()` | Copy field to multiple destinations |

## Metadata & Tracking Interfaces

| Feature | Description |
|---------|-------------|
| `ITracked` | Correlation/causation/user metadata tracking |
| `ITenanted` | Track tenant_id in document for conjoined tenancy |
| `CreatedSince()` / `CreatedBefore()` | LINQ query helpers for created_at column |
| `ModifiedSince()` / `ModifiedBefore()` | LINQ query helpers for last_modified column |
| `[IndexedCreatedAt]` | Index the created_at column |
| `[IndexedLastModified]` | Index the last_modified column |

## LINQ & Querying

| Feature | Description |
|---------|-------------|
| **Compiled Queries** | `ICompiledQuery<TDoc, TOut>` — parameterized, cached query plans |
| **Query Plans** | `IQueryPlan<T>` — specification pattern for complex queries |
| **Full-Text Search** | `SearchAsync()`, `PlainTextSearchAsync()`, `PhraseSearchAsync()`, `WebStyleSearchAsync()` |
| **Includes/Joins** | `IMartenQueryable<T>.Include<TInclude>()` — eager-load related documents |
| **Advanced SQL** | `IAdvancedSql` — typed raw SQL queries with tuple result support |
| **Pagination** | `IPagedList<T>` with `ToPagedListAsync()` — first-class pagination |
| **MatchesSql** | Raw SQL fragment filters in LINQ (with parameterization) |
| **Stream JSON** | `StreamJson<T>()` — stream results as JSON directly to output |
| `QueryForNonStaleData<T>()` | Wait for async daemon to catch up before querying |

## Batch Querying

| Feature | Description |
|---------|-------------|
| `IBatchedQuery` | Batch multiple queries in single DB roundtrip |
| Batch `Load<T>()` / `LoadMany<T>()` | Add document loads to batch |
| Batch `Query<T>()` | Add LINQ queries to batch |
| Batch compiled queries | Add compiled queries to batch |
| `IBatchEvents` | Batch event loading operations |
| `Execute()` | Execute all batched queries at once |

## Bulk Operations

| Feature | Description |
|---------|-------------|
| `BulkInsertAsync<T>()` | High-performance bulk insert (bypasses session) |
| `BulkInsertMode` | InsertsOnly, IgnoreDuplicates, OverwriteExisting, OverwriteIfVersionMatches |

## Enhanced CRUD

| Feature | Description |
|---------|-------------|
| `TryUpdateRevision<T>(entity, revision)` | Conditional revision update (no-throw variant) |
| `UseIdentityMapFor<T>()` | Opt into identity map for specific type in lightweight session |

## Event Store Enhancements

| Feature | Description |
|---------|-------------|
| **Event Archiving** | Archive streams and filter archived events in queries |
| **Tombstone Streams** | Mark streams as permanently deleted |
| **Event Snapshots** | `Snapshot<T>(SnapshotLifecycle)` — automatic snapshot storage |
| `FetchLatest<T>(id)` | Fetch latest projected state without FetchForWriting locking |
| Timestamp-based event fetching | Fetch events by time range |
| Optimized projection rebuilds | `UseOptimizedProjectionRebuilds` |

## Advanced Projections

| Feature | Description |
|---------|-------------|
| `MultiStreamProjection<TDoc, TId>` | Cross-stream aggregations (Polecat has multi-stream but may lack some features) |
| `CompositeProjection` | Multi-stage projection pipelines |
| Snapshot management | Automatic snapshot storage and retrieval |
| Projection rebuild/reset | Administrative rebuild of projection data |

## Schema & Index Management

| Feature | Description |
|---------|-------------|
| **Duplicated Fields** | Store JSON path in relational column for efficient indexing |
| **GIN Indexes** | `[GinIndexed]` — PostgreSQL-specific index type |
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
| `IDocumentCleaner` | Document cleanup/reset tools for testing |
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

## Priority Assessment

**High priority for production use:**
1. Compiled queries — query caching for performance
2. Batch querying — reduce DB roundtrips
3. Bulk insert — high-throughput document ingestion

**Medium priority:**
4. Document patching — incremental JSON updates
5. Session listeners — lifecycle hooks
6. Diagnostics & admin — schema management tools
7. Metadata interfaces (ITracked, ITenanted)
8. Pagination (IPagedList)

**Lower priority (PostgreSQL-specific or niche):**
9. Full-text search (needs SQL Server alternative approach)
10. GIN indexes (PostgreSQL-specific)
11. Dirty tracking sessions
12. Advanced SQL / MatchesSql
13. Event archiving & tombstones
