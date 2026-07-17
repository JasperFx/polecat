using JasperFx;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events;
using Polecat.Events.Daemon;
using Polecat.Events.Schema;
using Polecat.Linq;
using Polly;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Storage;

/// <summary>
///     Manages the Polecat database schema lifecycle using Weasel.
///     Handles auto-creation and migration of event store tables.
///     Implements IEventDatabase for async daemon infrastructure.
/// </summary>
public class PolecatDatabase : DatabaseBase<SqlConnection>, IEventDatabase, IProjectionDatabase,
    ICrossTenantRebuildSource, Weasel.Storage.IStorageDatabase
{
    private readonly StoreOptions _options;
    private readonly EventGraph _events;
    private readonly string _connectionString;
    private readonly ResiliencePipeline _resilience;

    public PolecatDatabase(StoreOptions options)
        : this(options, options.ConnectionString, "Polecat")
    {
    }

    internal PolecatDatabase(StoreOptions options, string connectionString, string identifier)
        : base(
            new DefaultMigrationLogger(),
            options.AutoCreateSchemaObjects,
            new SqlServerMigrator(),
            identifier,
            connectionString)
    {
        _options = options;
        _events = options.EventGraph;
        _connectionString = connectionString;
        _resilience = options.ResiliencePipeline;
        Tracker = new ShardStateTracker(NullLogger.Instance);
        // Mutates Skipped ShardStates in-place to set SkippedEventsCount.
        // Must be subscribed before any downstream consumers so the augmented
        // count is visible when they observe the state. The lifted observer
        // (jasperfx#329) adds a ShardName == HighWaterMark guard — harmless here
        // since Polecat only ever subscribes it for the HWM tracker.
        Tracker.Subscribe(new SkippedEventsCountObserver());
    }

    /// <summary>
    ///     The connection string this database instance uses.
    /// </summary>
    public string ConnectionString => _connectionString;

    internal EventGraph Events => _events;

    /// <summary>
    ///     Back-reference to the owning store, set by <see cref="DocumentStore" /> during
    ///     construction. Lets the database open sessions for the dead-letter document
    ///     store / count queries (it has no store or session of its own otherwise).
    /// </summary>
    internal DocumentStore? Store { get; set; }

    private DocumentStore RequireStore() =>
        Store ?? throw new InvalidOperationException(
            "PolecatDatabase.Store has not been wired; dead-letter document operations require the owning DocumentStore.");

    public ShardStateTracker Tracker { get; internal set; }

    public Uri DatabaseUri
    {
        get
        {
            var desc = Describe();
            return desc.DatabaseUri();
        }
    }

    public string StorageIdentifier => Identifier;

    public override IFeatureSchema[] BuildFeatureSchemas()
    {
        var naturalKeys = _options.Projections.All
            .OfType<IAggregateProjection>()
            .Where(p => p.NaturalKeyDefinition != null)
            .Select(p => p.NaturalKeyDefinition!)
            .ToList();

        var schemas = new List<IFeatureSchema>
        {
            new EventStoreFeatureSchema(_events, naturalKeys)
        };

        // Document tables + HiLo (if any providers are registered)
        if (_options.Providers != null && _options.Providers.AllProviders.Any())
        {
            schemas.Add(new DocumentFeatureSchema(_options));
        }

        if (_options.ExtendedSchemaObjects.Count > 0)
        {
            schemas.Add(new ExtendedObjectsFeatureSchema(_options.ExtendedSchemaObjects));
        }

        return schemas.ToArray();
    }

    public override DatabaseDescriptor Describe()
    {
        var builder = new SqlConnectionStringBuilder(_connectionString);
        return new DatabaseDescriptor
        {
            Engine = SqlServerProvider.EngineName,
            ServerName = builder.DataSource ?? string.Empty,
            DatabaseName = builder.InitialCatalog ?? string.Empty,
            Subject = GetType().FullNameInCode(),
            Identifier = Identifier
        };
    }

    public async Task<long> ProjectionProgressFor(ShardName name, CancellationToken token = default)
    {
        // #148: route through Options.ResiliencePipeline like the rest of the
        // database access. PolecatDatabase owns its own connection (it has no
        // session), so it wraps the work in the pipeline directly.
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, progressionTable, identity) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                SELECT last_seq_id FROM {progressionTable}
                WHERE name = @name;
                """;
            cmd.Parameters.AddWithValue("@name", identity);

            var result = await cmd.ExecuteScalarAsync(ct);
            return result is long seq ? seq : 0;
        }, (_connectionString, _events.ProgressionTableName, name.Identity), token);
    }

    // #220 (jasperfx#473 / #474, JasperFx.Events 2.16.0): exact-identity progression delete.
    // The JasperFx.Events default throws NotSupportedException, so Polecat must override this or
    // the operation is inert. Unlike the prefix-LIKE DeleteProjectionProgressAsync path on
    // DocumentStore.EventStore.cs, this matches the *raw* ShardName.Identity with exact equality so
    // ejecting (e.g.) "claim_lines:V9:All" cannot drop "claim_lines:V9:AllOther"-style siblings.
    // No registration check by design: the abstraction targets orphaned/unregistered shards too
    // (CritterWatch #476 "Eject Shard"). A non-existent identity is a clean zero-row no-op.
    public async Task DeleteProjectionProgressByShardNameAsync(string shardIdentity, CancellationToken token = default)
    {
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, progressionTable, identity) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DELETE FROM {progressionTable} WHERE name = @identity;";
            cmd.Parameters.AddWithValue("@identity", identity);
            await cmd.ExecuteNonQueryAsync(ct);
        }, (_connectionString, _events.ProgressionTableName, shardIdentity), token);
    }

    public async Task<IReadOnlyList<ShardState>> AllProjectionProgress(CancellationToken token = default)
    {
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events) = state;
            var list = new List<ShardState>();

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            if (events.EnableExtendedProgressionTracking)
            {
                cmd.CommandText = $"SELECT name, last_seq_id, heartbeat, agent_status, pause_reason, running_on_node, warning_behind_threshold, critical_behind_threshold FROM {events.ProgressionTableName};";
            }
            else
            {
                cmd.CommandText = $"SELECT name, last_seq_id FROM {events.ProgressionTableName};";
            }

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(0);
                var seq = reader.GetInt64(1);
                var shardState = new ShardState(name, seq);

                if (events.EnableExtendedProgressionTracking)
                {
                    if (!reader.IsDBNull(2)) shardState.LastHeartbeat = reader.GetDateTimeOffset(2);
                    if (!reader.IsDBNull(3)) shardState.AgentStatus = reader.GetString(3);
                    if (!reader.IsDBNull(4)) shardState.PauseReason = reader.GetString(4);
                    if (!reader.IsDBNull(5)) shardState.RunningOnNode = reader.GetInt32(5);
                    if (!reader.IsDBNull(6)) shardState.WarningBehindThreshold = reader.GetInt64(6);
                    if (!reader.IsDBNull(7)) shardState.CriticalBehindThreshold = reader.GetInt64(7);
                }

                list.Add(shardState);
            }

            return (IReadOnlyList<ShardState>)list;
        }, (_connectionString, _events), token);
    }

    // #324 (jasperfx#435 / jasperfx#518): targeted per-cell progression read. The JasperFx.Events
    // default throws NotSupportedException — null is the meaningful "no row yet" answer, so a store
    // that has not implemented this must not report a live cell as absent. This override reads the
    // single (projection, tenant) row from pc_event_progression.
    //
    // The progression row is keyed by ShardName.Identity (the daemon writes range.ShardName.Identity;
    // see PolecatProjectionBatch.RecordProgress). Tenant is always the trailing ":{tenant}" segment of
    // the identity grammar (ShardName ctor), and a null/default tenant carries no suffix — matching the
    // abstraction's "null tenantId => store-global on a non-tenanted store, or the default-tenant row
    // on a tenanted store". So the lookup key is projectionName as-is when tenantId is null, or
    // "{projectionName}:{tenantId}" otherwise. heartbeat + agent_status are only present under
    // EnableExtendedProgressionTracking; without it, AgentStatus/LastHeartbeat come back null.
    public async ValueTask<ProjectionProgressRow?> ReadProjectionProgressAsync(
        string projectionName, string? tenantId, CancellationToken token)
    {
        var name = string.IsNullOrEmpty(tenantId) ? projectionName : $"{projectionName}:{tenantId}";

        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, projName, tenant, lookupName) = state;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = events.EnableExtendedProgressionTracking
                ? $"SELECT last_seq_id, heartbeat, agent_status FROM {events.ProgressionTableName} WHERE name = @name;"
                : $"SELECT last_seq_id FROM {events.ProgressionTableName} WHERE name = @name;";
            cmd.Parameters.AddWithValue("@name", lookupName);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                // No row for this (projection, tenant) pair yet — the meaningful "not observed" answer.
                return (ProjectionProgressRow?)null;
            }

            var seq = reader.GetInt64(0);
            DateTimeOffset? heartbeat = null;
            string? agentStatus = null;
            if (events.EnableExtendedProgressionTracking)
            {
                if (!reader.IsDBNull(1)) heartbeat = reader.GetDateTimeOffset(1);
                if (!reader.IsDBNull(2)) agentStatus = reader.GetString(2);
            }

            return new ProjectionProgressRow(projName, tenant, seq, agentStatus, heartbeat);
        }, (_connectionString, _events, projectionName, tenantId, name), token);
    }

    // #333 / jasperfx#529 — exact per-cell progression read. Unlike the (projectionName, tenantId) overload
    // above this does no collapsing: it looks up the single pc_event_progression row whose name equals the
    // full ShardName.Identity verbatim (the daemon writes range.ShardName.Identity), so a blue/green deploy's
    // versions, a sliced projection's shard keys, and per-tenant partitions each address their own row. A
    // ShardKey of "All" is the projection's global cell. heartbeat + agent_status are only present under
    // EnableExtendedProgressionTracking; without it AgentStatus/LastHeartbeat come back null.
    public async ValueTask<ProjectionProgressRow?> ReadProjectionProgressAsync(
        ShardName name, CancellationToken token)
    {
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, shard) = state;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = events.EnableExtendedProgressionTracking
                ? $"SELECT last_seq_id, heartbeat, agent_status FROM {events.ProgressionTableName} WHERE name = @name;"
                : $"SELECT last_seq_id FROM {events.ProgressionTableName} WHERE name = @name;";
            cmd.Parameters.AddWithValue("@name", shard.Identity);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                // No row for this identity yet — the meaningful "not observed" answer.
                return (ProjectionProgressRow?)null;
            }

            var seq = reader.GetInt64(0);
            DateTimeOffset? heartbeat = null;
            string? agentStatus = null;
            if (events.EnableExtendedProgressionTracking)
            {
                if (!reader.IsDBNull(1)) heartbeat = reader.GetDateTimeOffset(1);
                if (!reader.IsDBNull(2)) agentStatus = reader.GetString(2);
            }

            return new ProjectionProgressRow(shard.Name, shard.TenantId, seq, agentStatus, heartbeat);
        }, (_connectionString, _events, name), token);
    }

    public async Task<long> FetchHighestEventSequenceNumber(CancellationToken token)
    {
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, eventsTable) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT ISNULL(MAX(seq_id), 0) FROM {eventsTable};";

            var result = await cmd.ExecuteScalarAsync(ct);
            return result is long seq ? seq : 0;
        }, (_connectionString, _events.EventsTableName), token);
    }

    public async Task<long?> FindEventStoreFloorAtTimeAsync(DateTimeOffset timestamp, CancellationToken token)
    {
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, eventsTable, ts) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            // Mirror Marten's MartenDatabase.FindEventStoreFloorAtTimeAsync: the floor is the
            // earliest event at or AFTER the target timestamp (the first sequence the rewind
            // re-applies from). A target before all events therefore resolves to the earliest
            // event's seq_id rather than NULL, so a ToTimestamp rewind re-applies the full stream
            // (within the documented one-event daemon floor boundary). See polecat#205.
            cmd.CommandText = $"""
                SELECT MIN(seq_id) FROM {eventsTable}
                WHERE timestamp >= @ts;
                """;
            cmd.Parameters.AddWithValue("@ts", ts.ToUniversalTime());

            var result = await cmd.ExecuteScalarAsync(ct);
            return result is long seq ? (long?)seq : null;
        }, (_connectionString, _events.EventsTableName, timestamp), token);
    }

    /// <summary>
    ///     #163 Phase 2 — <see cref="ICrossTenantRebuildSource" />: the tenants to rebuild when
    ///     rebuilding <paramref name="projectionName" /> across all tenants. Source of truth is the
    ///     registered partitions in <c>pc_tenant_partitions</c> (every tenant that has appended events
    ///     has a partition). Returns empty when per-tenant partitioning is off. Mirrors Marten's
    ///     <c>MartenDatabase.FindRebuildTenantsAsync</c>.
    /// </summary>
    public async Task<IReadOnlyList<string>> FindRebuildTenantsAsync(string projectionName,
        CancellationToken token)
    {
        if (!_events.UseTenantPartitionedEvents) return [];

        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, tenantPartitionsTable) = state;
            var tenants = new List<string>();

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT tenant_id FROM {tenantPartitionsTable} ORDER BY tenant_id;";

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                tenants.Add(reader.GetString(0));
            }

            return (IReadOnlyList<string>)tenants;
        }, (_connectionString, _events.TenantPartitionsTableName), token);
    }

    public async Task StoreDeadLetterEventAsync(object storage, DeadLetterEvent deadLetterEvent,
        CancellationToken token)
    {
        // DeadLetterEvent is a Polecat document (pc_doc_deadletterevent). Store it
        // through a session; Polecat assigns the Guid id when the framework leaves it
        // default. Mirrors Marten's document-backed dead-letter storage.
        await using var session = RequireStore().LightweightSession();
        session.Store(deadLetterEvent);
        await session.SaveChangesAsync(token);
    }

    /// <summary>
    ///     Count stored dead-letter events for a single shard (jasperfx#356) via a LINQ
    ///     query over the <see cref="DeadLetterEvent" /> document. The event records
    ///     <see cref="ShardName.Name" /> as <c>ProjectionName</c> and
    ///     <see cref="ShardName.ShardKey" /> as <c>ShardName</c>.
    /// </summary>
    public async Task<long> CountDeadLetterEventsAsync(ShardName shard, CancellationToken token = default)
    {
        var projectionName = shard.Name;
        var shardKey = shard.ShardKey;

        await using var session = RequireStore().QuerySession();
        return await session.Query<DeadLetterEvent>()
            .Where(x => x.ProjectionName == projectionName && x.ShardName == shardKey)
            .LongCountAsync(token);
    }

    /// <summary>
    ///     Count stored dead-letter events grouped by shard across all projections
    ///     (jasperfx#356). Dead letters are typically few, so the rows are materialized
    ///     and grouped in memory rather than relying on a SQL GROUP BY translation.
    /// </summary>
    public async Task<IReadOnlyList<DeadLetterShardCount>> FetchDeadLetterCountsAsync(
        CancellationToken token = default)
    {
        await using var session = RequireStore().QuerySession();
        var all = await session.Query<DeadLetterEvent>().ToListAsync(token);

        return all
            .GroupBy(x => (x.ProjectionName, x.ShardName))
            .Select(g => new DeadLetterShardCount(g.Key.ProjectionName, g.Key.ShardName, g.LongCount()))
            .ToList();
    }

    /// <summary>
    ///     Per-tenant dead-letter counts (CritterWatch#381 / jasperfx#450). The dead-letter table is
    ///     store-global, but each row records the failing event's <see cref="DeadLetterEvent.TenantId" />,
    ///     so counts that would otherwise collide on <c>{ProjectionName}:{ShardName}</c> are separated
    ///     per tenant. A null <paramref name="tenantId" /> falls back to the store-global shape.
    /// </summary>
    public async Task<IReadOnlyList<DeadLetterShardCount>> FetchDeadLetterCountsAsync(
        string? tenantId, CancellationToken token = default)
    {
        if (tenantId == null)
        {
            return await FetchDeadLetterCountsAsync(token);
        }

        await using var session = RequireStore().QuerySession();
        var all = await session.Query<DeadLetterEvent>().ToListAsync(token);

        return all
            .Where(x => x.TenantId == tenantId)
            .GroupBy(x => (x.ProjectionName, x.ShardName))
            .Select(g => new DeadLetterShardCount(g.Key.ProjectionName, g.Key.ShardName, g.LongCount(), tenantId))
            .ToList();
    }

    /// <summary>
    ///     CritterWatch#369: fetch the stored dead-letter event rows for a single shard — the drill-in
    ///     companion to <see cref="CountDeadLetterEventsAsync" />. Most recent failures first (by event
    ///     sequence), paged. A null <paramref name="tenantId" /> spans every tenant; otherwise scopes to
    ///     one partition. Dead letters are few, so rows are materialized then ordered/paged in memory
    ///     (consistent with <see cref="FetchDeadLetterCountsAsync(CancellationToken)" />).
    /// </summary>
    public async Task<IReadOnlyList<DeadLetterEvent>> QueryDeadLetterEventsAsync(ShardName shard,
        string? tenantId, int offset, int limit, CancellationToken token = default)
    {
        var projectionName = shard.Name;
        var shardKey = shard.ShardKey;

        await using var session = RequireStore().QuerySession();
        var all = await session.Query<DeadLetterEvent>().ToListAsync(token);

        return all
            .Where(x => x.ProjectionName == projectionName && x.ShardName == shardKey
                && (tenantId == null || x.TenantId == tenantId))
            .OrderByDescending(x => x.EventSequence)
            .Skip(offset)
            .Take(limit)
            .ToList();
    }

    public new async Task EnsureStorageExistsAsync(Type storageType, CancellationToken token)
    {
        // #267: honor AutoCreate.None. When the user opts out of runtime schema management
        // (e.g. the schema is owned by EF migrations on a least-privilege Azure SQL connection
        // with no ALTER), no DDL may run on a storage-ensure. ApplyAllConfiguredChangesToDatabaseAsync
        // would otherwise force-apply the WHOLE schema — and Weasel's DatabaseBase promotes
        // None -> CreateOrUpdate, so any drift between the live schema and Polecat's model emits
        // ALTER/sp_rename and fails. This is the daemon's first storage access (see
        // DocumentStore.BuildProjectionDaemonAsync); on-the-fly session/query creation is already
        // gated in DocumentTableEnsurer (#219).
        if (AutoCreate == AutoCreate.None)
        {
            return;
        }

        await ApplyAllConfiguredChangesToDatabaseAsync(ct: token);
    }

    public async Task WaitForNonStaleProjectionDataAsync(TimeSpan timeout)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        while (stopwatch.Elapsed < timeout)
        {
            var highWater = await FetchHighestEventSequenceNumber(CancellationToken.None);
            if (highWater == 0) return;

            var progress = await AllProjectionProgress(CancellationToken.None);

            // Filter out the HighWaterMark entry — only check actual projection shards
            var projectionProgress = progress
                .Where(p => p.ShardName != "HighWaterMark")
                .ToList();

            // All projection shards must be caught up to the high water mark
            if (projectionProgress.Count > 0 && projectionProgress.All(p => p.Sequence >= highWater))
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException(
            $"Timed out after {timeout} waiting for projection data to become non-stale.");
    }

    internal PolecatProjectionDaemon StartProjectionDaemon(DocumentStore store, ILoggerFactory loggerFactory)
    {
        var detector = new PolecatHighWaterDetector(_events, _connectionString,
            _options.DaemonSettings, loggerFactory.CreateLogger<PolecatHighWaterDetector>(),
            _options.ResiliencePipeline);
        return new PolecatProjectionDaemon(store, this, loggerFactory, detector);
    }

    // ---- Weasel.Storage.IStorageDatabase (#273) ----
    // The dialect-neutral database seam of the shared closed-shape storage runtime
    // (weasel#329/#331). Sessions expose this through IStorageSession.Database once they
    // retarget onto the shared contract.

    /// <summary>
    ///     The closed-shape provider graph (#273 phase E1) — lazily builds the shared-runtime
    ///     storage flavors per document type over the registry's mappings.
    /// </summary>
    Weasel.Storage.IProviderGraph Weasel.Storage.IStorageDatabase.Providers => _options.Providers.ClosedShapeGraph;

    /// <summary>
    ///     Creates an unopened connection to this database (shared-runtime counterpart of
    ///     <see cref="DatabaseBase{T}.CreateConnection" />). Callers own the connection and are
    ///     responsible for resilience around its use.
    /// </summary>
    public System.Data.Common.DbConnection CreateStorageConnection() => CreateConnection();

    public Task RunSqlAsync(string sql, CancellationToken ct = default)
    {
        // Like the rest of PolecatDatabase's direct access, this owns its own connection and
        // wraps the work in Options.ResiliencePipeline directly.
        return _resilience.ExecuteAsync(static async (state, token) =>
        {
            var (connectionString, sqlText) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(token);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sqlText;
            await cmd.ExecuteNonQueryAsync(token);
        }, (_connectionString, sql), ct).AsTask();
    }

    /// <summary>
    ///     Resolves the Hi-Lo sequence for a document type through the store's single
    ///     <see cref="SequenceFactory" /> (which caches Hi-Lo state per sequence).
    /// </summary>
    public ISequence SequenceFor(Type documentType) => RequireStore().Sequences.SequenceFor(documentType);
}
