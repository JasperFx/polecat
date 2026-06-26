using System.Diagnostics;
using System.Diagnostics.Metrics;
using JasperFx;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using JasperFx.Events.Descriptors;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Polecat.Events;
using Polecat.Events.Daemon;
using Polecat.Projections;
using Polecat.Storage;
using Polecat.Subscriptions;

namespace Polecat;

public partial class DocumentStore : IEventStore<IDocumentSession, IQuerySession>,
    ISubscriptionRunner<ISubscription>
{
    private static readonly Meter _meter = new("Polecat");
    private static readonly ActivitySource _activitySource = new("Polecat");

    IEventRegistry IEventStore<IDocumentSession, IQuerySession>.Registry => Events;

    string IEventStore<IDocumentSession, IQuerySession>.DefaultDatabaseName => Database.Identifier;

    ErrorHandlingOptions IEventStore<IDocumentSession, IQuerySession>.ContinuousErrors =>
        Options.Projections.Errors;

    ErrorHandlingOptions IEventStore<IDocumentSession, IQuerySession>.RebuildErrors =>
        Options.Projections.RebuildErrors;

    IReadOnlyList<AsyncShard<IDocumentSession, IQuerySession>>
        IEventStore<IDocumentSession, IQuerySession>.AllShards() =>
        Options.Projections.AllShards();

    TimeProvider IEventStore<IDocumentSession, IQuerySession>.TimeProvider => Events.TimeProvider;

    AutoCreate IEventStore<IDocumentSession, IQuerySession>.AutoCreateSchemaObjects =>
        Options.AutoCreateSchemaObjects;

    Meter IEventStore.Meter => _meter;

    ActivitySource IEventStore.ActivitySource => _activitySource;

    string IEventStore.MetricsPrefix => "polecat";

    DatabaseCardinality IEventStore.DatabaseCardinality =>
        Options.Tenancy?.Cardinality ?? DatabaseCardinality.Single;

    bool IEventStore.HasMultipleTenants =>
        Options.Events.TenancyStyle == TenancyStyle.Conjoined
        || Options.Tenancy?.Cardinality == DatabaseCardinality.StaticMultiple;

    // Vary the identity Name by the logical store name so multiple Polecat stores (primary + ancillary)
    // are distinguishable — mirrors Marten's `new(Options.StoreName.ToLowerInvariant(), "marten")`. The
    // Type stays the provider ("SqlServer"). See polecat#207.
    EventStoreIdentity IEventStore.Identity => new(Options.StoreName.ToLowerInvariant(), "SqlServer");

    Uri IEventStore.Subject => Database.DatabaseUri;

    // Store-agnostic accessor for every IEventDatabase backing this store (jasperfx#387).
    // Monitoring/tooling (e.g. CritterWatch) resolves IEventStore from DI and enumerates the
    // databases through this member rather than referencing concrete store types. The tenancy
    // accessor is synchronous and PolecatDatabase implements IEventDatabase, so we project and
    // wrap in a completed ValueTask — mirroring the ProjectionCoordinator's IProjectionDatabase cast.
    ValueTask<IReadOnlyList<IEventDatabase>> IEventStore.AllDatabases() =>
        ValueTask.FromResult<IReadOnlyList<IEventDatabase>>(
            Options.Tenancy?.AllDatabases().Cast<IEventDatabase>().ToList() ?? []);

    Type IEventStore<IDocumentSession, IQuerySession>.IdentityTypeForProjectedType(Type aggregateType) =>
        Events.StreamIdentity == StreamIdentity.AsGuid ? typeof(Guid) : typeof(string);

    IDocumentSession IEventStore<IDocumentSession, IQuerySession>.OpenSession(IEventDatabase database) =>
        LightweightSession();

    IDocumentSession IEventStore<IDocumentSession, IQuerySession>.OpenSession(IEventDatabase database,
        string tenantId) =>
        LightweightSession(new SessionOptions { TenantId = tenantId });

    ErrorHandlingOptions IEventStore<IDocumentSession, IQuerySession>.ErrorHandlingOptions(
        ShardExecutionMode mode) =>
        mode == ShardExecutionMode.Rebuild
            ? Options.Projections.RebuildErrors
            : Options.Projections.Errors;

    IEventLoader IEventStore<IDocumentSession, IQuerySession>.BuildEventLoader(
        IEventDatabase database, ILogger loggerFactory, EventFilterable filtering,
        AsyncOptions shardOptions)
        => BuildEventLoaderInternal(database, filtering, shardName: null);

    // #163 Phase 2 — the 5-arg overload (jasperfx#407 Phase 2c) threads the ShardName, and thus the
    // tenant, into loader construction. The base daemon composes a per-tenant rebuild shard
    // (ShardName.TenantId != null) under per-tenant partitioning, so the loader can bound the scan to
    // that single tenant. When the flag is off (or the shard has no tenant) the load stays store-global.
    IEventLoader IEventStore<IDocumentSession, IQuerySession>.BuildEventLoader(
        IEventDatabase database, ILogger loggerFactory, EventFilterable filtering,
        AsyncOptions shardOptions, ShardName shardName)
        => BuildEventLoaderInternal(database, filtering, shardName);

    private IEventLoader BuildEventLoaderInternal(
        IEventDatabase database, EventFilterable filtering, ShardName? shardName)
    {
        var connStr = database is PolecatDatabase pdb ? pdb.ConnectionString : Options.ConnectionString;

        var tenantFilter = (shardName?.TenantId != null && Events.UseTenantPartitionedEvents)
            ? shardName.TenantId
            : null;

        // The bare PolecatEventLoader is wrapped by the lifted ResilientEventLoader
        // (jasperfx#329), which runs it through Options.ResiliencePipeline and reports
        // loading metrics via EventRequest.Metrics.TrackLoading() — parity Polecat
        // lacked when it inlined the Polly call inside the loader.
        var inner = new PolecatEventLoader(Events, Options, connStr, filtering, tenantFilter);
        return new ResilientEventLoader(Options.ResiliencePipeline, inner, database);
    }

    async ValueTask<IProjectionBatch<IDocumentSession, IQuerySession>>
        IEventStore<IDocumentSession, IQuerySession>.StartProjectionBatchAsync(
            EventRange range, IEventDatabase database, ShardExecutionMode mode,
            AsyncOptions projectionOptions, CancellationToken token)
    {
        var connStr = database is PolecatDatabase pdb ? pdb.ConnectionString : Options.ConnectionString;
        var batch = new PolecatProjectionBatch(this, Events, connStr);
        await batch.RecordProgress(range);
        return batch;
    }

    IReadOnlyEventStore IEventStore.OpenReadOnlyEventStore()
    {
        var session = QuerySession();
        return (IReadOnlyEventStore)session.Events;
    }

    Task IEventStore.CompactStreamAsync(Guid streamId, CancellationToken token)
    {
        throw new NotSupportedException("Stream compaction is not yet supported in Polecat.");
    }

    Task IEventStore.CompactStreamAsync(string streamKey, CancellationToken token)
    {
        throw new NotSupportedException("Stream compaction is not yet supported in Polecat.");
    }

    async Task<EventStoreUsage?> IEventStore.TryCreateUsage(CancellationToken token)
    {
        // Explicitly build — no reflection via base(this)
        var usage = new EventStoreUsage
        {
            Subject = "Polecat.DocumentStore",
            SubjectUri = Database.DatabaseUri,
            Version = GetType().Assembly.GetName().Version?.ToString()!,
            Database = new DatabaseUsage
            {
                Cardinality = DatabaseCardinality.Single,
                MainDatabase = Database.Describe()
            }
        };

        // Event store configuration properties
        usage.AddValue(nameof(Options.Events.StreamIdentity), Options.Events.StreamIdentity);
        usage.AddValue(nameof(Options.Events.TenancyStyle), Options.Events.TenancyStyle);
        usage.AddValue(nameof(Options.Events.EnableExtendedProgressionTracking), Options.Events.EnableExtendedProgressionTracking);
        usage.AddValue(nameof(Options.Events.EnableCorrelationId), Options.Events.EnableCorrelationId);
        usage.AddValue(nameof(Options.Events.EnableCausationId), Options.Events.EnableCausationId);
        usage.AddValue(nameof(Options.Events.EnableHeaders), Options.Events.EnableHeaders);
        if (Options.Events.DatabaseSchemaName != null)
        {
            usage.AddValue(nameof(Options.Events.DatabaseSchemaName), Options.Events.DatabaseSchemaName);
        }

        // Daemon settings child
        var daemon = new OptionsDescription { Subject = "Polecat.DaemonSettings" };
        daemon.AddValue(nameof(Options.DaemonSettings.AsyncMode), Options.DaemonSettings.AsyncMode);
        daemon.AddValue(nameof(Options.DaemonSettings.HealthCheckPollingTime), Options.DaemonSettings.HealthCheckPollingTime);
        daemon.AddValue(nameof(Options.DaemonSettings.LeadershipPollingTime), Options.DaemonSettings.LeadershipPollingTime);
        daemon.AddValue(nameof(Options.DaemonSettings.StaleSequenceThreshold), Options.DaemonSettings.StaleSequenceThreshold);
        daemon.AddValue(nameof(Options.DaemonSettings.SlowPollingTime), Options.DaemonSettings.SlowPollingTime);
        daemon.AddValue(nameof(Options.DaemonSettings.FastPollingTime), Options.DaemonSettings.FastPollingTime);
        daemon.AddValue(nameof(Options.DaemonSettings.AgentPauseTime), Options.DaemonSettings.AgentPauseTime);
        daemon.AddValue(nameof(Options.DaemonSettings.DaemonLockId), Options.DaemonSettings.DaemonLockId);
        usage.Children["DaemonSettings"] = daemon;

        // OpenTelemetry child
        var otel = new OptionsDescription { Subject = "Polecat.OpenTelemetryOptions" };
        otel.AddValue(nameof(Options.OpenTelemetry.TrackConnections), Options.OpenTelemetry.TrackConnections);
        otel.AddValue(nameof(Options.OpenTelemetry.EventCountersEnabled), Options.OpenTelemetry.EventCountersEnabled);
        usage.Children["OpenTelemetry"] = otel;

        // Schema child — event-store table locations resolved through the lifted
        // IDocumentSchemaResolver (jasperfx#333), the cross-store schema-diagnostics surface.
        var schema = new OptionsDescription { Subject = "Polecat.Schema" };
        schema.AddValue(nameof(Options.SchemaResolver.DatabaseSchemaName), Options.SchemaResolver.DatabaseSchemaName);
        schema.AddValue("Events", Options.SchemaResolver.ForEvents());
        schema.AddValue("Streams", Options.SchemaResolver.ForStreams());
        schema.AddValue("EventProgression", Options.SchemaResolver.ForEventProgression());
        usage.Children["Schema"] = schema;

        // HiloSettings child
        var hilo = new OptionsDescription { Subject = "Polecat.HiloSettings" };
        hilo.AddValue(nameof(Options.HiloSequenceDefaults.MaxLo), Options.HiloSequenceDefaults.MaxLo);
        hilo.AddValue(nameof(Options.HiloSequenceDefaults.SequenceName), Options.HiloSequenceDefaults.SequenceName ?? "default");
        hilo.AddValue(nameof(Options.HiloSequenceDefaults.MaxAdvanceToNextHiAttempts), Options.HiloSequenceDefaults.MaxAdvanceToNextHiAttempts);
        usage.Children["HiloSequenceDefaults"] = hilo;

        // JasperFx/ProductSupport#3 — surface the async-daemon error-handling
        // policy on the wire so monitoring tools (CritterWatch) can render the
        // right "shard halts on error" vs "view related dead letters" affordance.
        // Mirror of the Marten companion so the descriptor is populated identically
        // regardless of which store backs the monitored service.
        usage.ProjectionErrors = new ProjectionErrorHandlingDescriptor
        {
            SkipApplyErrors = Options.Projections.Errors.SkipApplyErrors,
            SkipUnknownEvents = Options.Projections.Errors.SkipUnknownEvents,
            SkipSerializationErrors = Options.Projections.Errors.SkipSerializationErrors
        };
        usage.ProjectionRebuildErrors = new ProjectionErrorHandlingDescriptor
        {
            SkipApplyErrors = Options.Projections.RebuildErrors.SkipApplyErrors,
            SkipUnknownEvents = Options.Projections.RebuildErrors.SkipUnknownEvents,
            SkipSerializationErrors = Options.Projections.RebuildErrors.SkipSerializationErrors
        };

        // DCB tag-type registrations — first-class typed list mirroring
        // Marten's plumbing. Polecat doesn't expose a GlobalAggregates surface
        // yet; that list stays empty until the equivalent lands.
        foreach (var registration in Options.EventGraph.TagTypes)
        {
            usage.TagTypes.Add(new JasperFx.Events.Descriptors.TagTypeDescriptor
            {
                TagType = registration.TagType.FullName ?? registration.TagType.Name,
                SimpleType = registration.SimpleType.FullName ?? registration.SimpleType.Name,
                TableSuffix = registration.TableSuffix,
                AggregateType = registration.AggregateType?.FullName,
            });

            usage.DcbTagTypes.Add(new DcbTagDescriptor(
                Name: registration.TagType.Name,
                SimpleType: registration.SimpleType.FullName ?? registration.SimpleType.Name,
                TagType: TypeDescriptor.For(registration.TagType),
                Description: null!));
        }

        // Event-type registry — populates the explorer's "known event types"
        // panel without forcing operators to crack open assembly metadata.
        foreach (var registered in Options.EventGraph.AllKnownEventTypes())
        {
            usage.RegisteredEventTypes.Add(new EventTypeDescriptor(
                EventType: TypeDescriptor.For(registered.EventType),
                Alias: registered.EventTypeName,
                Description: null!));
        }

        // Highest physical seq_id in pc_events — CritterWatch#150 signal 2
        // ("HWM is behind the actual max event sequence") renders the gap
        // between this and the HighWaterMark. Tolerate the lookup failing
        // (e.g. schema not yet created) by leaving null.
        try
        {
            usage.MaxEventSequence = await Options.ResiliencePipeline.ExecuteAsync(static async (state, ct) =>
            {
                var (connString, eventsTable) = state;
                await using var conn = new SqlConnection(connString);
                await conn.OpenAsync(ct);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT MAX(seq_id) FROM {eventsTable};";
                var result = await cmd.ExecuteScalarAsync(ct);
                return result is null or DBNull ? (long?)null : (long?)Convert.ToInt64(result);
            }, (Database.ConnectionString, Events.EventsTableName), token);
        }
        catch
        {
            usage.MaxEventSequence = null;
        }

        Options.Projections.Describe(usage, this);
        return usage;
    }

    public async ValueTask<IProjectionDaemon> BuildProjectionDaemonAsync(
        string? tenantIdOrDatabaseIdentifier = null,
        ILogger? logger = null)
    {
        logger ??= NullLogger.Instance;

        // Resolve the right database — tenant-specific or default
        PolecatDatabase db;
        if (tenantIdOrDatabaseIdentifier != null && Options.Tenancy is SeparateDatabaseTenancy)
        {
            db = Options.Tenancy.GetDatabase(tenantIdOrDatabaseIdentifier);
        }
        else
        {
            db = Database;
        }

        await db.EnsureStorageExistsAsync(typeof(IEvent), CancellationToken.None);

        var connStr = db.ConnectionString;
        var detector = new PolecatHighWaterDetector(Events, connStr,
            Options.DaemonSettings, new Logger<PolecatHighWaterDetector>(new LoggerFactory()),
            Options.ResiliencePipeline);

        return new PolecatProjectionDaemon(this, db, logger, detector);
    }

    async ValueTask<IProjectionDaemon> IEventStore.BuildProjectionDaemonAsync(DatabaseId id)
    {
        return await BuildProjectionDaemonAsync();
    }

    private static string ResolveConnectionString(IEventDatabase database, StoreOptions options)
    {
        return database is PolecatDatabase pdb ? pdb.ConnectionString : options.ConnectionString;
    }

    async Task IEventStore<IDocumentSession, IQuerySession>.RewindSubscriptionProgressAsync(
        IEventDatabase database, string subscriptionName, CancellationToken token, long? sequenceFloor)
    {
        var connStr = ResolveConnectionString(database, Options);
        await Options.ResiliencePipeline.ExecuteAsync(static async (state, ct) =>
        {
            var (connString, progressionTable, name, seqFloor) = state;
            await using var conn = new SqlConnection(connString);
            await conn.OpenAsync(ct);

            if (seqFloor is null or 0)
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"DELETE FROM {progressionTable} WHERE name LIKE @name;";
                cmd.Parameters.AddWithValue("@name", name + "%");
                await cmd.ExecuteNonQueryAsync(ct);
            }
            else
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"""
                    MERGE {progressionTable} AS target
                    USING (SELECT @name AS name) AS source ON target.name = source.name
                    WHEN MATCHED THEN UPDATE SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
                    WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                        VALUES (@name, @seq, SYSDATETIMEOFFSET());
                    """;
                cmd.Parameters.AddWithValue("@name", name);
                cmd.Parameters.AddWithValue("@seq", seqFloor.Value);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }, (connStr, Events.ProgressionTableName, subscriptionName, sequenceFloor), token);
    }

    async Task IEventStore<IDocumentSession, IQuerySession>.RewindAgentProgressAsync(
        IEventDatabase database, string shardName, CancellationToken token, long sequenceFloor)
    {
        var connStr = ResolveConnectionString(database, Options);
        await Options.ResiliencePipeline.ExecuteAsync(static async (state, ct) =>
        {
            var (connString, progressionTable, name, seqFloor) = state;
            await using var conn = new SqlConnection(connString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                MERGE {progressionTable} AS target
                USING (SELECT @name AS name) AS source ON target.name = source.name
                WHEN MATCHED THEN UPDATE SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
                WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                    VALUES (@name, @seq, SYSDATETIMEOFFSET());
                """;
            cmd.Parameters.AddWithValue("@name", name);
            cmd.Parameters.AddWithValue("@seq", seqFloor);
            await cmd.ExecuteNonQueryAsync(ct);
        }, (connStr, Events.ProgressionTableName, shardName, sequenceFloor), token);
    }

    // TeardownExistingProjectionProgressAsync was removed from
    // IEventStore<,> in JasperFx.Events 2.0.0-alpha.13/.14/.15 — it had
    // been [Obsolete] in earlier alphas. Callers now use
    // TeardownExistingProjectionStateAsync (below).
    async Task IEventStore<IDocumentSession, IQuerySession>.TeardownExistingProjectionStateAsync(
        IEventDatabase database, string subscriptionName, CancellationToken token)
    {
        await TeardownProjectionStateAsync(database, subscriptionName, token);
    }

    async Task IEventStore<IDocumentSession, IQuerySession>.DeleteProjectionProgressAsync(
        IEventDatabase database, string subscriptionName, CancellationToken token)
    {
        var connStr = ResolveConnectionString(database, Options);
        await Options.ResiliencePipeline.ExecuteAsync(static async (state, ct) =>
        {
            var (connString, progressionTable, name) = state;
            await using var conn = new SqlConnection(connString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DELETE FROM {progressionTable} WHERE name LIKE @name;";
            cmd.Parameters.AddWithValue("@name", name + "%");
            await cmd.ExecuteNonQueryAsync(ct);
        }, (connStr, Events.ProgressionTableName, subscriptionName), token);
    }

    // #163 Phase 2 — per-tenant rebuild teardown (jasperfx#407 Phase 2b). A non-null tenantId scopes the
    // reset to a single tenant: delete only that tenant's projection-progress rows (the composed
    // {Proj}:{ShardKey}:{tenant} identities) and DELETE the projection's doc rows WHERE tenant_id =
    // tenant, instead of the store-global TRUNCATE. A null tenantId delegates to the store-global path.
    async Task IEventStore<IDocumentSession, IQuerySession>.DeleteProjectionProgressAsync(
        IEventDatabase database, string subscriptionName, string? tenantId, CancellationToken token)
    {
        if (tenantId == null)
        {
            await ((IEventStore<IDocumentSession, IQuerySession>)this)
                .DeleteProjectionProgressAsync(database, subscriptionName, token);
            return;
        }

        var connStr = ResolveConnectionString(database, Options);

        var publishedTableNames = Array.Empty<string>();
        var shardIdentities = Array.Empty<string>();
        if (Options.Projections.TryFindProjection(subscriptionName, out var source))
        {
            publishedTableNames = source.PublishedTypes()
                .Select(t => GetProvider(t).QualifiedTableName)
                .ToArray();

            // The exact per-tenant progression rows to delete: each shard's identity composed for
            // this tenant (carries the trailing :tenantId).
            shardIdentities = source.Shards()
                .Select(s => s.Name.ForTenant(tenantId).Identity)
                .ToArray();
        }

        try
        {
            await Options.ResiliencePipeline.ExecuteAsync(static async (state, ct) =>
            {
                var (connString, progressionTable, identities, tableNames, tenant) = state;
                await using var conn = new SqlConnection(connString);
                await conn.OpenAsync(ct);

                // #180: do the whole tenant teardown in one transaction so a cancellation can only leave
                // it fully applied or fully rolled back — never a half-reset (progression deleted but
                // docs kept, or vice versa). pc_event_progression therefore stays consistent on cancel.
                await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(ct);

                foreach (var identity in identities)
                {
                    await using var delProg = conn.CreateCommand();
                    delProg.Transaction = tx;
                    delProg.CommandText = $"DELETE FROM {progressionTable} WHERE name = @id;";
                    delProg.Parameters.AddWithValue("@id", identity);
                    await delProg.ExecuteNonQueryAsync(ct);
                }

                foreach (var tableName in tableNames)
                {
                    await using var delDocs = conn.CreateCommand();
                    delDocs.Transaction = tx;
                    // The projection's doc table is created lazily on first write, so on a first-ever
                    // rebuild it may not exist yet — guard the tenant-scoped delete accordingly.
                    delDocs.CommandText =
                        $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DELETE FROM {tableName} WHERE tenant_id = @tenant;";
                    delDocs.Parameters.AddWithValue("@tenant", tenant);
                    await delDocs.ExecuteNonQueryAsync(ct);
                }

                await tx.CommitAsync(ct);
            }, (connStr, Events.ProgressionTableName, shardIdentities, publishedTableNames, tenantId), token);
        }
        catch (Exception ex) when (token.IsCancellationRequested && ex is not OperationCanceledException)
        {
            // SqlClient surfaces a cancelled command as a raw SqlException ("Operation cancelled by
            // user" / "a severe error occurred"); translate it so a cancelled rebuild reports clean
            // cancellation. The transaction above has already rolled back, leaving state consistent.
            throw new OperationCanceledException("Per-tenant projection teardown was cancelled.", ex, token);
        }
    }

    // ISubscriptionRunner<ISubscription>
    async Task ISubscriptionRunner<ISubscription>.ExecuteAsync(
        ISubscription subscription, IEventDatabase database, EventRange range,
        ShardExecutionMode mode, CancellationToken token)
    {
        var connStr = ResolveConnectionString(database, Options);
        var batch = new PolecatProjectionBatch(this, Events, connStr);
        await batch.RecordProgress(range);

        // Create a session the subscription can use for reads/writes,
        // and register it with the batch so its pending operations are included
        // in the batch's transaction.
        await using var session = LightweightSession();
        batch.RegisterSession(session);
        var listener = await subscription.ProcessEventsAsync(range, range.Agent, session, token);

        await batch.ExecuteAsync(token);

        // Invoke post-commit listener if provided
        if (listener is not NullChangeListener)
        {
            await listener.AfterCommitAsync(token);
        }
    }

    private async Task TeardownProjectionStateAsync(IEventDatabase database, string subscriptionName,
        CancellationToken token)
    {
        var connStr = ResolveConnectionString(database, Options);

        // Resolve published table names outside the lambda to keep it static
        var publishedTableNames = Array.Empty<string>();
        if (Options.Projections.TryFindProjection(subscriptionName, out var source))
        {
            publishedTableNames = source.PublishedTypes()
                .Select(t => GetProvider(t).QualifiedTableName)
                .ToArray();
        }

        await Options.ResiliencePipeline.ExecuteAsync(static async (state, ct) =>
        {
            var (connString, progressionTable, name, tableNames) = state;
            await using var conn = new SqlConnection(connString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DELETE FROM {progressionTable} WHERE name LIKE @name;";
            cmd.Parameters.AddWithValue("@name", name + "%");
            await cmd.ExecuteNonQueryAsync(ct);

            foreach (var tableName in tableNames)
            {
                await using var truncCmd = conn.CreateCommand();
                truncCmd.CommandText = $"DELETE FROM {tableName};";
                await truncCmd.ExecuteNonQueryAsync(ct);
            }
        }, (connStr, Events.ProgressionTableName, subscriptionName, publishedTableNames), token);
    }
}
