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

    EventStoreIdentity IEventStore.Identity => new("Polecat", "SqlServer");

    Uri IEventStore.Subject => Database.DatabaseUri;

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
    {
        var connStr = database is PolecatDatabase pdb ? pdb.ConnectionString : Options.ConnectionString;
        return new PolecatEventLoader(Events, Options, connStr, filtering);
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
        usage.Children["OpenTelemetry"] = otel;

        // HiloSettings child
        var hilo = new OptionsDescription { Subject = "Polecat.HiloSettings" };
        hilo.AddValue(nameof(Options.HiloSequenceDefaults.MaxLo), Options.HiloSequenceDefaults.MaxLo);
        hilo.AddValue(nameof(Options.HiloSequenceDefaults.SequenceName), Options.HiloSequenceDefaults.SequenceName ?? "default");
        hilo.AddValue(nameof(Options.HiloSequenceDefaults.MaxAdvanceToNextHiAttempts), Options.HiloSequenceDefaults.MaxAdvanceToNextHiAttempts);
        usage.Children["HiloSequenceDefaults"] = hilo;

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

#pragma warning disable CS0618 // Obsolete member
    async Task IEventStore<IDocumentSession, IQuerySession>.TeardownExistingProjectionProgressAsync(
        IEventDatabase database, string subscriptionName, CancellationToken token)
    {
        await TeardownProjectionStateAsync(database, subscriptionName, token);
    }
#pragma warning restore CS0618

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
