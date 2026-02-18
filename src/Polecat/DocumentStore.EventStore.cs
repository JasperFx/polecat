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
using Polecat.Events;
using Polecat.Events.Daemon;
using Polecat.Projections;
using Polecat.Storage;

namespace Polecat;

public partial class DocumentStore : IEventStore<IDocumentSession, IQuerySession>
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

    DatabaseCardinality IEventStore.DatabaseCardinality => DatabaseCardinality.Single;

    bool IEventStore.HasMultipleTenants =>
        Options.Events.TenancyStyle == TenancyStyle.Conjoined;

    EventStoreIdentity IEventStore.Identity => new("Polecat", "SqlServer");

    Uri IEventStore.Subject => Database.DatabaseUri;

    Type IEventStore<IDocumentSession, IQuerySession>.IdentityTypeForProjectedType(Type aggregateType) =>
        Options.Events.StreamIdentity == StreamIdentity.AsGuid ? typeof(Guid) : typeof(string);

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
        AsyncOptions shardOptions) =>
        new PolecatEventLoader(Events, Options);

    async ValueTask<IProjectionBatch<IDocumentSession, IQuerySession>>
        IEventStore<IDocumentSession, IQuerySession>.StartProjectionBatchAsync(
            EventRange range, IEventDatabase database, ShardExecutionMode mode,
            AsyncOptions projectionOptions, CancellationToken token)
    {
        var batch = new PolecatProjectionBatch(this, Events);
        await batch.RecordProgress(range);
        return batch;
    }

    async Task<EventStoreUsage?> IEventStore.TryCreateUsage(CancellationToken token)
    {
        var usage = new EventStoreUsage(Database.DatabaseUri, this);
        Options.Projections.Describe(usage, this);
        return usage;
    }

    public async ValueTask<IProjectionDaemon> BuildProjectionDaemonAsync(
        string? tenantIdOrDatabaseIdentifier = null,
        ILogger? logger = null)
    {
        logger ??= NullLogger.Instance;

        await Database.EnsureStorageExistsAsync(typeof(IEvent), CancellationToken.None);

        var detector = new PolecatHighWaterDetector(Events, Options,
            new Logger<PolecatHighWaterDetector>(new LoggerFactory()));

        return new PolecatProjectionDaemon(this, Database, logger, detector);
    }

    async ValueTask<IProjectionDaemon> IEventStore.BuildProjectionDaemonAsync(DatabaseId id)
    {
        return await BuildProjectionDaemonAsync();
    }

    async Task IEventStore<IDocumentSession, IQuerySession>.RewindSubscriptionProgressAsync(
        IEventDatabase database, string subscriptionName, CancellationToken token, long? sequenceFloor)
    {
        await using var conn = new SqlConnection(Options.ConnectionString);
        await conn.OpenAsync(token);

        if (sequenceFloor is null or 0)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DELETE FROM {Events.ProgressionTableName} WHERE name LIKE @name;";
            cmd.Parameters.AddWithValue("@name", subscriptionName + "%");
            await cmd.ExecuteNonQueryAsync(token);
        }
        else
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                MERGE {Events.ProgressionTableName} AS target
                USING (SELECT @name AS name) AS source ON target.name = source.name
                WHEN MATCHED THEN UPDATE SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
                WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                    VALUES (@name, @seq, SYSDATETIMEOFFSET());
                """;
            cmd.Parameters.AddWithValue("@name", subscriptionName);
            cmd.Parameters.AddWithValue("@seq", sequenceFloor.Value);
            await cmd.ExecuteNonQueryAsync(token);
        }
    }

    async Task IEventStore<IDocumentSession, IQuerySession>.RewindAgentProgressAsync(
        IEventDatabase database, string shardName, CancellationToken token, long sequenceFloor)
    {
        await using var conn = new SqlConnection(Options.ConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            MERGE {Events.ProgressionTableName} AS target
            USING (SELECT @name AS name) AS source ON target.name = source.name
            WHEN MATCHED THEN UPDATE SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
            WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                VALUES (@name, @seq, SYSDATETIMEOFFSET());
            """;
        cmd.Parameters.AddWithValue("@name", shardName);
        cmd.Parameters.AddWithValue("@seq", sequenceFloor);
        await cmd.ExecuteNonQueryAsync(token);
    }

#pragma warning disable CS0618 // Obsolete member
    async Task IEventStore<IDocumentSession, IQuerySession>.TeardownExistingProjectionProgressAsync(
        IEventDatabase database, string subscriptionName, CancellationToken token)
    {
        await TeardownProjectionStateAsync(subscriptionName, token);
    }
#pragma warning restore CS0618

    async Task IEventStore<IDocumentSession, IQuerySession>.TeardownExistingProjectionStateAsync(
        IEventDatabase database, string subscriptionName, CancellationToken token)
    {
        await TeardownProjectionStateAsync(subscriptionName, token);
    }

    async Task IEventStore<IDocumentSession, IQuerySession>.DeleteProjectionProgressAsync(
        IEventDatabase database, string subscriptionName, CancellationToken token)
    {
        await using var conn = new SqlConnection(Options.ConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {Events.ProgressionTableName} WHERE name LIKE @name;";
        cmd.Parameters.AddWithValue("@name", subscriptionName + "%");
        await cmd.ExecuteNonQueryAsync(token);
    }

    private async Task TeardownProjectionStateAsync(string subscriptionName, CancellationToken token)
    {
        // Delete projection progress
        await using var conn = new SqlConnection(Options.ConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {Events.ProgressionTableName} WHERE name LIKE @name;";
        cmd.Parameters.AddWithValue("@name", subscriptionName + "%");
        await cmd.ExecuteNonQueryAsync(token);

        // If there's a projection source that publishes document types, truncate those tables
        if (Options.Projections.TryFindProjection(subscriptionName, out var source))
        {
            foreach (var publishedType in source.PublishedTypes())
            {
                var provider = GetProvider(publishedType);
                await using var truncCmd = conn.CreateCommand();
                truncCmd.CommandText = $"DELETE FROM {provider.QualifiedTableName};";
                await truncCmd.ExecuteNonQueryAsync(token);
            }
        }
    }
}
