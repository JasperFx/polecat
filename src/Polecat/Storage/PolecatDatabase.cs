using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events;
using Polecat.Events.Daemon;
using Polecat.Events.Schema;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Storage;

/// <summary>
///     Manages the Polecat database schema lifecycle using Weasel.
///     Handles auto-creation and migration of event store tables.
///     Implements IEventDatabase for async daemon infrastructure.
/// </summary>
public class PolecatDatabase : DatabaseBase<SqlConnection>, IEventDatabase
{
    private readonly StoreOptions _options;
    private readonly EventGraph _events;

    public PolecatDatabase(StoreOptions options)
        : base(
            new DefaultMigrationLogger(),
            options.AutoCreateSchemaObjects,
            new SqlServerMigrator(),
            "Polecat",
            options.ConnectionString)
    {
        _options = options;
        _events = options.EventGraph;
        Tracker = new ShardStateTracker(NullLogger.Instance);
    }

    internal EventGraph Events => _events;

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
        return [new EventStoreFeatureSchema(_events)];
    }

    public override DatabaseDescriptor Describe()
    {
        var builder = new SqlConnectionStringBuilder(_options.ConnectionString);
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
        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT last_seq_id FROM {_events.ProgressionTableName}
            WHERE name = @name;
            """;
        cmd.Parameters.AddWithValue("@name", name.Identity);

        var result = await cmd.ExecuteScalarAsync(token);
        return result is long seq ? seq : 0;
    }

    public async Task<IReadOnlyList<ShardState>> AllProjectionProgress(CancellationToken token = default)
    {
        var list = new List<ShardState>();

        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT name, last_seq_id FROM {_events.ProgressionTableName};";

        await using var reader = await cmd.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            var name = reader.GetString(0);
            var seq = reader.GetInt64(1);
            list.Add(new ShardState(name, seq));
        }

        return list;
    }

    public async Task<long> FetchHighestEventSequenceNumber(CancellationToken token)
    {
        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT ISNULL(MAX(seq_id), 0) FROM {_events.EventsTableName};";

        var result = await cmd.ExecuteScalarAsync(token);
        return result is long seq ? seq : 0;
    }

    public async Task<long?> FindEventStoreFloorAtTimeAsync(DateTimeOffset timestamp, CancellationToken token)
    {
        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT MAX(seq_id) FROM {_events.EventsTableName}
            WHERE timestamp <= @ts;
            """;
        cmd.Parameters.AddWithValue("@ts", timestamp);

        var result = await cmd.ExecuteScalarAsync(token);
        if (result is long seq)
        {
            return seq;
        }

        return null;
    }

    public Task StoreDeadLetterEventAsync(object storage, DeadLetterEvent deadLetterEvent,
        CancellationToken token)
    {
        // No-op for now — dead letter storage deferred to a future stage
        return Task.CompletedTask;
    }

    public new async Task EnsureStorageExistsAsync(Type storageType, CancellationToken token)
    {
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
        var detector = new PolecatHighWaterDetector(_events, _options,
            loggerFactory.CreateLogger<PolecatHighWaterDetector>());
        return new PolecatProjectionDaemon(store, this, loggerFactory, detector);
    }
}
