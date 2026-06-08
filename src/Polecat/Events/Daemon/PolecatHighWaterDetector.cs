using System.Text.Json;
using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Polly;

namespace Polecat.Events.Daemon;

/// <summary>
///     SQL Server implementation of IHighWaterDetector.
///     Detects the highest contiguous seq_id in pc_events and manages
///     the high water mark in pc_event_progression.
///     All SQL execution is wrapped with Polly resilience.
/// </summary>
internal class PolecatHighWaterDetector : IHighWaterDetector
{
    private readonly EventGraph _events;
    private readonly string _connectionString;
    private readonly DaemonSettings _daemonSettings;
    private readonly ILogger<PolecatHighWaterDetector> _logger;
    private readonly ResiliencePipeline _resilience;

    public PolecatHighWaterDetector(EventGraph events, string connectionString,
        DaemonSettings daemonSettings, ILogger<PolecatHighWaterDetector> logger,
        ResiliencePipeline resilience)
    {
        _events = events;
        _connectionString = connectionString;
        _daemonSettings = daemonSettings;
        _logger = logger;
        _resilience = resilience;

        var builder = new SqlConnectionStringBuilder(connectionString);
        var server = builder.DataSource ?? "localhost";
        // SQL Server uses comma for port (e.g. "localhost,11433") which is invalid in URIs
        if (server.Contains(','))
        {
            server = server.Replace(',', ':');
        }

        DatabaseUri = new Uri($"sqlserver://{server}/{builder.InitialCatalog}");
    }

    public Uri DatabaseUri { get; }

    public async Task<HighWaterStatistics> Detect(CancellationToken token)
    {
        var stats = await LoadStatisticsAsync(token);

        if (stats.CurrentMark == stats.HighestSequence)
        {
            return stats;
        }

        var (gapSeqId, _, maxSeqId) = await DetectGapAsync(stats.CurrentMark + 1, token);

        if (gapSeqId.HasValue)
        {
            // The gap starts AFTER gapSeqId, so everything up to gapSeqId is contiguous
            stats.CurrentMark = gapSeqId.Value;
        }
        else if (maxSeqId.HasValue)
        {
            stats.CurrentMark = maxSeqId.Value;
        }

        if (stats.HasChanged)
        {
            await MarkHighWaterAsync(stats.CurrentMark, token);
        }

        return stats;
    }

    public async Task<HighWaterStatistics> DetectInSafeZone(CancellationToken token)
    {
        var stats = await LoadStatisticsAsync(token);

        if (stats.CurrentMark == stats.HighestSequence)
        {
            return stats;
        }

        var start = stats.CurrentMark + 1;
        var (gapSeqId, minSeqId, maxSeqId) = await DetectGapAsync(start, token);

        // Detect "leading gap": no inter-event gap, but first event in range > start
        var hasLeadingGap = gapSeqId == null && minSeqId.HasValue && minSeqId.Value > start;

        if (gapSeqId.HasValue || hasLeadingGap)
        {
            // Check if the gap is stale enough to skip
            if (stats.TryGetStaleAge(out var timeSinceUpdate) &&
                timeSinceUpdate > _daemonSettings.StaleSequenceThreshold)
            {
                _logger.LogWarning(
                    "Skipping stale gap starting after seq_id {CurrentMark}. High water was last updated {TimeSinceUpdate} ago",
                    stats.CurrentMark, timeSinceUpdate);

                // Move past the gap to the max available
                stats.CurrentMark = maxSeqId ?? stats.CurrentMark;
                stats.IncludesSkipping = true;
            }
            else if (gapSeqId.HasValue)
            {
                // The gap starts AFTER gapSeqId, so everything up to gapSeqId is contiguous
                stats.CurrentMark = gapSeqId.Value;
            }
            // If only a leading gap exists and it's not stale, don't advance
        }
        else if (maxSeqId.HasValue)
        {
            stats.CurrentMark = maxSeqId.Value;
        }

        if (stats.HasChanged)
        {
            await MarkHighWaterAsync(stats.CurrentMark, token);
        }

        return stats;
    }

    internal async Task<HighWaterStatistics> LoadStatisticsAsync(CancellationToken token)
    {
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                SELECT ISNULL(MAX(seq_id), 0) FROM {events.EventsTableName};
                SELECT last_seq_id, last_updated FROM {events.ProgressionTableName}
                    WHERE name = 'HighWaterMark';
                """;

            var stats = new HighWaterStatistics();

            await using var reader = await cmd.ExecuteReaderAsync(ct);

            // First result: highest sequence
            if (await reader.ReadAsync(ct))
            {
                stats.HighestSequence = reader.GetInt64(0);
            }

            // Second result: current mark from progression
            if (await reader.NextResultAsync(ct) && await reader.ReadAsync(ct))
            {
                stats.LastMark = reader.GetInt64(0);
                stats.CurrentMark = stats.LastMark;
                stats.SafeStartMark = stats.LastMark;
                stats.LastUpdated = reader.GetDateTimeOffset(1);
            }

            stats.Timestamp = DateTimeOffset.UtcNow;

            return stats;
        }, (_connectionString, _events), token);
    }

    internal async Task<(long? GapSeqId, long? MinSeqId, long? MaxSeqId)> DetectGapAsync(long start,
        CancellationToken token)
    {
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, startSeq) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                SELECT TOP 1 seq_id FROM (
                    SELECT seq_id, LEAD(seq_id) OVER (ORDER BY seq_id) AS next_seq
                    FROM {events.EventsTableName} WHERE seq_id >= @start
                ) ct WHERE next_seq IS NOT NULL AND next_seq - seq_id > 1;

                SELECT MIN(seq_id) FROM {events.EventsTableName} WHERE seq_id >= @start;

                SELECT MAX(seq_id) FROM {events.EventsTableName} WHERE seq_id >= @start;
                """;

            cmd.Parameters.AddWithValue("@start", startSeq);

            long? gapSeqId = null;
            long? minSeqId = null;
            long? maxSeqId = null;

            await using var reader = await cmd.ExecuteReaderAsync(ct);

            // First result: gap detection between consecutive events
            if (await reader.ReadAsync(ct) && !reader.IsDBNull(0))
            {
                gapSeqId = reader.GetInt64(0);
            }

            // Second result: min seq_id in range
            if (await reader.NextResultAsync(ct) && await reader.ReadAsync(ct) && !reader.IsDBNull(0))
            {
                minSeqId = reader.GetInt64(0);
            }

            // Third result: max seq_id in range
            if (await reader.NextResultAsync(ct) && await reader.ReadAsync(ct) && !reader.IsDBNull(0))
            {
                maxSeqId = reader.GetInt64(0);
            }

            return (gapSeqId, minSeqId, maxSeqId);
        }, (_connectionString, _events, start), token);
    }

    internal async Task MarkHighWaterAsync(long mark, CancellationToken token)
    {
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, markValue) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                MERGE {events.ProgressionTableName} AS target
                USING (SELECT 'HighWaterMark' AS name) AS source ON target.name = source.name
                WHEN MATCHED THEN UPDATE SET last_seq_id = @mark, last_updated = SYSDATETIMEOFFSET()
                WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                    VALUES ('HighWaterMark', @mark, SYSDATETIMEOFFSET());
                """;

            cmd.Parameters.AddWithValue("@mark", markValue);
            await cmd.ExecuteNonQueryAsync(ct);
        }, (_connectionString, _events, mark), token);
    }

    // ── #163 Phase 2: vectorized per-tenant high-water ──────────────────────

    /// <summary>
    ///     Opt the running daemon into per-tenant high-water + per-tenant rebuilds when the store uses
    ///     per-tenant event sequencing. Off (default) keeps the single store-global mark, byte-for-byte.
    /// </summary>
    public bool SupportsTenantPartitioning => _events.UseTenantPartitionedEvents;

    public async Task<HighWaterVector> DetectForTenantsAsync(
        IReadOnlyCollection<string> tenantIds, CancellationToken token)
    {
        if (!_events.UseTenantPartitionedEvents)
        {
            return HighWaterVector.ForGlobal(await Detect(token));
        }

        if (tenantIds.Count == 0) return new HighWaterVector([]);

        return new HighWaterVector(await LoadPerTenantStatisticsAsync(tenantIds, token));
    }

    public async Task<HighWaterVector> DetectInSafeZoneForTenantsAsync(
        IReadOnlyCollection<string> tenantIds, CancellationToken token)
    {
        if (!_events.UseTenantPartitionedEvents)
        {
            return HighWaterVector.ForGlobal(await DetectInSafeZone(token));
        }

        if (tenantIds.Count == 0) return new HighWaterVector([]);

        // The vectorized poll is the same shape as the normal one; the daemon-level safe-zone gap
        // skipping is layered on top by the base VectorizedHighWaterMonitor — the store's job is one
        // round-trip per poll regardless of mode. Mirrors Marten's per-tenant detector.
        return new HighWaterVector(await LoadPerTenantStatisticsAsync(tenantIds, token));
    }

    /// <summary>
    ///     One round-trip vectorized per-tenant high-water read: for each requested tenant, join
    ///     pc_tenant_partitions → sys.sequences (that tenant's pc_events_sequence_{partition_id} current
    ///     value) → pc_event_progression (the per-tenant high-water row, keyed "HighWaterMark:{tenant}").
    ///     LEFT JOINs keep a row per input tenant even before its partition/sequence/progression exist
    ///     (resolved to 0). The SQL Server analogue of Marten's pg_sequences join.
    /// </summary>
    private async Task<IReadOnlyList<HighWaterStatistics>> LoadPerTenantStatisticsAsync(
        IReadOnlyCollection<string> tenantIds, CancellationToken token)
    {
        var schema = _events.DatabaseSchemaName;
        var tenantsJson = JsonSerializer.Serialize(tenantIds);
        var highWaterPrefix = ShardState.HighWaterMark + ":";

        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, schema, tenantsJson, prefix) = state;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                WITH inputs AS (SELECT value AS tenant_id FROM OPENJSON(@tenants))
                SELECT
                    i.tenant_id,
                    CAST(ISNULL(sq.current_value, 0) AS bigint) AS last_value,
                    ISNULL(prog.last_seq_id, 0)                 AS last_seq_id,
                    prog.last_updated
                FROM inputs i
                LEFT JOIN {events.TenantPartitionsTableName} p
                    ON p.tenant_id = i.tenant_id
                LEFT JOIN sys.sequences sq
                    ON sq.name = 'pc_events_sequence_' + CAST(p.partition_id AS varchar(20))
                    AND sq.schema_id = SCHEMA_ID(@schema)
                LEFT JOIN {events.ProgressionTableName} prog
                    ON prog.name = @prefix + i.tenant_id;
                """;
            cmd.Parameters.AddWithValue("@tenants", tenantsJson);
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@prefix", prefix);

            var results = new List<HighWaterStatistics>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var tenantId = reader.GetString(0);
                var lastValue = reader.GetInt64(1);
                var lastSeqId = reader.GetInt64(2);
                DateTimeOffset? lastUpdated = reader.IsDBNull(3) ? null : reader.GetDateTimeOffset(3);

                // Seed the mark from the saved per-tenant high-water row when present, else from the
                // tenant's sequence value (highest issued). Per-tenant gap detection is a later
                // refinement, matching Marten's current Phase 2 behavior.
                var currentMark = lastSeqId > 0 ? lastSeqId : lastValue;

                results.Add(new HighWaterStatistics
                {
                    TenantId = tenantId,
                    HighestSequence = lastValue,
                    LastMark = lastSeqId,
                    SafeStartMark = currentMark,
                    CurrentMark = currentMark,
                    LastUpdated = lastUpdated,
                    Timestamp = DateTimeOffset.UtcNow
                });
            }

            return (IReadOnlyList<HighWaterStatistics>)results;
        }, (_connectionString, _events, schema, tenantsJson, highWaterPrefix), token);
    }
}
