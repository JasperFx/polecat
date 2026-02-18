using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Polecat.Events.Daemon;

/// <summary>
///     SQL Server implementation of IHighWaterDetector.
///     Detects the highest contiguous seq_id in pc_events and manages
///     the high water mark in pc_event_progression.
/// </summary>
internal class PolecatHighWaterDetector : IHighWaterDetector
{
    private readonly EventGraph _events;
    private readonly string _connectionString;
    private readonly DaemonSettings _daemonSettings;
    private readonly ILogger<PolecatHighWaterDetector> _logger;

    public PolecatHighWaterDetector(EventGraph events, string connectionString,
        DaemonSettings daemonSettings, ILogger<PolecatHighWaterDetector> logger)
    {
        _events = events;
        _connectionString = connectionString;
        _daemonSettings = daemonSettings;
        _logger = logger;

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
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT ISNULL(MAX(seq_id), 0) FROM {_events.EventsTableName};
            SELECT last_seq_id, last_updated FROM {_events.ProgressionTableName}
                WHERE name = 'HighWaterMark';
            """;

        var stats = new HighWaterStatistics();

        await using var reader = await cmd.ExecuteReaderAsync(token);

        // First result: highest sequence
        if (await reader.ReadAsync(token))
        {
            stats.HighestSequence = reader.GetInt64(0);
        }

        // Second result: current mark from progression
        if (await reader.NextResultAsync(token) && await reader.ReadAsync(token))
        {
            stats.LastMark = reader.GetInt64(0);
            stats.CurrentMark = stats.LastMark;
            stats.SafeStartMark = stats.LastMark;
            stats.LastUpdated = reader.GetDateTimeOffset(1);
        }

        stats.Timestamp = DateTimeOffset.UtcNow;

        return stats;
    }

    internal async Task<(long? GapSeqId, long? MinSeqId, long? MaxSeqId)> DetectGapAsync(long start,
        CancellationToken token)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT TOP 1 seq_id FROM (
                SELECT seq_id, LEAD(seq_id) OVER (ORDER BY seq_id) AS next_seq
                FROM {_events.EventsTableName} WHERE seq_id >= @start
            ) ct WHERE next_seq IS NOT NULL AND next_seq - seq_id > 1;

            SELECT MIN(seq_id) FROM {_events.EventsTableName} WHERE seq_id >= @start;

            SELECT MAX(seq_id) FROM {_events.EventsTableName} WHERE seq_id >= @start;
            """;

        cmd.Parameters.AddWithValue("@start", start);

        long? gapSeqId = null;
        long? minSeqId = null;
        long? maxSeqId = null;

        await using var reader = await cmd.ExecuteReaderAsync(token);

        // First result: gap detection between consecutive events
        if (await reader.ReadAsync(token) && !reader.IsDBNull(0))
        {
            gapSeqId = reader.GetInt64(0);
        }

        // Second result: min seq_id in range
        if (await reader.NextResultAsync(token) && await reader.ReadAsync(token) && !reader.IsDBNull(0))
        {
            minSeqId = reader.GetInt64(0);
        }

        // Third result: max seq_id in range
        if (await reader.NextResultAsync(token) && await reader.ReadAsync(token) && !reader.IsDBNull(0))
        {
            maxSeqId = reader.GetInt64(0);
        }

        return (gapSeqId, minSeqId, maxSeqId);
    }

    internal async Task MarkHighWaterAsync(long mark, CancellationToken token)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            MERGE {_events.ProgressionTableName} AS target
            USING (SELECT 'HighWaterMark' AS name) AS source ON target.name = source.name
            WHEN MATCHED THEN UPDATE SET last_seq_id = @mark, last_updated = SYSDATETIMEOFFSET()
            WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                VALUES ('HighWaterMark', @mark, SYSDATETIMEOFFSET());
            """;

        cmd.Parameters.AddWithValue("@mark", mark);
        await cmd.ExecuteNonQueryAsync(token);
    }
}
