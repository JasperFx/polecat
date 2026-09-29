using System.Text.Json;
using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Polly;

using Polecat.Internal;
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
            // WITH (UPDLOCK, HOLDLOCK) is load-bearing, not decoration — #500. A bare MERGE takes no
            // lasting lock over the key it probed, so two detectors racing on a cold start (no
            // HighWaterMark row yet) both match nothing and both take the INSERT branch: "Violation
            // of PRIMARY KEY constraint 'pkey_pc_event_progression_name'".
            // Why both hints rather than HOLDLOCK alone. HOLDLOCK makes the probe hold its range
            // lock to end of statement, which is what closes the probe/probe/insert/insert window.
            // UPDLOCK additionally makes that a RangeS-U rather than a RangeS-S: two RangeS-U locks
            // are mutually incompatible, so the loser blocks at the probe and re-reads into the
            // MATCHED branch, instead of both sides holding a shared lock and then needing to
            // convert it to an insert lock the other side blocks — the documented lock-conversion
            // deadlock (1205) that a bare HOLDLOCK upsert is prone to.
            // Measured, so the comment does not overstate it: against Bug_500's reproducer the bare
            // MERGE fails with 2627 every run, and BOTH hinted forms pass — the conversion deadlock
            // did not reproduce here. UPDLOCK is kept because it forecloses that failure mode at no
            // cost on a row this cold (one write per detection cycle), not because it was observed
            // to be load-bearing. Do not "simplify" it away on the grounds that HOLDLOCK passes.
            // The document upserts in SqlServerDocumentStorageDescriptorBuilder carry HOLDLOCK alone
            // and are deliberately left alone: they contend on distinct document ids, where two
            // sessions rarely probe the SAME key. Every agent here contends on the one literal
            // 'HighWaterMark' row, so this is the maximal-contention case.
            // JasperFx 2.56.0 (jasperfx#709) collapses N concurrent agent starts in ONE process into
            // a single priming Detect(), which is what produced the reported trace — but a second
            // process still races, so the guard has to be here, server-side.
            cmd.CommandText = $"""
                MERGE {events.ProgressionTableName} WITH (UPDLOCK, HOLDLOCK) AS target
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
    ///     The <c>pc_event_progression.name</c> of a tenant's high-water row. ONE definition, because
    ///     the read and the write have to agree: #697 was in part a reader keyed on this prefix with no
    ///     writer producing it, and a silent disagreement here reads exactly like "this tenant has never
    ///     progressed".
    /// </summary>
    internal static string PerTenantHighWaterName(string tenantId) => PerTenantHighWaterPrefix + tenantId;

    /// <summary>
    ///     The prefix alone, for the SQL that builds the name by concatenating a column value.
    /// </summary>
    internal const string PerTenantHighWaterPrefix = ShardState.HighWaterMark + ":";

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

        // Same reading as the normal poll. The store-global pair differs in how a gap is resolved;
        // per tenant the two share one rule, because only the non-safe-zone entry point is ever called
        // and confining the stale-gap escape to this one would make it unreachable — see
        // AdvancePerTenantMark's remarks.
        return new HighWaterVector(await LoadPerTenantStatisticsAsync(tenantIds, token));
    }

    /// <summary>
    ///     #697 — persist a tenant's high-water row so its mark survives a daemon restart. Invoked by
    ///     JasperFx's <c>TenantedHighWaterCoordinator</c> on each vectorized poll, which is why the
    ///     store-global <see cref="MarkHighWaterAsync" /> has no per-tenant caller: the coordinator owns
    ///     the write, the store owns only the statement.
    /// </summary>
    /// <remarks>
    ///     The base interface declares this as a default member returning <c>Task.CompletedTask</c>, so
    ///     an unimplemented store does not fail — it silently never persists, which is exactly the
    ///     failure #697 reported. This override is what makes the reader's
    ///     <c>HighWaterMark:{tenant}</c> join find anything.
    ///     <para>
    ///         The timestamp overload is the one implemented (jasperfx#449) because it is the one the
    ///         coordinator calls; implementing only the three-argument form would persist the row but
    ///         stamp it with the server clock instead of the poll's own reading, losing per-tenant
    ///         staleness.
    ///     </para>
    /// </remarks>
    public async Task MarkHighWaterForTenantAsync(string tenantId, long sequence, DateTimeOffset timestamp,
        CancellationToken token)
    {
        await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, name, markValue, stamp) = state;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            // WITH (UPDLOCK, HOLDLOCK) for the same reason as the store-global MarkHighWaterAsync above
            // — see the long note there. The contention is if anything sharper here: every node polls
            // the same tenant set on the same cadence, so two detectors probing a tenant's row before
            // it exists is the ordinary cold start rather than a rare race.
            cmd.CommandText = $"""
                MERGE {events.ProgressionTableName} WITH (UPDLOCK, HOLDLOCK) AS target
                USING (SELECT @name AS name) AS source ON target.name = source.name
                WHEN MATCHED THEN UPDATE SET last_seq_id = @mark, last_updated = @stamp
                WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                    VALUES (@name, @mark, @stamp);
                """;

            cmd.Parameters.AddVarChar("@name", name);
            cmd.Parameters.AddWithValue("@mark", markValue);
            cmd.Parameters.AddWithValue("@stamp", stamp);
            await cmd.ExecuteNonQueryAsync(ct);
        }, (_connectionString, _events, PerTenantHighWaterName(tenantId), sequence, timestamp), token);
    }

    /// <summary>
    ///     One round-trip vectorized per-tenant high-water read. For each requested tenant it resolves
    ///     the tenant's partition ordinal, then reads from <c>pc_events</c> itself:
    ///     the committed height (<c>MAX(seq_id)</c> within that tenant's partition), the first
    ///     contiguity gap above the persisted mark, and the lowest committed seq_id above it — plus the
    ///     persisted <c>HighWaterMark:{tenant}</c> row. LEFT JOINs keep a row per input tenant even
    ///     before its partition or progression row exists.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>The height comes from <c>MAX(seq_id)</c>, never from the tenant's sequence.</b> This
    ///         reader used to join <c>sys.sequences</c> and take <c>current_value</c>, which is wrong in
    ///         both directions and was half of #697. A sequence reports the last value it HANDED OUT,
    ///         which runs ahead of what is committed while an append is in flight or after one rolls
    ///         back; and a tenant whose events arrived through the shared sequence or were renumbered by
    ///         a bulk insert leaves its own sequence untouched, where SQL Server reports
    ///         <c>current_value = 1</c> — the START value, not 0 (measured). That second case is the
    ///         damaging one: the reader returned <c>HighestSequence = CurrentMark = 1</c> for a fully
    ///         populated tenant, which every caller reads as "caught up", so its events never project.
    ///         Marten hit the identical defect and fixed it the same way (marten#4712).
    ///     </para>
    ///     <para>
    ///         <b>The gap walk is not optional.</b> Without it the mark advances straight to the
    ///         committed height, so an append still in flight below that height is stepped over and
    ///         never projected; and once a persisted row exists, a reader that seeded the mark from it
    ///         and stopped would freeze at that value forever (marten#4867). The walk is predicated on
    ///         <c>seq_id &gt; mark</c>, so a caught-up tenant touches no rows.
    ///     </para>
    ///     <para>
    ///         Both APPLYs key on <c>tenant_ordinal</c> rather than <c>tenant_id</c>: it is the physical
    ///         partition column and leads the clustered index, so each tenant's read is partition-
    ///         eliminated instead of scanning every tenant's events. Note the deliberate asymmetry in the
    ///         join — the registry table spells the column <c>ordinal</c> and only <c>pc_events</c>
    ///         spells it <c>tenant_ordinal</c>, so <c>p.ordinal</c> is correct and not a typo.
    ///     </para>
    /// </remarks>
    private async Task<IReadOnlyList<HighWaterStatistics>> LoadPerTenantStatisticsAsync(
        IReadOnlyCollection<string> tenantIds, CancellationToken token)
    {
        var tenantsJson = JsonSerializer.Serialize(tenantIds);
        var staleThreshold = _daemonSettings.StaleSequenceThreshold;
        var logger = _logger;

        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, tenantsJson, staleThreshold, logger) = state;

            var ordinal = events.TenantPartitionManager.Column;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                WITH inputs AS (SELECT CAST(value AS varchar(250)) AS tenant_id FROM OPENJSON(@tenants))
                SELECT
                    i.tenant_id,
                    ISNULL(prog.last_seq_id, 0) AS last_seq_id,
                    prog.last_updated,
                    ISNULL(hi.max_seq_id, 0)    AS max_seq_id,
                    lo.min_above,
                    walk.gap_edge
                FROM inputs i
                LEFT JOIN {events.TenantPartitionsTableName} p
                    ON p.tenant_id = i.tenant_id
                LEFT JOIN {events.ProgressionTableName} prog
                    ON prog.name = @prefix + i.tenant_id
                OUTER APPLY (
                    SELECT MAX(e.seq_id) AS max_seq_id
                    FROM {events.EventsTableName} e
                    WHERE e.{ordinal} = p.ordinal
                ) hi
                OUTER APPLY (
                    SELECT TOP 1 e.seq_id AS min_above
                    FROM {events.EventsTableName} e
                    WHERE e.{ordinal} = p.ordinal AND e.seq_id > ISNULL(prog.last_seq_id, 0)
                    ORDER BY e.seq_id
                ) lo
                OUTER APPLY (
                    SELECT TOP 1 g.seq_id AS gap_edge
                    FROM (
                        SELECT e.seq_id, LEAD(e.seq_id) OVER (ORDER BY e.seq_id) AS next_seq
                        FROM {events.EventsTableName} e
                        WHERE e.{ordinal} = p.ordinal AND e.seq_id > ISNULL(prog.last_seq_id, 0)
                    ) g
                    WHERE g.next_seq IS NOT NULL AND g.next_seq - g.seq_id > 1
                    ORDER BY g.seq_id
                ) walk;
                """;
            cmd.Parameters.AddWithValue("@tenants", tenantsJson);
            cmd.Parameters.AddVarChar("@prefix", PerTenantHighWaterPrefix);

            var results = new List<HighWaterStatistics>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var tenantId = reader.GetString(0);
                var lastSeqId = reader.GetInt64(1);
                DateTimeOffset? lastUpdated = reader.IsDBNull(2) ? null : reader.GetDateTimeOffset(2);
                var maxSeqId = reader.GetInt64(3);
                long? minAbove = reader.IsDBNull(4) ? null : reader.GetInt64(4);
                long? gapEdge = reader.IsDBNull(5) ? null : reader.GetInt64(5);

                var stats = new HighWaterStatistics
                {
                    TenantId = tenantId,
                    HighestSequence = maxSeqId,
                    LastMark = lastSeqId,
                    SafeStartMark = lastSeqId,
                    CurrentMark = lastSeqId,
                    LastUpdated = lastUpdated,
                    Timestamp = DateTimeOffset.UtcNow
                };

                AdvancePerTenantMark(stats, minAbove, gapEdge, staleThreshold, logger);

                results.Add(stats);
            }

            return (IReadOnlyList<HighWaterStatistics>)results;
        }, (_connectionString, _events, tenantsJson, staleThreshold, logger), token);
    }

    /// <summary>
    ///     The per-tenant mark rule: how far a tenant's high water may advance given its committed
    ///     events. Modelled on the store-global <see cref="Detect" /> / <see cref="DetectInSafeZone" />
    ///     pair, but deliberately NOT a copy of either, for a reason worth stating.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>Both entry points share this one rule, including the stale-gap escape that only the
    ///         store-global SAFE-ZONE path has.</b> That is not carelessness about the distinction:
    ///         <c>VectorizedHighWaterMonitor.PollSafeZoneAsync</c> has no caller anywhere in JasperFx
    ///         today, so <see cref="DetectInSafeZoneForTenantsAsync" /> never runs and
    ///         <see cref="DetectForTenantsAsync" /> is the only live path. A gap-respecting rule with
    ///         the escape confined to the dead path would hold a tenant's mark behind a gap that never
    ///         fills — a rolled-back append — with nothing able to release it. That trades #697's
    ///         "never advances" for a subtler permanent stall, which is not a fix.
    ///     </para>
    ///     <para>
    ///         <b>A tenant with no persisted mark cannot have a leading gap.</b> Before the first mark
    ///         is written there is no floor to protect, so committed events starting above seq_id 1 —
    ///         which is simply where that tenant's numbering began, or what a failed first append left
    ///         behind — are not a hole to wait on. Treating them as one strands such a tenant at zero
    ///         permanently, because staleness is measured from <c>last_updated</c> and a tenant that has
    ///         never persisted a mark has no <c>last_updated</c> to age.
    ///     </para>
    /// </remarks>
    private static void AdvancePerTenantMark(HighWaterStatistics stats, long? minAbove, long? gapEdge,
        TimeSpan staleThreshold, ILogger logger)
    {
        // Caught up: the persisted mark already equals the committed height.
        if (stats.CurrentMark == stats.HighestSequence)
        {
            return;
        }

        var start = stats.CurrentMark + 1;
        var hasPersistedMark = stats.LastMark > 0;

        // A leading gap is "nothing committed at `start`, but something committed above it" — an append
        // in flight, or one that rolled back, at the bottom of the range. It only means anything
        // relative to a mark we actually persisted; see the remarks.
        var hasLeadingGap = hasPersistedMark && gapEdge == null && minAbove.HasValue && minAbove.Value > start;

        if (gapEdge.HasValue || hasLeadingGap)
        {
            if (stats.TryGetStaleAge(out var timeSinceUpdate) && timeSinceUpdate > staleThreshold)
            {
                logger.LogWarning(
                    "Tenant {TenantId}: skipping stale gap starting after seq_id {CurrentMark}. Its high water was last updated {TimeSinceUpdate} ago",
                    stats.TenantId, stats.CurrentMark, timeSinceUpdate);

                stats.CurrentMark = stats.HighestSequence;
                stats.IncludesSkipping = true;
                return;
            }

            if (gapEdge.HasValue)
            {
                // The gap opens AFTER gapEdge, so everything up to gapEdge is contiguous.
                stats.CurrentMark = gapEdge.Value;
            }

            // A leading gap that is not yet stale: hold and let the append land.
            return;
        }

        stats.CurrentMark = stats.HighestSequence;
    }
}
