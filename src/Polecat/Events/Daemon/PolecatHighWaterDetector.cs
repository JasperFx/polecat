using Weasel.SqlServer;
using JasperFx.Descriptors;
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
/// <remarks>
///     <para>
///         <b>#833: every read of pc_events here names its own locking, on purpose.</b> With no hint the
///         detector's behaviour was decided by a database setting. <c>READ_COMMITTED_SNAPSHOT</c> off made
///         the read block on an uncommitted row, which is what the design was silently relying on. On, which
///         is the Azure SQL Database default, an in-flight append was simply a hole, and a hole directly above
///         the mark was invisible, so the mark stepped over it and the event was never projected.
///     </para>
///     <para>
///         <see cref="CommittedRows" /> and <see cref="PhysicalRows" /> together answer the question the old
///         read could not: is a hole dead, or is an append still filling it? See
///         <see cref="findSkipTargetAsync" />.
///     </para>
/// </remarks>
internal class PolecatHighWaterDetector : IHighWaterDetector
{
    /// <summary>
    ///     #833: committed rows only, never waiting for a writer, on every database setting.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>Why not <c>READCOMMITTEDLOCK</c> alone</b>, which would restore the blocking the design
    ///         relied on. It is the simplest correct read, but it waits behind every open append for as long
    ///         as that append stays open, on either database setting. A cycle stuck in that wait does not
    ///         re-stamp the mark's liveness (#705), and at the command timeout it throws, out of
    ///         <c>CheckNowAsync</c> and so out of <c>CatchUpAsync</c>. <c>READPAST</c> gets the same
    ///         independence from the database setting without any of that: a locked row is left out, so it
    ///         reads as a hole, which holds the mark instead of stopping the detector.
    ///     </para>
    ///     <para>
    ///         <c>READCOMMITTEDLOCK</c> stays beside <c>READPAST</c> because Microsoft documents the pair as
    ///         the form to use under <c>READ_COMMITTED_SNAPSHOT</c>, and a read this load-bearing should say
    ///         outright that it is a locking read. <c>ROWLOCK</c> because a scan that takes page locks would
    ///         skip a whole page for one locked row; that only delays the mark, but there is no reason to pay
    ///         it. Measured on SQL Server 2025: the same rows with <c>READ_COMMITTED_SNAPSHOT</c> on and off, and
    ///         under optimized locking, where the writer holds only its transaction lock and no row lock.
    ///     </para>
    ///     <para>
    ///         What <c>READPAST</c> cannot step past is a TABLE lock. Polecat's own appends write one
    ///         single-row INSERT per event, which stays well under the escalation threshold, but a foreign
    ///         bulk load holding <c>TABLOCK</c> would block this read until the command timeout.
    ///     </para>
    /// </remarks>
    private const string CommittedRows = "WITH (READCOMMITTEDLOCK, READPAST, ROWLOCK)";

    /// <summary>
    ///     #833: every row physically present, committed or not. Only ever compared with
    ///     <see cref="CommittedRows" /> to find an append in flight, and never the source of a mark: a mark
    ///     read from here could pass an append that then rolls back. See <see cref="findSkipTargetAsync" />.
    /// </summary>
    private const string PhysicalRows = "WITH (READUNCOMMITTED)";

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

        DatabaseUri = BuildDatabaseUri(connectionString);
    }

    /// <summary>
    ///     The detector's database identity, built through JasperFx's
    ///     <see cref="DatabaseDescriptor.DatabaseUri" /> rather than by hand.
    /// </summary>
    /// <remarks>
    ///     This used to compose the URI itself — <c>sqlserver://{DataSource}/{InitialCatalog}</c>, with
    ///     a comma-to-colon swap for the "host,port" form — and that threw
    ///     <see cref="UriFormatException" /> out of this CONSTRUCTOR for server names SQL Server
    ///     deployments use constantly: a named instance <c>db-host\MSSQL2017</c>, <c>.\SQLEXPRESS</c>,
    ///     <c>(localdb)\MSSQLLocalDB</c>, or a <c>tcp:</c> prefix with a port. Measured, all four.
    ///     Throwing here takes the async daemon down at startup, not at first use.
    ///     <para>
    ///         The fix is to delegate rather than to add the missing characters. jasperfx#918 is the
    ///         same bug upstream and records that the equivalent method had already been patched three
    ///         times, once per character class somebody hit in production, and that 27 of the 95
    ///         printable ASCII characters still threw; a fourth patch here would have been the same
    ///         mistake a fourth time. JasperFx 2.76.1 sanitizes to the alphabet a host actually permits,
    ///         so borrowing it means this never needs a fifth.
    ///     </para>
    /// </remarks>
    private static Uri BuildDatabaseUri(string connectionString)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);

        return new DatabaseDescriptor
        {
            Engine = SqlServerProvider.EngineName,
            ServerName = builder.DataSource ?? string.Empty,
            DatabaseName = builder.InitialCatalog ?? string.Empty
        }.DatabaseUri();
    }

    public Uri DatabaseUri { get; }

    // #791: when THIS detector first saw the store-global mark held at its current position by a gap.
    // The stale-gap skip in DetectInSafeZone ages the gap from here. It used to age it from the
    // HighWaterMark row's last_updated, but #705 re-stamps that column on every detection cycle (it is
    // liveness, not progress), so on a running daemon the gap never looked older than one polling
    // interval and an inter-event hole left by a rolled-back append was never skipped — the mark
    // crawled, or sat, under it. Marten measures the same age the same way (its _stuckGap), and for the
    // same reason it never trusts last_updated for it: on a store nobody was polling, that column is
    // arbitrarily old, and a gap that has only just opened would be skipped the moment a daemon starts.
    // Detector-scoped and in memory, so a restart only means one fresh threshold before a skip.
    //
    // #833: Ceiling is the highest seq_id known to exist when the mark was first seen held. Everything up
    // to it had been handed out by then, so by the time the threshold has passed, a value at or below it
    // with no row behind it is dead rather than merely unwritten. A skip goes no higher than this: a value
    // allocated a moment ago can have no row YET, and that hole is not evidence of anything.
    private sealed record HeldMark(long Mark, DateTimeOffset Since, long Ceiling);

    private HeldMark? _heldMark;

    private HeldMark observeHeldMark(HighWaterStatistics stats, long mark, long lowestAbove)
    {
        var held = _heldMark;
        if (held == null || held.Mark != mark)
        {
            // HighestSequence was read before the walk that found this gap, so it can be behind it. Without
            // lowestAbove the ceiling could sit at the mark itself and the gap could never be skipped.
            held = new HeldMark(mark, stats.Timestamp, Math.Max(stats.HighestSequence, lowestAbove));
            _heldMark = held;
        }

        return held;
    }

    public async Task<HighWaterStatistics> Detect(CancellationToken token)
    {
        var stats = await LoadStatisticsAsync(token);

        if (stats.CurrentMark == stats.HighestSequence)
        {
            // #705: THE idle path, and the one that matters for liveness. Caught up is exactly the
            // state a monitor cannot otherwise distinguish from abandoned, so the row is re-stamped
            // here rather than returned untouched. Everything below this point has, by definition,
            // found work to do.
            _heldMark = null;
            await MarkHighWaterAsync(stats.CurrentMark, token);
            return stats;
        }

        // Starts (or continues) the clock of any gap it stops at, so that by the time the agent has waited
        // out its own StaleSequenceThreshold and calls DetectInSafeZone, the gap is already that old.
        await advanceToGapAsync(stats, token);

        // #705 / jasperfx#924 — UNCONDITIONAL, where this used to be `if (stats.HasChanged)`.
        // ShardState.LastUpdated is liveness, not progress: a monitor has to be able to tell "caught
        // up, no new events" from "no longer maintained", and a row only rewritten when the mark moves
        // says nothing on an idle store. So every completed detection cycle re-stamps last_updated,
        // and the mark it writes is simply the one it just computed — unchanged when nothing arrived.
        //
        // The cost is one small single-row MERGE per detection cycle on an otherwise idle store, at
        // DaemonSettings.SlowPollingTime (1s by default). That is the price of the field meaning
        // anything at all; a store that skips the write has a LastUpdated that ages exactly like a
        // dead one.
        await MarkHighWaterAsync(stats.CurrentMark, token);

        return stats;
    }

    public async Task<HighWaterStatistics> DetectInSafeZone(CancellationToken token)
    {
        var stats = await LoadStatisticsAsync(token);

        if (stats.CurrentMark == stats.HighestSequence)
        {
            // #705: THE idle path, and the one that matters for liveness. Caught up is exactly the
            // state a monitor cannot otherwise distinguish from abandoned, so the row is re-stamped
            // here rather than returned untouched. Everything below this point has, by definition,
            // found work to do.
            _heldMark = null;
            await MarkHighWaterAsync(stats.CurrentMark, token);
            return stats;
        }

        var held = await advanceToGapAsync(stats, token);

        // #791: aged from when this detector first saw the mark held here, never from last_updated. See
        // _heldMark.
        if (held != null && stats.Timestamp - held.Since > _daemonSettings.StaleSequenceThreshold)
        {
            var target = await FindSkipTargetAsync(held.Mark, held.Ceiling, token);
            if (target.HasValue)
            {
                _logger.LogWarning(
                    "Skipping stale gap starting after seq_id {HeldAt}. The high water mark has been held there since {Since}, with no append in flight below {Target}",
                    held.Mark, held.Since, target.Value);

                stats.CurrentMark = target.Value;
                stats.IncludesSkipping = true;
            }
            else
            {
                // #833: not stuck, waiting. This is the state the read used to block in with
                // READ_COMMITTED_SNAPSHOT off; HighWaterAgent's stall watchdog warns if it goes on.
                _logger.LogInformation(
                    "The high water mark is held after seq_id {HeldAt} by an append that is still in flight",
                    held.Mark);
            }

            // Either way this observation is finished with, so the next one takes a fresh ceiling and the
            // check above runs once per threshold rather than on every cycle of a long-held mark.
            _heldMark = null;
        }

        // #705 / jasperfx#924 — UNCONDITIONAL, where this used to be `if (stats.HasChanged)`.
        // ShardState.LastUpdated is liveness, not progress: a monitor has to be able to tell "caught
        // up, no new events" from "no longer maintained", and a row only rewritten when the mark moves
        // says nothing on an idle store. So every completed detection cycle re-stamps last_updated,
        // and the mark it writes is simply the one it just computed — unchanged when nothing arrived.
        //
        // The cost is one small single-row MERGE per detection cycle on an otherwise idle store, at
        // DaemonSettings.SlowPollingTime (1s by default). That is the price of the field meaning
        // anything at all; a store that skips the write has a LastUpdated that ages exactly like a
        // dead one.
        await MarkHighWaterAsync(stats.CurrentMark, token);

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
                SELECT ISNULL(MAX(seq_id), 0) FROM {events.EventsTableName} {CommittedRows};
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

    /// <summary>
    ///     Move <paramref name="stats" /> as far as it can go without crossing a hole, and report the gap
    ///     that stopped it, if one did.
    /// </summary>
    /// <remarks>
    ///     <b>A leading gap is checked first, and it is the case that lost events (#833).</b> The walk compares
    ///     each row with the next, so a hole directly above the mark has no row below it to be compared with,
    ///     and the rows above it look contiguous. It used to fall through to <c>MAX(seq_id)</c>. Marten fixed the
    ///     same shape as marten#4964, with <c>HoldBeforeLeadingGap</c>.
    /// </remarks>
    private async Task<HeldMark?> advanceToGapAsync(HighWaterStatistics stats, CancellationToken token)
    {
        var mark = stats.CurrentMark;
        var run = await ReadContiguousRunAsync(mark, token);

        if (await hasLeadingGapAsync(stats, run, token))
        {
            return observeHeldMark(stats, mark, run.First!.Value);
        }

        if (run.Next.HasValue)
        {
            // The gap starts AFTER run.End, so everything up to it is contiguous.
            stats.CurrentMark = run.End!.Value;
            return observeHeldMark(stats, run.End.Value, run.Next.Value);
        }

        if (run.End.HasValue)
        {
            stats.CurrentMark = run.End.Value;
        }

        _heldMark = null;
        return null;
    }

    /// <summary>
    ///     Is there a hole between the mark and the lowest committed row above it?
    /// </summary>
    /// <remarks>
    ///     A store with no mark yet has no floor to protect. Its events may simply start above 1, after a failed
    ///     first append or once the table has been emptied, because IDENTITY is not reset by either. The hole
    ///     only counts there if an append is actually in it, which is what keeps the very first appends on a
    ///     new store safe. Same rule, and same reasoning, as <see cref="advancePerTenantMarkAsync" />.
    /// </remarks>
    private async Task<bool> hasLeadingGapAsync(HighWaterStatistics stats, ContiguousRun run,
        CancellationToken token)
    {
        if (run.First is not { } first || first <= stats.CurrentMark + 1) return false;

        return stats.LastMark > 0 || await FindSkipTargetAsync(stats.CurrentMark, first, token) != first;
    }

    /// <summary>
    ///     The first contiguous run of committed rows matching <paramref name="predicate" />: where it starts
    ///     (<see cref="ContiguousRun.First" />), where it ends (<see cref="ContiguousRun.End" />), and the
    ///     next committed row after it, if a hole follows (<see cref="ContiguousRun.Next" />).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>One scan, not one statement.</b> This replaces three separate readings (first gap, MIN,
    ///         MAX) which could disagree. A single statement is a single snapshot only on row versions; with
    ///         <see cref="CommittedRows" /> each table reference in a statement is a separate walk of the
    ///         table at a separate moment. Then a walk that finished at 5 and a <c>MAX</c> that ran after it
    ///         and saw 9 would move the mark to 9 over whatever is in flight at 6. All three values here come
    ///         from the same rows of the same scan, so they cannot disagree, and the mark never moves on the
    ///         strength of a height.
    ///     </para>
    ///     <para>
    ///         <c>TOP (1)</c> rather than an aggregate because it stops at the first hole. A held mark can
    ///         have a long way above it. Measured on 300,000 rows with a gap near the mark: 397 logical reads
    ///         against 26,407 for the same three values as <c>MIN</c>/<c>MAX</c>.
    ///     </para>
    /// </remarks>
    private static string contiguousRunSql(EventGraph events, string predicate) => $"""
        SELECT TOP (1) r.first_seq, r.seq_id AS run_end, r.next_seq
        FROM (
            SELECT e.seq_id,
                MIN(e.seq_id) OVER (ORDER BY e.seq_id ROWS UNBOUNDED PRECEDING) AS first_seq,
                LEAD(e.seq_id) OVER (ORDER BY e.seq_id) AS next_seq
            FROM {events.EventsTableName} e {CommittedRows}
            WHERE {predicate}
        ) r
        WHERE r.next_seq IS NULL OR r.next_seq - r.seq_id > 1
        ORDER BY r.seq_id
        """;

    internal async Task<ContiguousRun> ReadContiguousRunAsync(long mark, CancellationToken token)
    {
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, markValue) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = contiguousRunSql(events, "e.seq_id > @mark") + ";";
            cmd.Parameters.AddWithValue("@mark", markValue);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) return default;

            return new ContiguousRun(reader.GetInt64(0), reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2));
        }, (_connectionString, _events, mark), token);
    }

    internal async Task<long?> FindSkipTargetAsync(long mark, long ceiling, CancellationToken token)
    {
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, markValue, ceilingValue) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            return await findSkipTargetAsync(conn, events, null, markValue, ceilingValue, ct);
        }, (_connectionString, _events, mark, ceiling), token);
    }

    /// <summary>
    ///     #833: how far the mark may safely skip from <paramref name="mark" />. The highest committed seq_id no
    ///     higher than <paramref name="ceiling" /> and below every append still in flight, or null if there
    ///     is none, which means the mark has to stay where it is.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>This is what makes skipping safe.</b> Holding at a gap is the easy half; a gap left by a rollback
    ///         is permanent here, because IDENTITY and sequence values are burned and Polecat writes no
    ///         tombstones, so the mark has to get past it. But a hole that is merely old is not necessarily
    ///         dead, and the default threshold is only three seconds: long enough for a large
    ///         <c>SaveChangesAsync</c> or a transaction the application holds open. Age alone would hand exactly
    ///         the same data loss back, three seconds later.
    ///     </para>
    ///     <para>
    ///         An append in flight is a row that <see cref="PhysicalRows" /> can see and
    ///         <see cref="CommittedRows" /> cannot. Its row exists from the moment its INSERT runs, inside
    ///         the statement that allocates the seq_id, so the only hole this cannot see into is a value
    ///         allocated and not yet inserted. The ceiling closes that for the store-global mark. A row can
    ///         also read as in flight because a transaction is updating or deleting it (archiving, masking,
    ///         compaction), which holds the mark a little longer and is harmless.
    ///     </para>
    ///     <para>
    ///         Deliberately not one scan, unlike <see cref="contiguousRunSql" />, and it does not need to be: the
    ///         two views can only disagree about a row that changed state between them, and each direction of
    ///         that is safe. A row committed in between reads as committed; a row rolled back reads as gone.
    ///     </para>
    /// </remarks>
    private static async Task<long?> findSkipTargetAsync(SqlConnection conn, EventGraph events, int? ordinal,
        long mark, long ceiling, CancellationToken ct)
    {
        string inScope(string alias) =>
            ordinal == null ? "" : $" AND {alias}.{events.TenantPartitionManager.Column} = @ordinal";

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT MAX(c.seq_id)
            FROM {events.EventsTableName} c {CommittedRows}
            WHERE c.seq_id > @mark AND c.seq_id <= @ceiling{inScope("c")}
              AND c.seq_id < ISNULL((
                  SELECT MIN(p.seq_id)
                  FROM {events.EventsTableName} p {PhysicalRows}
                  WHERE p.seq_id > @mark AND p.seq_id <= @ceiling{inScope("p")}
                    AND NOT EXISTS (
                        SELECT 1 FROM {events.EventsTableName} r {CommittedRows}
                        WHERE r.seq_id = p.seq_id{inScope("r")})
              ), 9223372036854775807);
            """;
        cmd.Parameters.AddWithValue("@mark", mark);
        cmd.Parameters.AddWithValue("@ceiling", ceiling);
        if (ordinal != null) cmd.Parameters.AddWithValue("@ordinal", ordinal.Value);

        var result = await cmd.ExecuteScalarAsync(ct);
        return result is long target ? target : null;
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
    ///     One round-trip vectorized per-tenant high-water read, plus one more per tenant that is held at a
    ///     gap and has gone stale. For each requested tenant it resolves the tenant's partition ordinal,
    ///     then reads from <c>pc_events</c> itself: the committed height (<c>MAX(seq_id)</c> within that
    ///     tenant's partition) and the first contiguous run above the persisted mark — plus the persisted
    ///     <c>HighWaterMark:{tenant}</c> row. LEFT JOINs keep a row per input tenant even before its
    ///     partition or progression row exists.
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
    ///         <b>#833: the height and the walk are two scans, so the height moves no mark.</b> Under
    ///         <see cref="CommittedRows" /> they run at different moments and may disagree, so the mark comes
    ///         from the walk alone. See <see cref="contiguousRunSql" />.
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
                    p.ordinal,
                    run.first_seq,
                    run.run_end,
                    run.next_seq
                FROM inputs i
                LEFT JOIN {events.TenantPartitionsTableName} p
                    ON p.tenant_id = i.tenant_id
                LEFT JOIN {events.ProgressionTableName} prog
                    ON prog.name = @prefix + i.tenant_id
                OUTER APPLY (
                    SELECT MAX(e.seq_id) AS max_seq_id
                    FROM {events.EventsTableName} e {CommittedRows}
                    WHERE e.{ordinal} = p.ordinal
                ) hi
                OUTER APPLY (
                    {contiguousRunSql(events, $"e.{ordinal} = p.ordinal AND e.seq_id > ISNULL(prog.last_seq_id, 0)")}
                ) run;
                """;
            cmd.Parameters.AddWithValue("@tenants", tenantsJson);
            cmd.Parameters.AddVarChar("@prefix", PerTenantHighWaterPrefix);

            var readings = new List<(HighWaterStatistics Stats, ContiguousRun Run, int? Partition)>();
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var tenantId = reader.GetString(0);
                    var lastSeqId = reader.GetInt64(1);
                    DateTimeOffset? lastUpdated = reader.IsDBNull(2) ? null : reader.GetDateTimeOffset(2);
                    var maxSeqId = reader.GetInt64(3);
                    int? partition = reader.IsDBNull(4) ? null : reader.GetInt32(4);
                    long? first = reader.IsDBNull(5) ? null : reader.GetInt64(5);
                    long? end = reader.IsDBNull(6) ? null : reader.GetInt64(6);
                    long? next = reader.IsDBNull(7) ? null : reader.GetInt64(7);

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

                    readings.Add((stats, new ContiguousRun(first, end, next), partition));
                }
            }

            // After the reader is closed: a connection string need not enable MARS, and a tenant held at a gap
            // has to go back to the database on this same connection.
            var results = new List<HighWaterStatistics>(readings.Count);
            foreach (var (stats, run, partition) in readings)
            {
                await advancePerTenantMarkAsync(conn, events, stats, run, partition, staleThreshold, logger, ct);
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
    ///         <b>A tenant with no persisted mark has a leading gap only if an append is in it.</b> Before
    ///         the first mark is written there is no floor to protect, so committed events starting above
    ///         seq_id 1 — which is simply where that tenant's numbering began, or what a failed first append
    ///         left behind — are not a hole to wait on. Treating them as one strands such a tenant at zero
    ///         permanently, because staleness is measured from <c>last_updated</c> and a tenant that has
    ///         never persisted a mark has no <c>last_updated</c> to age.
    ///     </para>
    ///     <para>
    ///         <b>#833: staleness is not enough to skip, and here it means less than it looks.</b>
    ///         <c>last_updated</c> is stamped only when a tenant's mark advances, so this "age" is the time
    ///         since the tenant last moved, not the age of the gap. A tenant idle for longer than the threshold
    ///         reads as stale on its first sight of a brand new gap. The store-global path stopped trusting that
    ///         column for this in #791, and the same change is wanted here, but
    ///         <c>a_gap_that_never_fills_is_skipped_once_the_tenant_goes_stale</c> pins the first-sight skip.
    ///         So <see cref="findSkipTargetAsync" /> is what protects a tenant: a stale reading skips only what
    ///         no in-flight row is in the way of. Unlike the store-global mark there is no observed ceiling to
    ///         bound it, so what is left is a value allocated and not yet inserted, the instant between the
    ///         allocation and the row, on the first poll that sees an idle tenant move.
    ///     </para>
    /// </remarks>
    private static async Task advancePerTenantMarkAsync(SqlConnection conn, EventGraph events,
        HighWaterStatistics stats, ContiguousRun run, int? partition, TimeSpan staleThreshold, ILogger logger,
        CancellationToken ct)
    {
        // Caught up: the persisted mark already equals the committed height.
        if (stats.CurrentMark == stats.HighestSequence)
        {
            return;
        }

        // A leading gap is "nothing committed at mark + 1, but something committed above it" — an append in
        // flight, or one that rolled back, at the bottom of the range. Relative to a mark we actually
        // persisted that is always a hole; see the remarks for one we did not.
        var mark = stats.CurrentMark;
        var hasLeadingGap = run.First is { } first && first > mark + 1 &&
                            (stats.LastMark > 0 ||
                             await findSkipTargetAsync(conn, events, partition, mark, first, ct) != first);

        if (!hasLeadingGap && run.Next == null)
        {
            // Nothing above the mark, but a mark above the height follows it down: a tenant removed and added
            // back restarts its sequence below its old row. The height is never what moves a mark UP.
            stats.CurrentMark = run.End ?? Math.Min(mark, stats.HighestSequence);
            return;
        }

        // The gap opens AFTER run.End, so everything up to it is contiguous. A leading gap stays at the mark.
        var heldAt = hasLeadingGap ? mark : run.End!.Value;
        stats.CurrentMark = heldAt;

        if (stats.TryGetStaleAge(out var timeSinceUpdate) && timeSinceUpdate > staleThreshold)
        {
            var above = hasLeadingGap ? run.First!.Value : run.Next!.Value;
            var target = await findSkipTargetAsync(conn, events, partition, heldAt,
                Math.Max(stats.HighestSequence, above), ct);

            if (target.HasValue)
            {
                logger.LogWarning(
                    "Tenant {TenantId}: skipping stale gap starting after seq_id {HeldAt}, with no append in flight below {Target}. Its high water was last updated {TimeSinceUpdate} ago",
                    stats.TenantId, heldAt, target.Value, timeSinceUpdate);

                stats.CurrentMark = target.Value;
                stats.IncludesSkipping = true;
            }
            else
            {
                logger.LogInformation(
                    "Tenant {TenantId}: the high water mark is held after seq_id {HeldAt} by an append that is still in flight",
                    stats.TenantId, heldAt);
            }
        }
    }

    // ── #786 / jasperfx#1031: operator-driven advance to the committed ceiling ──────────────

    /// <summary>
    ///     #786 / jasperfx#1031: preview advancing this database's marks to the committed ceiling. Reads only.
    ///     <paramref name="tenantIds" /> selects the marks: null for the database's store-global mark, otherwise
    ///     those tenants' own <c>HighWaterMark:{tenant}</c> marks (per-tenant partitioned events), where an
    ///     empty list means every tenant in the partition registry.
    /// </summary>
    internal Task<IReadOnlyList<HighWaterMarkPreview>> PreviewAdvanceAsync(string databaseIdentifier,
        IReadOnlyCollection<string>? tenantIds, CancellationToken token)
        => runPerMarkAsync(databaseIdentifier, tenantIds, token,
            static (conn, events, database, tenantId, ct) => previewMarkAsync(conn, events, database, tenantId, ct));

    /// <summary>
    ///     #786 / jasperfx#1031: move the selected marks to the highest COMMITTED seq_id. Each mark is read,
    ///     checked and written in one transaction holding <c>UPDLOCK, HOLDLOCK</c> on its progression row —
    ///     the #500 hints every running detector's write takes — so the compare-and-set against
    ///     <paramref name="expectedCurrentMark" /> cannot lose a race to a detector, and a mark never moves
    ///     backwards.
    /// </summary>
    /// <remarks>
    ///     <c>MAX(seq_id)</c>, never the IDENTITY's or a tenant sequence's current value: a handed-out value can
    ///     belong to an append still in flight, and marking past it skips that append's events for good once it
    ///     commits (marten#4953). Whatever is still in flight BELOW the committed ceiling is skipped anyway —
    ///     that is the operation, and why it is an explicit operator action.
    /// </remarks>
    internal Task<IReadOnlyList<HighWaterMarkAdvance>> AdvanceToCommittedCeilingAsync(string databaseIdentifier,
        IReadOnlyCollection<string>? tenantIds, long? expectedCurrentMark, CancellationToken token)
        => runPerMarkAsync(databaseIdentifier, tenantIds, token,
            (conn, events, database, tenantId, ct) =>
                advanceMarkAsync(conn, events, database, tenantId, expectedCurrentMark, ct));

    private Task<IReadOnlyList<T>> runPerMarkAsync<T>(string databaseIdentifier,
        IReadOnlyCollection<string>? tenantIds, CancellationToken token,
        Func<SqlConnection, EventGraph, string, string?, CancellationToken, Task<T>> perMark)
    {
        return _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, events, database, requested, perMark) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            // null = the store-global mark; otherwise one mark per tenant, all of them when none are named.
            IReadOnlyList<string?> marks = [null];
            if (requested != null)
            {
                var tenants = requested.ToList();
                if (tenants.Count == 0)
                {
                    await using var list = conn.CreateCommand();
                    list.CommandText =
                        $"SELECT tenant_id FROM {events.TenantPartitionsTableName} ORDER BY tenant_id;";
                    await using var reader = await list.ExecuteReaderAsync(ct);
                    while (await reader.ReadAsync(ct)) tenants.Add(reader.GetString(0));
                }

                marks = tenants.Cast<string?>().ToList();
            }

            var results = new List<T>(marks.Count);
            foreach (var tenantId in marks)
            {
                results.Add(await perMark(conn, events, database, tenantId, ct));
            }

            return (IReadOnlyList<T>)results;
        }, (_connectionString, _events, databaseIdentifier, tenantIds, perMark), token).AsTask();
    }

    /// <summary>
    ///     The events one mark covers: every event for the store-global mark, or one tenant's partition for a
    ///     per-tenant mark — keyed on the partition ordinal, which leads the clustered index, as the per-tenant
    ///     reader is.
    /// </summary>
    private static string eventsInScope(EventGraph events, string? tenantId) => tenantId == null
        ? $"FROM {events.EventsTableName} e"
        : $"""
            FROM {events.EventsTableName} e
            JOIN {events.TenantPartitionsTableName} p
                ON e.{events.TenantPartitionManager.Column} = p.ordinal AND p.tenant_id = @tenant
            """;

    private static string markName(string? tenantId)
        => tenantId == null ? ShardState.HighWaterMark : PerTenantHighWaterName(tenantId);

    private static async Task<HighWaterMarkPreview> previewMarkAsync(SqlConnection conn, EventGraph events,
        string databaseIdentifier, string? tenantId, CancellationToken ct)
    {
        var scope = eventsInScope(events, tenantId);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            DECLARE @current bigint = ISNULL(
                (SELECT last_seq_id FROM {events.ProgressionTableName} WHERE name = @name), 0);
            DECLARE @ceiling bigint = (SELECT ISNULL(MAX(e.seq_id), 0) {scope});
            SELECT @current, @ceiling,
                CASE WHEN @ceiling > @current
                    THEN (@ceiling - @current) - (SELECT COUNT_BIG(*) {scope} WHERE e.seq_id > @current AND e.seq_id <= @ceiling)
                    ELSE 0 END;
            """;
        cmd.Parameters.AddVarChar("@name", markName(tenantId));
        if (tenantId != null) cmd.Parameters.AddVarChar("@tenant", tenantId);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new HighWaterMarkPreview(databaseIdentifier, tenantId, reader.GetInt64(0), reader.GetInt64(1),
            reader.GetInt64(2));
    }

    private static async Task<HighWaterMarkAdvance> advanceMarkAsync(SqlConnection conn, EventGraph events,
        string databaseIdentifier, string? tenantId, long? expectedCurrentMark, CancellationToken ct)
    {
        var scope = eventsInScope(events, tenantId);

        await using var cmd = conn.CreateCommand();
        // XACT_ABORT so an error mid-batch rolls the transaction back rather than leaving it open on a pooled
        // connection. The UPDLOCK, HOLDLOCK read holds the progression row (or, before it exists, its key
        // range) until COMMIT, which is what makes check-then-write one step against a detector's MERGE.
        cmd.CommandText = $"""
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;

            DECLARE @current bigint = ISNULL(
                (SELECT last_seq_id FROM {events.ProgressionTableName} WITH (UPDLOCK, HOLDLOCK) WHERE name = @name), 0);
            DECLARE @ceiling bigint = (SELECT ISNULL(MAX(e.seq_id), 0) {scope});
            DECLARE @outcome int;

            IF @expected IS NOT NULL AND @expected <> @current
                SET @outcome = {(int)HighWaterAdvanceOutcome.RefusedStaleExpectation};
            ELSE IF @ceiling <= @current
                SET @outcome = {(int)HighWaterAdvanceOutcome.AlreadyAtCeiling};
            ELSE
            BEGIN
                MERGE {events.ProgressionTableName} WITH (UPDLOCK, HOLDLOCK) AS target
                USING (SELECT @name AS name) AS source ON target.name = source.name
                WHEN MATCHED THEN UPDATE SET last_seq_id = @ceiling, last_updated = SYSDATETIMEOFFSET()
                WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                    VALUES (@name, @ceiling, SYSDATETIMEOFFSET());
                SET @outcome = {(int)HighWaterAdvanceOutcome.Advanced};
            END

            DECLARE @new bigint = CASE WHEN @outcome = {(int)HighWaterAdvanceOutcome.Advanced} THEN @ceiling ELSE @current END;
            DECLARE @skipped bigint = CASE WHEN @new > @current
                THEN (@new - @current) - (SELECT COUNT_BIG(*) {scope} WHERE e.seq_id > @current AND e.seq_id <= @new)
                ELSE 0 END;

            COMMIT TRANSACTION;

            SELECT @outcome, @current, @new, @skipped;
            """;
        cmd.Parameters.AddVarChar("@name", markName(tenantId));
        if (tenantId != null) cmd.Parameters.AddVarChar("@tenant", tenantId);
        cmd.Parameters.Add(new SqlParameter("@expected", System.Data.SqlDbType.BigInt)
        {
            Value = (object?)expectedCurrentMark ?? DBNull.Value
        });

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new HighWaterMarkAdvance(databaseIdentifier, tenantId, (HighWaterAdvanceOutcome)reader.GetInt32(0),
            reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3));
    }
}

/// <summary>
///     #833: what one scan of the committed rows above a mark found. See
///     <see cref="PolecatHighWaterDetector" />'s <c>contiguousRunSql</c>.
/// </summary>
/// <param name="First">The lowest committed seq_id above the mark, or null if there is none.</param>
/// <param name="End">The last seq_id of the contiguous run that starts at <paramref name="First" />.</param>
/// <param name="Next">The next committed seq_id after a hole, or null if the run reaches the height.</param>
internal readonly record struct ContiguousRun(long? First, long? End, long? Next);
