using System.Buffers;
using System.Data;
using System.Text;
using System.Text.Json;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Polecat.Events.Schema;
using Polecat.Internal;
using Polecat.Storage;

namespace Polecat.Events.Daemon;

/// <summary>
///     #1054 (marten#6007): a skip of the high water mark is provisional for
///     <see cref="Projections.PolecatProjectionOptions.LateEventDetectionWindow" />. Every skip records the sequence
///     numbers inside its range that had no committed event at the time (<c>pc_high_water_skips.gaps</c>). This
///     checks those numbers on the high water poll, at most once per
///     <see cref="Projections.PolecatProjectionOptions.LateEventProbeInterval" />, and for each event that has since
///     committed in one of them:
///     <list type="bullet">
///         <item>records it in <c>pc_high_water_late_events</c> (exactly once, whichever node finds it first),</item>
///         <item>counts it in the <c>polecat.daemon.late_events</c> metric and logs a warning, or an error when the
///         range was skipped by proof that nothing could fill it, because that is a bug in high water detection,</item>
///         <item>and rebuilds the event's stream in every async single-stream projection that handles it, through the
///         per-identity rebuild of the shard running in this process (#1060).</item>
///     </list>
///     Other projections and subscriptions are not repaired: they cannot safely take an event out of order.
/// </summary>
internal sealed class LateEventRecovery
{
    private const int PendingRepairBatchSize = 500;

    // Locked reads of committed rows only, never waiting for a writer, as the detector reads them (#833). An
    // append still in flight inside a gap is not late yet.
    private const string CommittedRows = "WITH (READCOMMITTEDLOCK, READPAST, ROWLOCK)";

    private readonly DocumentStore _store;
    private readonly PolecatDatabase _database;
    private readonly EventGraph _events;
    private readonly ILogger _logger;
    private readonly object _repairLock = new();

    private long _nextProbeTicks;
    private Task _repairs = Task.CompletedTask;

    public LateEventRecovery(DocumentStore store, PolecatDatabase database, ILogger logger)
    {
        _store = store;
        _database = database;
        _events = store.Events;
        _logger = logger;
    }

    /// <summary>The repair pass started by the last probe, for tests.</summary>
    internal Task Repairs
    {
        get
        {
            lock (_repairLock) return _repairs;
        }
    }

    /// <summary>
    ///     Called on every high water poll. Costs a clock read unless a probe is due, and the probe itself is three
    ///     statements against two tables that are empty on a store that never skipped.
    /// </summary>
    public async Task ProbeIfDueAsync(CancellationToken token)
    {
        if (!_events.EnableLateEventDetection) return;

        var now = DateTimeOffset.UtcNow.UtcTicks;
        var due = Interlocked.Read(ref _nextProbeTicks);
        if (now < due) return;

        var next = now + _events.LateEventProbeInterval.Ticks;
        if (Interlocked.CompareExchange(ref _nextProbeTicks, next, due) != due) return;

        try
        {
            await ProbeAsync(token).ConfigureAwait(false);
        }
        catch (SqlException e) when (e.Number is 1205 or 2601 or 2627 && !token.IsCancellationRequested)
        {
            // Another node's probe got to the same late event first. The next probe sees its row.
            _logger.LogDebug(e, "Checking the skipped high water ranges of database {Database} lost a race with another node",
                _database.Identifier);
        }
        catch (Exception e) when (!token.IsCancellationRequested)
        {
            // Never fail a high water poll over this
            _logger.LogError(e, "Unable to check the skipped high water ranges of database {Database} for late events",
                _database.Identifier);
        }
    }

    internal async Task ProbeAsync(CancellationToken token)
    {
        var skips = _events.HighWaterSkipsTableName;
        var ledger = _events.HighWaterLateEventsTableName;

        // Under per-tenant partitioned events every tenant's partition draws its own sequence and only per-tenant
        // marks move projections, so a store-global skip says nothing about any one tenant's events. A tenant's
        // skip covers its partition, which tenants sharing an ordinal share.
        var tenantMatch = _events.UseTenantPartitionedEvents
            ? $"s.tenant_id <> '{HighWaterSkipsTable.GlobalTenant}' AND e.{_events.TenantPartitionManager.Column} = (SELECT p.ordinal FROM {_events.TenantPartitionsTableName} p WHERE p.tenant_id = s.tenant_id)"
            : $"(s.tenant_id = '{HighWaterSkipsTable.GlobalTenant}' OR e.tenant_id = s.tenant_id)";

        var streamId = _events.StreamIdentity == StreamIdentity.AsGuid
            ? "LOWER(CONVERT(varchar(36), e.stream_id))"
            : "e.stream_id";

        // 1. Record every event that has committed inside a recorded gap of an open skip. The MERGE's key is what
        //    makes each late event reported once, by whichever node finds it first.
        // 2. Close the skips that are past the window, after the last look in (1). A skip without gaps had nothing
        //    missing, so there is nothing to look at.
        // 3. The late events that one of this store's async single-stream projections has not taken in yet. Each
        //    store tracks its own projections, so two applications sharing the schema do not settle each other's.
        var sql = $"""
            MERGE {ledger} WITH (UPDLOCK, HOLDLOCK) AS target
            USING (
                SELECT f.seq_id, f.tenant_id, f.stream_id, f.event_type, f.starting_sequence, f.ending_sequence, f.reason
                FROM (
                    SELECT e.seq_id, e.tenant_id, {streamId} AS stream_id, e.type AS event_type,
                           s.starting_sequence, s.ending_sequence, s.reason,
                           ROW_NUMBER() OVER (PARTITION BY e.seq_id, e.tenant_id ORDER BY s.ending_sequence) AS n
                    FROM {skips} s
                    CROSS APPLY OPENJSON(s.gaps) WITH (lo bigint '$[0]', hi bigint '$[1]') g
                    JOIN {_events.EventsTableName} e {CommittedRows}
                      ON e.seq_id >= g.lo AND e.seq_id <= g.hi AND {tenantMatch}
                    WHERE s.closed_at IS NULL AND s.gaps IS NOT NULL
                ) f
                WHERE f.n = 1
            ) AS source
            ON target.seq_id = source.seq_id AND target.tenant_id = source.tenant_id
            WHEN NOT MATCHED THEN
                INSERT (seq_id, tenant_id, stream_id, event_type, skip_starting_sequence, skip_ending_sequence, skip_reason)
                VALUES (source.seq_id, source.tenant_id, source.stream_id, source.event_type, source.starting_sequence,
                        source.ending_sequence, source.reason)
            OUTPUT inserted.seq_id, inserted.tenant_id, inserted.stream_id, inserted.event_type,
                   inserted.skip_starting_sequence, inserted.skip_ending_sequence, inserted.skip_reason;

            UPDATE {skips} SET closed_at = SYSDATETIMEOFFSET()
            WHERE closed_at IS NULL
              AND (gaps IS NULL OR gaps = '[]'
                   OR [timestamp] < DATEADD(millisecond, -@window_ms, DATEADD(second, -@window_seconds, SYSDATETIMEOFFSET())));

            SELECT TOP ({PendingRepairBatchSize}) l.seq_id, l.tenant_id, l.stream_id, l.event_type, l.repaired_projections
            FROM {ledger} l
            WHERE EXISTS (
                SELECT 1 FROM OPENJSON(@repairs) WITH (name nvarchar(200) '$') r
                WHERE r.name NOT IN (
                    SELECT x.name FROM OPENJSON(ISNULL(l.repaired_projections, N'[]')) WITH (name nvarchar(200) '$') x))
            ORDER BY l.seq_id;
            """;

        var late = new List<LateEvent>();
        var pending = new List<PendingRepair>();
        var repairable = repairableProjectionNames();
        var window = _events.LateEventDetectionWindow;

        await using (var conn = new SqlConnection(_database.ConnectionString))
        {
            await conn.OpenAsync(token).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.Add("@repairs", SqlDbType.NVarChar, -1).Value = writeJsonArray(repairable);
            cmd.Parameters.Add("@window_seconds", SqlDbType.Int).Value =
                (int)Math.Min(int.MaxValue, Math.Floor(window.TotalSeconds));
            cmd.Parameters.Add("@window_ms", SqlDbType.Int).Value = window.Milliseconds;

            await using var reader = await cmd.ExecuteReaderAsync(token).ConfigureAwait(false);

            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                late.Add(new LateEvent(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetInt64(4),
                    reader.GetInt64(5),
                    await reader.IsDBNullAsync(6, token).ConfigureAwait(false) ? null : reader.GetString(6)));
            }

            // The update has no result set
            await reader.NextResultAsync(token).ConfigureAwait(false);

            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                pending.Add(new PendingRepair(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    await reader.IsDBNullAsync(4, token).ConfigureAwait(false)
                        ? []
                        : readJsonArray(reader.GetString(4))));
            }
        }

        // What this store still owes each event
        foreach (var repair in pending)
        {
            repair.Owed = repairable.Except(repair.Repaired, StringComparer.Ordinal).ToArray();
        }

        foreach (var @event in late)
        {
            report(@event);
        }

        if (pending.Count > 0)
        {
            startRepairs(pending, token);
        }
    }

    private void report(LateEvent late)
    {
        var reason = late.Reason ?? "Unknown";
        _events.OpenTelemetry.RecordLateEvent(late.EventType, late.TenantId, reason);

        if (reason == nameof(HighWaterSkipReason.Proven))
        {
            _logger.LogError(
                "Event {Sequence} ({EventType}) of stream {StreamId} for tenant {TenantId} on database {Database} committed inside the sequence range ({Starting}, {Ending}] after the high water mark skipped it, but that range was skipped because no append that could fill it was in flight. This is a bug in Polecat's high water detection, please report it. Async projections and subscriptions that were already past the event did not see it",
                late.Sequence, late.EventType, late.StreamId, late.TenantId, _database.Identifier, late.Starting,
                late.Ending);
        }
        else
        {
            _logger.LogWarning(
                "Event {Sequence} ({EventType}) of stream {StreamId} for tenant {TenantId} on database {Database} committed inside the sequence range ({Starting}, {Ending}] after the high water mark skipped it ({Reason}). Async projections and subscriptions that were already past the event did not see it. Async single stream projections rebuild the stream; other projections and subscriptions are not repaired",
                late.Sequence, late.EventType, late.StreamId, late.TenantId, _database.Identifier, late.Starting,
                late.Ending, reason);
        }
    }

    // Every async single-stream projection of this store owes each late event a repair, until it rebuilds the
    // event's stream or finds that it does not handle the event's type
    private string[] repairableProjectionNames()
    {
        return _store.Options.Projections.All
            .Where(x => x is IAggregateProjection
            {
                Scope: AggregationScope.SingleStream, Lifecycle: ProjectionLifecycle.Async
            })
            .Select(x => x.Name)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
    }

    private void startRepairs(IReadOnlyList<PendingRepair> pending, CancellationToken token)
    {
        lock (_repairLock)
        {
            // One pass at a time. A pass waits on shard batches, so it can outlive a probe interval.
            if (!_repairs.IsCompleted) return;

            _repairs = Task.Run(() => repairAsync(pending, token), CancellationToken.None);
        }
    }

    private async Task repairAsync(IReadOnlyList<PendingRepair> pending, CancellationToken token)
    {
        var done = new List<(long Sequence, string TenantId, string Projection)>();

        var groups = pending
            .SelectMany(p => p.Owed.Select(name => (Projection: name, Event: p)))
            .GroupBy(x => (x.Projection, x.Event.TenantId, x.Event.StreamId));

        foreach (var group in groups)
        {
            if (token.IsCancellationRequested) break;

            var (name, tenantId, streamId) = group.Key;
            var events = group.Select(x => x.Event).ToList();

            if (!_store.Options.Projections.TryFindProjection(name, out var source)
                || source is not IAggregateProjection
                {
                    Scope: AggregationScope.SingleStream, Lifecycle: ProjectionLifecycle.Async
                } aggregate)
            {
                continue;
            }

            var relevant = events.Where(x => handles(aggregate, x.EventType)).ToList();
            done.AddRange(events.Except(relevant).Select(x => (x.Sequence, x.TenantId, name)));
            if (relevant.Count == 0) continue;

            // Only the node running the shard can rebuild inside it. Leave it for that node's probe otherwise.
            var daemon = daemonRunning(source, tenantId);
            if (daemon == null)
            {
                _logger.LogDebug(
                    "No daemon in this process runs projection {Projection} on database {Database}, so the late event(s) {Sequences} of stream {StreamId} are left for the node that does",
                    name, _database.Identifier, relevant.Select(x => x.Sequence).ToArray(), streamId);
                continue;
            }

            _logger.LogDebug("Rebuilding stream {StreamId} for tenant {TenantId} in projection {Projection} for the late event(s) {Sequences}",
                streamId, tenantId, name, relevant.Select(x => x.Sequence).ToArray());

            var identity = DocumentStore.DocumentIdentityFor(aggregate.IdentityType, streamIdFor(streamId));

            // marten#6003 (decision 12): a parked identity's replay or discard folds the late events in. Until then
            // they are owed, and a rebuild here would only fail on the event the identity was parked for.
            if (daemon.IsIdentityParked(name, tenantId, identity))
            {
                _logger.LogDebug(
                    "Stream {StreamId} for tenant {TenantId} is parked in projection {Projection}, so its late event(s) {Sequences} wait for its replay",
                    streamId, tenantId, name, relevant.Select(x => x.Sequence).ToArray());
                continue;
            }

            try
            {
                var result = await daemon.RebuildIdentityAsync(name, tenantId, identity, token).ConfigureAwait(false);

                // The re-fold covers the stream up to the batch it was bound to. An event above that is not in
                // the document yet, and the shard may not reach it either, so it stays owed.
                var repaired = relevant.Where(x => x.Sequence <= result.BoundCeiling).ToList();
                done.AddRange(repaired.Select(x => (x.Sequence, x.TenantId, name)));

                if (repaired.Count > 0)
                {
                    _logger.LogInformation(
                        "Rebuilt stream {StreamId} for tenant {TenantId} in projection {Projection} to take in the late event(s) {Sequences}",
                        streamId, tenantId, name, repaired.Select(x => x.Sequence).ToArray());
                }
            }
            catch (IdentityRebuildRefusedException e) when (e.Reason is IdentityRebuildRefusal.NotASingleStreamProjection
                                                                or IdentityRebuildRefusal.NotSupportedByStore)
            {
                _logger.LogWarning(e,
                    "Projection {Projection} cannot rebuild stream {StreamId} for tenant {TenantId} to take in the late event(s) {Sequences}. Rebuild the projection to include them",
                    name, streamId, tenantId, relevant.Select(x => x.Sequence).ToArray());
                done.AddRange(relevant.Select(x => (x.Sequence, x.TenantId, name)));
            }
            catch (IdentityRebuildRefusedException e)
            {
                // Stopped, paused, being rebuilt or never committed: try again on a later probe
                _logger.LogDebug(e, "Rebuilding stream {StreamId} of projection {Projection} for a late event was refused",
                    streamId, name);
            }
            catch (Exception e) when (!token.IsCancellationRequested)
            {
                _logger.LogError(e,
                    "Unable to rebuild stream {StreamId} for tenant {TenantId} in projection {Projection} to take in the late event(s) {Sequences}. Will try again",
                    streamId, tenantId, name, relevant.Select(x => x.Sequence).ToArray());
            }
        }

        if (done.Count == 0) return;

        try
        {
            await markRepairedAsync(done, token).ConfigureAwait(false);
        }
        catch (Exception e) when (!token.IsCancellationRequested)
        {
            // The rebuilds are idempotent, so losing this only repeats them on the next probe
            _logger.LogError(e, "Unable to record the late event repairs on database {Database}", _database.Identifier);
        }
    }

    private async Task markRepairedAsync(IReadOnlyList<(long Sequence, string TenantId, string Projection)> done,
        CancellationToken token)
    {
        await using var conn = new SqlConnection(_database.ConnectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);

        // repaired_projections is nvarchar(max), never the native json type, so JSON_MODIFY is safe here (#907)
        foreach (var (sequence, tenantId, projection) in done)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                UPDATE {_events.HighWaterLateEventsTableName}
                SET repaired_projections = JSON_MODIFY(ISNULL(repaired_projections, N'[]'), 'append $', @projection)
                WHERE seq_id = @seq AND tenant_id = @tenant
                  AND NOT EXISTS (
                      SELECT 1 FROM OPENJSON(ISNULL(repaired_projections, N'[]')) WITH (name nvarchar(200) '$') x
                      WHERE x.name = @projection);
                """;
            cmd.Parameters.Add("@projection", SqlDbType.NVarChar, 200).Value = projection;
            cmd.Parameters.Add("@seq", SqlDbType.BigInt).Value = sequence;
            cmd.Parameters.AddKey("@tenant", tenantId, _events.UnicodeKeys);
            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }
    }

    private PolecatProjectionDaemon? daemonRunning(IProjectionSource<IDocumentSession, IQuerySession> source,
        string tenantId)
    {
        var shards = source.Shards().Select(x => x.Name).ToList();
        if (_events.UseTenantPartitionedEvents)
        {
            shards.AddRange(shards.Select(x => x.ForTenant(tenantId)).ToArray());
        }

        return _store.DaemonsInThisProcess().FirstOrDefault(daemon =>
            string.Equals(daemon.Database.Identifier, _database.Identifier, StringComparison.OrdinalIgnoreCase)
            && shards.Any(x => daemon.StatusFor(x.Identity) == AgentStatus.Running));
    }

    // An event type this store cannot resolve is treated as handled: rebuilding a stream is never wrong, only wasted
    private bool handles(IAggregateProjection aggregate, string eventTypeName)
    {
        var mapping = _events.AllKnownEventTypes().FirstOrDefault(x => x.EventTypeName == eventTypeName);
        if (mapping == null || aggregate.AllEventTypes.Length == 0) return true;

        return aggregate.AllEventTypes.Any(x => x.IsAssignableFrom(mapping.EventType));
    }

    private object streamIdFor(string streamId)
    {
        return _events.StreamIdentity == StreamIdentity.AsGuid ? Guid.Parse(streamId) : streamId;
    }

    // By hand rather than through JsonSerializer, so this stays trim- and AOT-clean
    private static string writeJsonArray(IEnumerable<string> values)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var value in values) writer.WriteStringValue(value);
            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static string[] readJsonArray(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.EnumerateArray().Select(x => x.GetString()!).ToArray();
    }

    private sealed record LateEvent(long Sequence, string TenantId, string StreamId, string EventType, long Starting,
        long Ending, string? Reason);

    private sealed record PendingRepair(long Sequence, string TenantId, string StreamId, string EventType,
        string[] Repaired)
    {
        public string[] Owed { get; set; } = [];
    }
}
