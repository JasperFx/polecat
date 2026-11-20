using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using Polecat.Events.Schema;
using Polecat.Internal;

namespace Polecat.Events.Protected;

/// <summary>
///     #1099 (marten#6114): one open skip of the high water mark with recorded gaps that hold some of the events a
///     delete is about to remove.
/// </summary>
/// <param name="Starting">The skipped range is (<paramref name="Starting" />, <paramref name="Ending" />].</param>
/// <param name="Ending">The high water mark the skip moved to.</param>
/// <param name="TenantId">The tenant whose own mark skipped, or the empty string for the store-global mark.</param>
/// <param name="Reason">The <see cref="HighWaterSkipReason" />, null for a skip recorded without one.</param>
/// <param name="SkippedAt">When the skip was recorded.</param>
/// <param name="Sequences">The events about to be deleted that sit in the skip's gaps.</param>
internal sealed record OpenHighWaterSkip(long Starting, long Ending, string TenantId, string? Reason,
    DateTimeOffset SkippedAt, long[] Sequences);

/// <summary>
///     #1099 (marten#6114): the open skips of the high water mark that a delete of events must wait for. Used by
///     stream compaction, and meant for the cold move (#1051) too, so the two apply one rule.
/// </summary>
/// <remarks>
///     <para>
///         <see cref="AsyncShardProgressGuard" /> takes "the shard's <c>last_seq_id</c> is at or past the highest
///         deleted <c>seq_id</c>" as proof that the shard processed the events. After a high water skip (#1054) that
///         is not true: an event that commits late inside a skipped range has a <c>seq_id</c> the shards already moved
///         past. <see cref="Daemon.LateEventRecovery" /> finds such an event by checking the skip's recorded gaps until
///         it closes the skip, <see cref="Projections.PolecatProjectionOptions.LateEventDetectionWindow" /> after it
///         was recorded. A delete that removes the event first leaves recovery nothing to find.
///     </para>
///     <para>
///         The rule is recovery's own: a skip is open while <c>closed_at</c> is null, and an event is at risk when
///         its <c>seq_id</c> is inside one of the skip's recorded gaps, for the same tenant match recovery uses. An
///         event outside the gaps had already committed when the range was skipped, so the shards read it. Nothing
///         is checked without <see cref="EventStoreOptions.EnableLateEventDetection" />: the tables do not exist then,
///         and nothing would ever close a skip.
///     </para>
/// </remarks>
internal static class OpenHighWaterSkips
{
    /// <summary>
    ///     The open skips with a recorded gap holding one of <paramref name="sequences" />, for the tenant of
    ///     <paramref name="events" />, read through its session. Empty when late event detection is off.
    /// </summary>
    internal static async Task<IReadOnlyList<OpenHighWaterSkip>> FindHoldingAsync(EventOperations events,
        IReadOnlyCollection<long> sequences, CancellationToken token)
    {
        var graph = events.Graph;
        if (!graph.EnableLateEventDetection || sequences.Count == 0) return [];

        // The tenant match is LateEventRecovery's: under per-tenant partitioned events every tenant's partition draws
        // its own sequence and only that partition's own skips apply, which tenants sharing an ordinal share.
        // Otherwise a store-global skip covers every tenant.
        var partitions = graph.TenantPartitionsTableName;
        var tenantMatch = graph.UseTenantPartitionedEvents
            ? $"s.tenant_id <> '{HighWaterSkipsTable.GlobalTenant}' AND (SELECT p.ordinal FROM {partitions} p WHERE p.tenant_id = s.tenant_id) = (SELECT p.ordinal FROM {partitions} p WHERE p.tenant_id = @tenant)"
            : $"(s.tenant_id = '{HighWaterSkipsTable.GlobalTenant}' OR s.tenant_id = @tenant)";

        await using var cmd = new SqlCommand();
        cmd.CommandText = $"""
            SELECT DISTINCT s.starting_sequence, s.ending_sequence, s.tenant_id, s.reason, s.[timestamp], d.seq
            FROM {graph.HighWaterSkipsTableName} s
            CROSS APPLY OPENJSON(s.gaps) WITH (lo bigint '$[0]', hi bigint '$[1]') g
            JOIN OPENJSON(@sequences) WITH (seq bigint '$') d ON d.seq >= g.lo AND d.seq <= g.hi
            WHERE s.closed_at IS NULL AND s.gaps IS NOT NULL AND {tenantMatch}
            ORDER BY s.ending_sequence, d.seq;
            """;
        cmd.Parameters.Add("@sequences", SqlDbType.NVarChar, -1).Value = "[" + string.Join(",", sequences) + "]";
        cmd.Parameters.AddKey("@tenant", events.TenantId, graph.UnicodeKeys);

        var rows = new List<(long Starting, long Ending, string TenantId, string? Reason, DateTimeOffset SkippedAt, long Sequence)>();
        await using (var reader = await events.Session.ExecuteReaderAsync(cmd, token).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                rows.Add((
                    reader.GetInt64(0),
                    reader.GetInt64(1),
                    reader.GetString(2),
                    await reader.IsDBNullAsync(3, token).ConfigureAwait(false) ? null : reader.GetString(3),
                    await reader.GetFieldValueAsync<DateTimeOffset>(4, token).ConfigureAwait(false),
                    reader.GetInt64(5)));
            }
        }

        return rows
            .GroupBy(x => (x.Starting, x.Ending, x.TenantId, x.Reason, x.SkippedAt))
            .Select(g => new OpenHighWaterSkip(g.Key.Starting, g.Key.Ending, g.Key.TenantId, g.Key.Reason,
                g.Key.SkippedAt, g.Select(x => x.Sequence).Distinct().Order().ToArray()))
            .ToArray();
    }

    /// <summary>
    ///     Why a delete has to wait for <paramref name="skips" />, in plain words: which events, which skip and its
    ///     range, and when recovery will close it.
    /// </summary>
    internal static string Describe(IReadOnlyList<OpenHighWaterSkip> skips, EventGraph events)
    {
        var window = events.LateEventDetectionWindow;
        var text = new StringBuilder();

        foreach (var skip in skips)
        {
            if (text.Length > 0) text.Append(" Also, ");

            var mark = skip.TenantId == HighWaterSkipsTable.GlobalTenant
                ? "the high water mark"
                : $"tenant '{skip.TenantId}''s high water mark";
            var reason = skip.Reason == null ? "" : $" ({skip.Reason})";

            text.Append($"event(s) {string.Join(", ", skip.Sequences)} sit in a gap of the sequence range ")
                .Append($"({skip.Starting}, {skip.Ending}] that {mark} skipped at {skip.SkippedAt.UtcDateTime:u}{reason}, ")
                .Append("so they may have committed after the async projections and subscriptions moved past them, and ")
                .Append("late event recovery has not finished checking that range. Late event recovery in the async daemon ")
                .Append($"closes that skip on its first check after {(skip.SkippedAt + window).UtcDateTime:u} ")
                .Append($"(LateEventDetectionWindow is {window}).");
        }

        return text.ToString();
    }
}
