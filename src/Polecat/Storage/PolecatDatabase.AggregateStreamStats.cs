using System.Data;
using System.Text;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Internal;

namespace Polecat.Storage;

// jasperfx#1282 (#1130, marten#6152): stream counts by aggregate type for monitoring
public partial class PolecatDatabase
{
    /// <summary>
    ///     jasperfx#1282 (#1130): this database's streams counted by aggregate type, read from <c>pc_streams</c> alone
    ///     in one round trip.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>One command, never one per aggregate type, never a read of <c>pc_events</c>.</b> A stream's
    ///         <c>version</c> already counts every event appended to it, so the grouped statement is a
    ///         <c>GROUP BY type</c> with a conditional count per length threshold, and the second statement in the
    ///         same batch ranks each type's streams with <c>ROW_NUMBER()</c>. The second is left out when
    ///         <see cref="AggregateStreamStatsRequest.LongestStreams" /> is zero.
    ///     </para>
    ///     <para>
    ///         Archived streams are left out unless <see cref="AggregateStreamStatsRequest.IncludeArchived" /> is
    ///         set. A tenant narrows the read only where streams carry one, which is conjoined tenancy (per-tenant
    ///         partitioning requires it), by the same rule as <see cref="FetchFirstEventTimestampsAfterAsync" />.
    ///         Under database-per-tenant each database answers for its own streams.
    ///     </para>
    ///     <para>
    ///         Untagged streams come back under a null alias, and an alias this deployment cannot resolve keeps
    ///         its row with a null type. Like the other monitoring reads, a database whose event storage has not
    ///         been created yet has no streams to report (#677).
    ///     </para>
    /// </remarks>
    public async Task<IReadOnlyList<AggregateStreamStats>> FetchAggregateStreamStatsAsync(
        AggregateStreamStatsRequest request, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var tenantId = _events.TenancyStyle == TenancyStyle.Conjoined ? request.TenantId : null;
        var thresholds = request.LengthThresholds;
        var sql = AggregateStreamStatsSql(_events, thresholds.Count, tenantId != null, request.IncludeArchived,
            request.LongestStreams > 0);

        // A dictionary key can't be null, so the untagged streams get a key no alias can be
        const string untagged = "\0";
        var stats = new Dictionary<string, (long Count, long Total, long Max, long[] Buckets)>();
        var longest = new Dictionary<string, List<AggregateStreamLength>>();

        try
        {
            await executeAsync(static async (state, ct) =>
            {
                var (connectionString, sql, events, request, tenantId, stats, longest) = state;
                var thresholds = request.LengthThresholds;

                // A retried attempt starts over
                stats.Clear();
                longest.Clear();

                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync(ct).ConfigureAwait(false);

                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                if (tenantId != null)
                {
                    // #363: tenant_id is varchar unless the store uses Unicode keys
                    cmd.Parameters.AddKey("@tenant_id", tenantId, events.UnicodeKeys);
                }

                for (var i = 0; i < thresholds.Count; i++)
                {
                    cmd.Parameters.Add(new SqlParameter($"@t{i}", SqlDbType.BigInt) { Value = thresholds[i] });
                }

                if (request.LongestStreams > 0)
                {
                    cmd.Parameters.Add(new SqlParameter("@longest", SqlDbType.BigInt)
                    {
                        Value = (long)request.LongestStreams
                    });
                }

                await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var counts = new long[thresholds.Count];
                    for (var i = 0; i < counts.Length; i++)
                    {
                        counts[i] = reader.GetInt64(4 + i);
                    }

                    stats[reader.IsDBNull(0) ? untagged : reader.GetString(0)] =
                        (reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), counts);
                }

                if (request.LongestStreams > 0)
                {
                    await reader.NextResultAsync(ct).ConfigureAwait(false);

                    var guidIdentity = events.StreamIdentity == StreamIdentity.AsGuid;
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                    {
                        var alias = reader.IsDBNull(0) ? untagged : reader.GetString(0);
                        var stream = new AggregateStreamLength(
                            guidIdentity ? reader.GetGuid(1) : Guid.Empty,
                            guidIdentity ? null : reader.GetString(1),
                            reader.IsDBNull(2) ? null : reader.GetString(2),
                            reader.GetInt64(3));

                        if (!longest.TryGetValue(alias, out var list))
                        {
                            longest[alias] = list = [];
                        }

                        list.Add(stream);
                    }
                }
            }, (_connectionString, sql, _events, request, tenantId, stats, longest), token).ConfigureAwait(false);
        }
        catch (Exception e) when (MissingStorageDetection.IsMissingStorage(e))
        {
            // #677: no streams table, so there are no streams to count
            return [];
        }

        return stats
            .Select(pair =>
            {
                var alias = pair.Key == untagged ? null : pair.Key;
                var (count, total, max, counts) = pair.Value;

                return new AggregateStreamStats(
                    alias,
                    alias == null ? null : _events.TryResolveAggregateType(alias),
                    count,
                    total,
                    max,
                    thresholds.Select((threshold, i) => new StreamLengthBucket(threshold, counts[i])).ToList(),
                    longest.TryGetValue(pair.Key, out var streams) ? streams : []);
            })
            .ToList();
    }

    /// <summary>
    ///     The batch behind <see cref="FetchAggregateStreamStatsAsync" />: the grouped count, then, when asked for,
    ///     the longest streams of each type in rank order. Thresholds bind as <c>@t0</c>, <c>@t1</c>, ...; the
    ///     ranking's limit as <c>@longest</c>.
    /// </summary>
    internal static string AggregateStreamStatsSql(EventGraph events, int thresholdCount, bool filterByTenant,
        bool includeArchived, bool rankLongest)
    {
        var conditions = new List<string>();
        if (!includeArchived) conditions.Add("is_archived = 0");
        if (filterByTenant) conditions.Add("tenant_id = @tenant_id");
        var where = conditions.Count == 0 ? "" : " WHERE " + string.Join(" AND ", conditions);

        var sql = new StringBuilder("SELECT type, COUNT_BIG(*), COALESCE(SUM(version), 0), COALESCE(MAX(version), 0)");
        for (var i = 0; i < thresholdCount; i++)
        {
            sql.Append($", SUM(CASE WHEN version >= @t{i} THEN CAST(1 AS bigint) ELSE 0 END)");
        }

        sql.Append($" FROM {events.StreamsTableName}{where} GROUP BY type;");

        if (rankLongest)
        {
            sql.Append(
                " SELECT type, id, tenant_id, version FROM (SELECT type, id, tenant_id, version, " +
                "ROW_NUMBER() OVER (PARTITION BY type ORDER BY version DESC) AS stream_rank " +
                $"FROM {events.StreamsTableName}{where}) ranked WHERE stream_rank <= @longest " +
                "ORDER BY type, stream_rank;");
        }

        return sql.ToString();
    }
}
