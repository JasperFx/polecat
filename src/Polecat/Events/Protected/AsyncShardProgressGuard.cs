using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal;

namespace Polecat.Events.Protected;

/// <summary>
///     Which async projections and subscriptions have not yet processed the events up to a sequence. For an
///     operation that takes events away from the daemon: compaction deletes them (#1086, marten#6050), and so
///     does moving a stream to cold storage (#1051, marten#5710).
/// </summary>
/// <remarks>
///     A shard is behind unless its <c>pc_event_progression</c> row has reached the sequence, and a shard with no
///     row (one that never ran) is behind. A shard's row is the one for the tenant (<c>{shard}:{tenant}</c>) when
///     the daemon keeps per-tenant progress, else the store-global one. A shard only moves forward, so one that has
///     caught up when this reads is still caught up when the caller's work commits.
/// </remarks>
internal static class AsyncShardProgressGuard
{
    /// <summary>
    ///     The shards behind <paramref name="sequence" />, each with the sequence it is at, for the tenant of
    ///     <paramref name="events" />. Empty when nothing is behind.
    /// </summary>
    /// <param name="events">The tenant-scoped event operations whose session reads the progression rows.</param>
    /// <param name="sequence">The highest sequence the caller is about to take away.</param>
    /// <param name="exemptAggregateType">
    ///     An aggregate type whose single-stream projections are left out: compaction's own type, which
    ///     fast-forwards over the <c>Compacted&lt;T&gt;</c> it leaves behind.
    /// </param>
    /// <param name="token"></param>
    internal static async Task<IReadOnlyList<(string Name, long Sequence)>> FindShardsBehindAsync(
        EventOperations events, long sequence, Type? exemptAggregateType, CancellationToken token)
    {
        var options = events.Session.Options;

        var exempt = exemptAggregateType == null
            ? new HashSet<string>(StringComparer.Ordinal)
            : options.Projections.All
                .Where(x => x is IAggregateProjection { Scope: AggregationScope.SingleStream } aggregate &&
                            aggregate.AggregateType == exemptAggregateType)
                .Select(x => x.Name)
                .ToHashSet(StringComparer.Ordinal);

        var tenantId = events.TenantId;
        var shards = options.Projections.AllShards()
            .Where(x => !exempt.Contains(x.Name.Name))
            .Select(x => (Global: x.Name.Identity,
                Tenant: ShardName.Compose(x.Name.Name, x.Name.ShardKey, tenantId, x.Name.Version).Identity))
            .Distinct()
            .ToArray();

        if (shards.Length == 0)
        {
            return [];
        }

        var names = shards.SelectMany(x => new[] { x.Global, x.Tenant }).Distinct(StringComparer.Ordinal).ToArray();

        await using var cmd = new SqlCommand();
        var parameters = new string[names.Length];
        for (var i = 0; i < names.Length; i++)
        {
            parameters[i] = "@name" + i;
            cmd.Parameters.AddKey(parameters[i], names[i], events.Graph.UnicodeKeys);
        }

        cmd.CommandText =
            $"SELECT name, last_seq_id FROM {events.Graph.ProgressionTableName} WHERE name IN ({string.Join(", ", parameters)});";

        // Keyed as the server compares names: case-insensitively under the default collation
        var progress = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        await using (var reader = await events.Session.ExecuteReaderAsync(cmd, token).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                progress[await reader.GetFieldValueAsync<string>(0, token).ConfigureAwait(false)] =
                    await reader.GetFieldValueAsync<long>(1, token).ConfigureAwait(false);
            }
        }

        return shards
            .Select(x => progress.TryGetValue(x.Tenant, out var tenantSeq)
                ? (Name: x.Tenant, Sequence: tenantSeq)
                : (Name: x.Global, Sequence: progress.GetValueOrDefault(x.Global)))
            .Where(x => x.Sequence < sequence)
            .ToArray();
    }
}
