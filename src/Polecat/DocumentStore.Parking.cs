using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Schema;
using Polecat.Internal;
using Polecat.Storage;

namespace Polecat;

// marten#6003 / #1053: what a rebuild, a rewind and a per-tenant teardown do to the parked identities and the slice
// indexes of the projection they reset
public partial class DocumentStore
{
    /// <summary>
    ///     A rebuild re-folds every identity, so none of the projection's identities stays parked (every version of it),
    ///     and it writes the slice index of the registered version again from its start. With a tenant, only that
    ///     tenant's rows.
    /// </summary>
    internal Task ClearParkingForTeardownAsync(PolecatDatabase database, string projectionName, string? tenantId,
        CancellationToken token)
    {
        var statements = new List<string>();
        var events = Options.EventGraph;
        var tenantFilter = tenantId == null ? "" : " AND tenant_id = @tenant";

        if (events.EnableParkedIdentities)
        {
            statements.Add(guarded(events.ParkedIdentitiesTableName,
                $"DELETE FROM {events.ParkedIdentitiesTableName} WHERE projection_name = @name{tenantFilter};"));
        }

        if (events.SupportsSliceIndex && Options.Projections.TryFindProjection(projectionName, out var source)
                                      && source.Options.SliceIndex != null)
        {
            var table = sliceIndexTable(source.Name, source.Version);
            statements.Add(guarded(table,
                tenantId == null ? $"DELETE FROM {table};" : $"DELETE FROM {table} WHERE tenant_id = @tenant;"));
        }

        return executeParkingCleanupAsync(database, statements, projectionName, tenantId, null, token);
    }

    /// <summary>
    ///     A rewind replays everything above its floor: an identity parked above it is parked again when the shard gets
    ///     there, and the slice index rows above it are written again. The parks below the floor stay, because those
    ///     identities are still frozen.
    /// </summary>
    internal async Task ClearParkingForRewindAsync(PolecatDatabase database, IReadOnlyList<ShardName> shards,
        long floor, CancellationToken token)
    {
        var events = Options.EventGraph;
        if (!events.EnableParkedIdentities) return;

        foreach (var shard in shards)
        {
            var tenantFilter = shard.TenantId == null ? "" : " AND tenant_id = @tenant";
            var statements = new List<string>
            {
                guarded(events.ParkedIdentitiesTableName,
                    $"DELETE FROM {events.ParkedIdentitiesTableName} WHERE projection_name = @name AND projection_version = @version AND shard_key = @shard AND first_failing_seq > @floor{tenantFilter};")
            };

            if (events.SupportsSliceIndex)
            {
                var table = sliceIndexTable(shard.Name, shard.Version);
                statements.Add(guarded(table, $"DELETE FROM {table} WHERE seq_id > @floor{tenantFilter};"));
            }

            await executeParkingCleanupAsync(database, statements, shard.Name, shard.TenantId,
                (shard.Version, shard.ShardKey, floor), token).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     The shards a rewind of <paramref name="subscriptionName" /> reaches: a registered shard's identity is that
    ///     shard, a projection's name is all of its shards, and anything else may be a per-tenant identity.
    /// </summary>
    internal IReadOnlyList<ShardName> ParkingShardsFor(string subscriptionName)
    {
        var shards = ProgressionShardsFor(subscriptionName);

        if (shards.FirstOrDefault(x => x.Identity == subscriptionName) is { } exact) return [exact];
        if (shards.Length > 0) return shards;

        return ShardName.TryParse(subscriptionName, out var parsed) ? [parsed!] : [];
    }

    private string sliceIndexTable(string projectionName, uint version)
        => SqlEscaping.QualifiedName(Options.EventGraph.DatabaseSchemaName,
            SliceIndexTable.TableNameFor(projectionName, version));

    // A table left out by AutoCreate.None, or the slice index of a projection that has never run, has nothing to clear
    private static string guarded(string table, string statement)
        => $"IF OBJECT_ID({SqlEscaping.Literal(table)}, 'U') IS NOT NULL {statement}";

    private async Task executeParkingCleanupAsync(PolecatDatabase database, List<string> statements,
        string projectionName, string? tenantId, (uint Version, string ShardKey, long Floor)? shard,
        CancellationToken token)
    {
        if (statements.Count == 0) return;

        await Options.ResiliencePipeline.ExecuteAsync(static async (state, ct) =>
        {
            var (connectionString, sql, name, tenant, shard, unicode) = state;
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = string.Join(Environment.NewLine, sql);
            cmd.Parameters.AddKey("@name", name, unicode);
            if (tenant != null) cmd.Parameters.AddKey("@tenant", tenant, unicode);
            if (shard is { } s)
            {
                cmd.Parameters.Add("@version", System.Data.SqlDbType.Int).Value = (int)s.Version;
                cmd.Parameters.AddKey("@shard", s.ShardKey, unicode);
                cmd.Parameters.Add("@floor", System.Data.SqlDbType.BigInt).Value = s.Floor;
            }

            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, (database.ConnectionString, statements, projectionName, tenantId, shard, Options.Advanced.UseUnicodeKeys),
            token).ConfigureAwait(false);
    }
}
