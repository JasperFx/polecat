using System.Data;
using Microsoft.Data.SqlClient;
using Polly;

namespace Polecat.Internal;

/// <summary>
///     Deletes one tenant's rows from every table in a store's schema that carries a
///     <c>tenant_id</c>, plus that tenant's <c>pc_event_progression</c> rows. The row-deleting
///     counterpart of <c>RemovePolecatManagedTenantsAsync(..., TenantDropBehavior.DeleteData)</c>,
///     which only works for a PARTITIONED store and drops a partition rather than deleting rows —
///     so before this a conjoined store without partitioning had no supported way to offboard a
///     tenant (polecat#680). Mirrors Marten's <c>TenantDataCleaner</c>.
/// </summary>
/// <remarks>
///     <para>
///         <b>Tables are discovered, not enumerated from configuration.</b> The alternative — walking
///         the document mappings — sees only what this process happens to have registered, and a
///         tenant wipe that misses a table is worse than one that fails: the caller is told the
///         tenant is gone. Discovery from <c>sys.columns</c> covers document tables, the event and
///         stream tables, DCB tag tables, natural-key tables, full-text token tables and flat-table
///         projections uniformly, including tables written by a deployment that configured more
///         document types than this one.
///     </para>
///     <para>
///         <b>Delete order comes from the live foreign-key graph</b> rather than a hardcoded list,
///         for the same reason: Polecat has FKs from tag tables to <c>pc_events</c>, from natural-key
///         tables to <c>pc_streams</c> and from <c>pc_events</c> to <c>pc_streams</c>, and users can
///         add their own between document tables. A fixed order is right until someone adds a
///         reference it does not know about, and then it fails as an FK violation mid-wipe with a
///         tenant half-deleted.
///     </para>
/// </remarks>
internal sealed class TenantDataCleaner
{
    private readonly string _tenantId;
    private readonly string _connectionString;
    private readonly string _schemaName;
    private readonly string _progressionTable;
    private readonly ResiliencePipeline _resilience;

    public TenantDataCleaner(string tenantId, string connectionString, string schemaName,
        string progressionTable, ResiliencePipeline resilience)
    {
        _tenantId = tenantId;
        _connectionString = connectionString;
        _schemaName = schemaName;
        _progressionTable = progressionTable;
        _resilience = resilience;
    }

    public async Task<IReadOnlyDictionary<string, int>> ExecuteAsync(CancellationToken token)
    {
        return await _resilience.ExecuteAsync(static async (state, ct) =>
        {
            var (tenantId, connectionString, schemaName, progressionTable) = state;

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            var tenanted = await LoadTenantedTablesAsync(conn, schemaName, ct).ConfigureAwait(false);
            var edges = await LoadForeignKeyEdgesAsync(conn, schemaName, ct).ConfigureAwait(false);

            var ordered = OrderChildrenFirst(tenanted, edges);

            var deleted = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            // One transaction: a tenant wipe that fails halfway leaves a tenant that is neither
            // present nor absent, and the caller has no way to tell which tables made it.
            await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(
                IsolationLevel.ReadCommitted, ct).ConfigureAwait(false);

            foreach (var table in ordered)
            {
                await using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText =
                    $"DELETE FROM {SqlEscaping.QualifiedName(schemaName, table)} WHERE tenant_id = @tenant;";
                // VarChar, not nvarchar: tenant_id is varchar(250), and an nvarchar parameter makes
                // SQL Server wrap the column in CONVERT_IMPLICIT and scan (#363).
                cmd.Parameters.Add("@tenant", SqlDbType.VarChar, 250).Value = tenantId;
                var rows = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                if (rows > 0) deleted[table] = rows;
            }

            // pc_event_progression has no tenant_id — a tenant's rows are identified by their NAME.
            // Two shapes: the per-tenant high-water row "HighWaterMark:{tenant}" (polecat#697) and a
            // per-tenant projection shard "{Projection}:{Slice}:{tenant}". Both end in ":{tenant}",
            // so one suffix match covers them, with LIKE metacharacters in the tenant id escaped —
            // a tenant literally named "%" must not delete every other tenant's progression.
            await using (var progression = conn.CreateCommand())
            {
                progression.Transaction = tx;
                progression.CommandText = $"""
                    IF OBJECT_ID({SqlEscaping.Literal(progressionTable)}, 'U') IS NOT NULL
                    DELETE FROM {progressionTable} WHERE name LIKE @suffix ESCAPE '\';
                    """;
                progression.Parameters.Add("@suffix", SqlDbType.VarChar, 500).Value =
                    "%:" + EscapeLikePattern(tenantId);
                var rows = await progression.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                if (rows > 0) deleted[progressionTable] = rows;
            }

            await tx.CommitAsync(ct).ConfigureAwait(false);

            return (IReadOnlyDictionary<string, int>)deleted;
        }, (_tenantId, _connectionString, _schemaName, _progressionTable), token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Every table in the schema carrying a <c>tenant_id</c> column. A table without one holds no
    ///     per-tenant rows by construction, so it is not a gap that it is skipped.
    /// </summary>
    private static async Task<List<string>> LoadTenantedTablesAsync(SqlConnection conn, string schema,
        CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT t.name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.columns c ON c.object_id = t.object_id
            WHERE s.name = @schema AND c.name = 'tenant_id'
            ORDER BY t.name;
            """;
        cmd.Parameters.Add("@schema", SqlDbType.NVarChar, 128).Value = schema;

        var tables = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            tables.Add(reader.GetString(0));
        }

        return tables;
    }

    /// <summary>Child → parent foreign-key edges within the schema, self-references excluded.</summary>
    private static async Task<List<(string Child, string Parent)>> LoadForeignKeyEdgesAsync(
        SqlConnection conn, string schema, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT DISTINCT pt.name AS child, rt.name AS parent
            FROM sys.foreign_keys fk
            JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
            JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
            JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
            WHERE ps.name = @schema AND rs.name = @schema
              AND fk.parent_object_id <> fk.referenced_object_id;
            """;
        cmd.Parameters.Add("@schema", SqlDbType.NVarChar, 128).Value = schema;

        var edges = new List<(string, string)>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            edges.Add((reader.GetString(0), reader.GetString(1)));
        }

        return edges;
    }

    /// <summary>
    ///     Order the tables so every child is deleted before the parent it references. A cycle — which
    ///     Weasel can create deliberately (weasel#540) — cannot be satisfied by ordering at all, so the
    ///     remaining tables are appended in name order rather than throwing: the delete may still
    ///     succeed if the cycle's rows go together, and failing the whole wipe over an ordering we
    ///     cannot prove is needed would be worse than attempting it.
    /// </summary>
    internal static List<string> OrderChildrenFirst(
        IReadOnlyList<string> tables, IReadOnlyList<(string Child, string Parent)> edges)
    {
        var set = new HashSet<string>(tables, StringComparer.OrdinalIgnoreCase);

        // parents[child] = the tables it references, restricted to tables we are deleting from.
        var parents = tables.ToDictionary(
            t => t,
            _ => new HashSet<string>(StringComparer.OrdinalIgnoreCase),
            StringComparer.OrdinalIgnoreCase);

        foreach (var (child, parent) in edges)
        {
            if (set.Contains(child) && set.Contains(parent) &&
                !child.Equals(parent, StringComparison.OrdinalIgnoreCase))
            {
                parents[child].Add(parent);
            }
        }

        var ordered = new List<string>(tables.Count);
        var emitted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Repeatedly emit any table none of whose remaining parents are still pending.
        bool progress;
        do
        {
            progress = false;
            foreach (var table in tables)
            {
                if (emitted.Contains(table)) continue;
                if (parents[table].Any(p => !emitted.Contains(p))) continue;

                // A table with no pending parents is safe only once every table REFERENCING it has
                // gone, so emit in reverse: collect leaves last. Handled by the reverse below.
                ordered.Add(table);
                emitted.Add(table);
                progress = true;
            }
        } while (progress);

        // Anything left is in a cycle; append deterministically.
        foreach (var table in tables)
        {
            if (emitted.Add(table)) ordered.Add(table);
        }

        // `ordered` currently runs parents-first (a table appears once its parents have). Deleting
        // needs the opposite: a parent's rows cannot go while a child still references them.
        ordered.Reverse();
        return ordered;
    }

    /// <summary>
    ///     Escape the LIKE metacharacters SQL Server honours inside a pattern, so a tenant id
    ///     containing one matches itself rather than acting as a wildcard.
    /// </summary>
    internal static string EscapeLikePattern(string value) => value
        .Replace("\\", "\\\\")
        .Replace("%", "\\%")
        .Replace("_", "\\_")
        .Replace("[", "\\[");
}
