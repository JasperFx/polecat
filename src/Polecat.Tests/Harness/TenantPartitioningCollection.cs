using Microsoft.Data.SqlClient;
using Polecat.TestUtils;

namespace Polecat.Tests.Harness;

/// <summary>
///     Serializes the per-tenant physical-partitioning tests. SQL Server partition functions and
///     schemes are database-global objects (their names — e.g. <c>pf_pc_events_tenant_ordinal</c> —
///     carry no schema), so two tenant-partitioned <c>pc_events</c> tables in different schemas of the
///     same database would collide if their tests ran concurrently. DisableParallelization keeps them
///     strictly sequential; <see cref="PartitionTestCleanup" /> wipes the shared objects between runs.
/// </summary>
[CollectionDefinition("tenant-partitioning", DisableParallelization = true)]
public class TenantPartitioningCollection;

/// <summary>
///     Drops the database-global partition function/scheme that the managed tenant partitioning leaves
///     behind for a tenant-partitioned <c>pc_events</c> — they outlive a schema/table drop and must be
///     removed explicitly so each test starts clean.
/// </summary>
public static class PartitionTestCleanup
{
    public static async Task DropEventsPartitionObjectsAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            -- The scheme is shared across every test schema's pc_events, so drop ALL tables sitting on
            -- it (in any schema) before the scheme/function can be removed.
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
            JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
            WHERE ds.name = 'ps_pc_events_tenant_ordinal';
            IF @sql <> N'' EXEC sp_executesql @sql;

            IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_pc_events_tenant_ordinal')
                DROP PARTITION SCHEME ps_pc_events_tenant_ordinal;
            IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_pc_events_tenant_ordinal')
                DROP PARTITION FUNCTION pf_pc_events_tenant_ordinal;
            """;
        await cmd.ExecuteNonQueryAsync();
    }
}
