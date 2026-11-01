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
///     Drops the database-global partition functions/schemes that the managed tenant partitioning
///     leaves behind for tenant-partitioned tables (<c>pc_events</c>, <c>pc_streams</c> since #335,
///     and any tenant-partitioned <c>pc_doc_*</c>) — they outlive a schema/table drop and must be
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
            -- A tag table has a foreign key to a partitioned pc_events (#842), and is not partitioned
            -- itself, so it is not dropped below and would block the drop of what it references.
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + SCHEMA_NAME(c.schema_id) + '].[' + c.name
                               + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables c ON fk.parent_object_id = c.object_id
            JOIN sys.indexes i ON i.object_id = fk.referenced_object_id AND i.index_id IN (0, 1)
            JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
            WHERE ds.name LIKE 'ps[_]pc[_]%[_]tenant[_]ordinal';
            IF @sql <> N'' EXEC sp_executesql @sql;

            -- The schemes are shared across every test schema's tables, so drop ALL tables sitting
            -- on any pc_* tenant-ordinal scheme (in any schema) before the schemes/functions can be
            -- removed.
            SET @sql = N'';
            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
            JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
            WHERE ds.name LIKE 'ps[_]pc[_]%[_]tenant[_]ordinal';
            IF @sql <> N'' EXEC sp_executesql @sql;

            SET @sql = N'';
            SELECT @sql = @sql + 'DROP PARTITION SCHEME [' + name + '];'
            FROM sys.partition_schemes WHERE name LIKE 'ps[_]pc[_]%[_]tenant[_]ordinal';
            IF @sql <> N'' EXEC sp_executesql @sql;

            SET @sql = N'';
            SELECT @sql = @sql + 'DROP PARTITION FUNCTION [' + name + '];'
            FROM sys.partition_functions WHERE name LIKE 'pf[_]pc[_]%[_]tenant[_]ordinal';
            IF @sql <> N'' EXEC sp_executesql @sql;
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    ///     The same for <c>UseArchivedStreamPartitioning</c>: <c>ps_pc_events_is_archived</c> is one
    ///     database-global object, so another test's <c>pc_events</c> left sitting on it blocks Weasel
    ///     from recreating it for the next test.
    /// </summary>
    public static async Task DropArchivedPartitionObjectsAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name)
                               + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id <= 1
            JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            WHERE ps.name = 'ps_pc_events_is_archived';
            IF @sql <> N'' EXEC sp_executesql @sql;

            SET @sql = N'';
            SELECT @sql = @sql + 'DROP TABLE IF EXISTS ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name) + ';'
            FROM sys.tables t
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id <= 1
            JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            WHERE ps.name = 'ps_pc_events_is_archived';
            IF @sql <> N'' EXEC sp_executesql @sql;

            IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_pc_events_is_archived')
                DROP PARTITION SCHEME [ps_pc_events_is_archived];
            IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_pc_events_is_archived')
                DROP PARTITION FUNCTION [pf_pc_events_is_archived];
            """;
        await cmd.ExecuteNonQueryAsync();
    }
}
