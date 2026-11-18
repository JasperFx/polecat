using Microsoft.Data.SqlClient;
using Polecat.TestUtils;

namespace Polecat.Tests.Harness;

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
    ///     The same for <c>UseArchivedStreamPartitioning</c>, except that it never drops a table:
    ///     <c>ps_pc_events_is_archived</c> is one database-global object, and since it became the 6.0
    ///     default (#1041) every test schema's <c>pc_events</c> sits on it, so dropping the tables on it
    ///     would pull the event store out from under whatever else is running. Weasel's partition DDL has
    ///     reused a scheme that is in use since weasel#765, so a leftover table no longer blocks anything;
    ///     this only removes the scheme and function when nothing is on them.
    /// </summary>
    public static async Task DropArchivedPartitionObjectsAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes ps
                           JOIN sys.indexes i ON i.data_space_id = ps.data_space_id
                           WHERE ps.name = 'ps_pc_events_is_archived')
            BEGIN
                IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_pc_events_is_archived')
                    DROP PARTITION SCHEME [ps_pc_events_is_archived];
                IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_pc_events_is_archived')
                    DROP PARTITION FUNCTION [pf_pc_events_is_archived];
            END
            """;
        await cmd.ExecuteNonQueryAsync();
    }
}
