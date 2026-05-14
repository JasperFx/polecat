using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class archived_stream_partitioning : IntegrationContext
{
    public archived_stream_partitioning(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        // The partition scheme/function ps_pc_events_is_archived /
        // pf_pc_events_is_archived are global SQL Server objects (Weasel does not
        // schema-qualify them). When another test that uses
        // UseArchivedStreamPartitioning leaves a pc_events table behind (e.g.
        // archived_partitioning_dcb_tag_tests), Weasel's migration here tries to
        // drop+recreate the partition scheme and fails with:
        //
        //     The partition scheme "ps_pc_events_is_archived" is currently being
        //     used to partition one or more tables.
        //
        // Defensively drop every table using the scheme, then drop our schema, then
        // drop the now-unreferenced scheme + function so the migration starts clean.
        var conn = await OpenConnectionAsync();
        await using var dropCmd = new SqlCommand("""
            -- Drop FK constraints owned by tables using ps_pc_events_is_archived
            DECLARE @fkSql NVARCHAR(MAX) = '';
            SELECT @fkSql += 'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name)
                          + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id <= 1
            JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            WHERE ps.name = 'ps_pc_events_is_archived';
            IF LEN(@fkSql) > 0 EXEC sp_executesql @fkSql;

            -- Drop tables that use the partition scheme (anywhere)
            DECLARE @ptSql NVARCHAR(MAX) = '';
            SELECT @ptSql += 'DROP TABLE IF EXISTS ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name) + ';'
            FROM sys.tables t
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id <= 1
            JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            WHERE ps.name = 'ps_pc_events_is_archived';
            IF LEN(@ptSql) > 0 EXEC sp_executesql @ptSql;

            -- Drop remaining FKs in our schema
            DECLARE @fkSql2 NVARCHAR(MAX) = '';
            SELECT @fkSql2 += 'ALTER TABLE [archived_part].' + QUOTENAME(t.name)
                           + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            WHERE t.schema_id = SCHEMA_ID('archived_part');
            IF LEN(@fkSql2) > 0 EXEC sp_executesql @fkSql2;

            -- Drop remaining tables in our schema
            DECLARE @tblSql NVARCHAR(MAX) = '';
            SELECT @tblSql += 'DROP TABLE IF EXISTS [archived_part].' + QUOTENAME(name) + ';'
            FROM sys.tables WHERE schema_id = SCHEMA_ID('archived_part');
            IF LEN(@tblSql) > 0 EXEC sp_executesql @tblSql;

            -- Drop the schema itself
            IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'archived_part')
                DROP SCHEMA [archived_part];

            -- Drop the now-unreferenced partition scheme + function
            IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_pc_events_is_archived')
                DROP PARTITION SCHEME [ps_pc_events_is_archived];
            IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_pc_events_is_archived')
                DROP PARTITION FUNCTION [pf_pc_events_is_archived];
            """, conn);
        await dropCmd.ExecuteNonQueryAsync();

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "archived_part";
            opts.EventGraph.UseArchivedStreamPartitioning = true;
        });
    }

    [Fact]
    public async Task events_table_is_partitioned()
    {
        // Write some events to ensure schema is created
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Partitioned Quest"),
            new MembersJoined(1, "Town", ["Alpha"]));
        await theSession.SaveChangesAsync();

        // Verify partition function exists
        var conn = await OpenConnectionAsync();

        await using var pfCmd = new SqlCommand(
            "SELECT COUNT(*) FROM sys.partition_functions WHERE name = 'pf_pc_events_is_archived'", conn);
        var pfCount = (int)(await pfCmd.ExecuteScalarAsync())!;
        pfCount.ShouldBeGreaterThan(0);

        // Verify partition scheme exists
        await using var psCmd = new SqlCommand(
            "SELECT COUNT(*) FROM sys.partition_schemes WHERE name = 'ps_pc_events_is_archived'", conn);
        var psCount = (int)(await psCmd.ExecuteScalarAsync())!;
        psCount.ShouldBeGreaterThan(0);

        // Verify the table in our schema has 2 partitions
        await using var partCmd = new SqlCommand("""
            SELECT COUNT(*)
            FROM sys.partitions p
            JOIN sys.objects o ON p.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = 'archived_part' AND o.name = 'pc_events' AND p.index_id <= 1
            """, conn);
        var partitionCount = (int)(await partCmd.ExecuteScalarAsync())!;
        // With 1 boundary value (is_archived=1), we get 2 partitions:
        // Partition 1: is_archived < 1 (active), Partition 2: is_archived >= 1 (archived)
        partitionCount.ShouldBe(2);
    }

    [Fact]
    public async Task can_write_and_read_events_with_partitioning()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Readable Quest"),
            new MembersJoined(1, "Town", ["Beta", "Gamma"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(2);
        events[0].Data.ShouldBeOfType<QuestStarted>();
        events[1].Data.ShouldBeOfType<MembersJoined>();
    }

    [Fact]
    public async Task can_archive_and_query_active_events_only()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archive Quest"),
            new MembersJoined(1, "Town", ["Delta"]));
        await theSession.SaveChangesAsync();

        // Archive the stream
        theSession.Events.ArchiveStream(streamId);
        await theSession.SaveChangesAsync();

        // FetchStream should return empty for archived streams (default filters archived)
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(0);

        // Aggregate should return null for archived stream
        var aggregate = await query.Events.FetchLatest<QuestAggregate>(streamId);
        aggregate.ShouldBeNull();
    }
}
