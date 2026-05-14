using JasperFx.Events;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Events.Dcb;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public record ArchStudentId(Guid Value);
public record ArchCourseId(Guid Value);
public record ArchStudentEnrolled(string Name, string Course);

[Collection("integration")]
public class archived_partitioning_dcb_tag_tests : IntegrationContext
{
    public archived_partitioning_dcb_tag_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        var conn = await OpenConnectionAsync();

        // The partition scheme/function ps_pc_events_is_archived /
        // pf_pc_events_is_archived are global SQL Server objects (Weasel does not
        // schema-qualify them). Other tests that use UseArchivedStreamPartitioning
        // (e.g. archived_stream_partitioning.cs) may leave a pc_events table in
        // their own schema still referencing the partition scheme — when our
        // Weasel migration later tries to drop+recreate the scheme it raises:
        //
        //     The partition scheme "ps_pc_events_is_archived" is currently being
        //     used to partition one or more tables.
        //
        // Cleanup strategy:
        //   1. Drop every table anywhere that uses the partition scheme (and the
        //      FK constraints they own first so the drops don't cascade-block).
        //   2. Drop our own schema's remaining tables / FKs and the schema itself.
        //   3. Drop the now-unreferenced partition scheme + function so Weasel
        //      can recreate them cleanly on apply.
        await using var dropCmd = new SqlCommand("""
            -- 1a. Drop FK constraints owned by tables using ps_pc_events_is_archived
            DECLARE @fkSql NVARCHAR(MAX) = '';
            SELECT @fkSql += 'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name)
                          + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id <= 1
            JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            WHERE ps.name = 'ps_pc_events_is_archived';
            IF LEN(@fkSql) > 0 EXEC sp_executesql @fkSql;

            -- 1b. Drop tables that use the partition scheme (anywhere)
            DECLARE @ptSql NVARCHAR(MAX) = '';
            SELECT @ptSql += 'DROP TABLE IF EXISTS ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name) + ';'
            FROM sys.tables t
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id <= 1
            JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
            WHERE ps.name = 'ps_pc_events_is_archived';
            IF LEN(@ptSql) > 0 EXEC sp_executesql @ptSql;

            -- 2a. Drop remaining FKs owned by our schema
            DECLARE @fkSql2 NVARCHAR(MAX) = '';
            SELECT @fkSql2 += 'ALTER TABLE [arch_dcb_tags].' + QUOTENAME(t.name)
                           + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            WHERE t.schema_id = SCHEMA_ID('arch_dcb_tags');
            IF LEN(@fkSql2) > 0 EXEC sp_executesql @fkSql2;

            -- 2b. Drop remaining tables in our schema
            DECLARE @tblSql NVARCHAR(MAX) = '';
            SELECT @tblSql += 'DROP TABLE IF EXISTS [arch_dcb_tags].' + QUOTENAME(name) + ';'
            FROM sys.tables WHERE schema_id = SCHEMA_ID('arch_dcb_tags');
            IF LEN(@tblSql) > 0 EXEC sp_executesql @tblSql;

            -- 2c. Drop the schema itself
            IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'arch_dcb_tags')
                DROP SCHEMA [arch_dcb_tags];

            -- 3. Drop the now-unreferenced partition scheme + function
            IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_pc_events_is_archived')
                DROP PARTITION SCHEME [ps_pc_events_is_archived];
            IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_pc_events_is_archived')
                DROP PARTITION FUNCTION [pf_pc_events_is_archived];
            """, conn);
        await dropCmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task can_create_schema_with_archived_partitioning_and_tags()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "arch_dcb_tags";
            opts.EventGraph.UseArchivedStreamPartitioning = true;
            opts.Events.RegisterTagType<ArchStudentId>("arch_student");
            opts.Events.RegisterTagType<ArchCourseId>("arch_course");
            opts.EventGraph.AddEventType(typeof(ArchStudentEnrolled));
        });

        // Idempotent re-apply
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task can_append_events_with_tags_and_archived_partitioning()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "arch_dcb_tags";
            opts.EventGraph.UseArchivedStreamPartitioning = true;
            opts.Events.RegisterTagType<ArchStudentId>("arch_student");
            opts.Events.RegisterTagType<ArchCourseId>("arch_course");
        });

        var studentId = new ArchStudentId(Guid.NewGuid());
        var courseId = new ArchCourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        var enrolled = theSession.Events.BuildEvent(new ArchStudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        theSession.Events.StartStream(streamId, enrolled);
        await theSession.SaveChangesAsync();
    }

    [Fact]
    public async Task can_query_events_exist_with_tags_and_archived_partitioning()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "arch_dcb_tags";
            opts.EventGraph.UseArchivedStreamPartitioning = true;
            opts.Events.RegisterTagType<ArchStudentId>("arch_student");
            opts.Events.RegisterTagType<ArchCourseId>("arch_course");
        });

        var studentId = new ArchStudentId(Guid.NewGuid());
        var courseId = new ArchCourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        var enrolled = theSession.Events.BuildEvent(new ArchStudentEnrolled("Bob", "Science"));
        enrolled.WithTag(studentId, courseId);
        theSession.Events.StartStream(streamId, enrolled);
        await theSession.SaveChangesAsync();

        await using var reader = theStore.LightweightSession();
        var exists = await reader.Events.EventsExistAsync(
            new EventTagQuery().Or<ArchStudentId>(studentId));
        exists.ShouldBeTrue();
    }
}
