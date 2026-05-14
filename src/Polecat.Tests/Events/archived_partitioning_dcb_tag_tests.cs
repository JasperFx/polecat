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
        await using var dropCmd = new SqlCommand("""
            IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'arch_dcb_tags')
            BEGIN
                DECLARE @sql NVARCHAR(MAX) = '';
                SELECT @sql = @sql + 'DROP TABLE IF EXISTS [arch_dcb_tags].' + QUOTENAME(name) + ';'
                FROM sys.tables WHERE schema_id = SCHEMA_ID('arch_dcb_tags');
                EXEC sp_executesql @sql;
                DROP SCHEMA IF EXISTS [arch_dcb_tags];
            END
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
