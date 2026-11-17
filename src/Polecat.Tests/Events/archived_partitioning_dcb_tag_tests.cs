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

    public override async ValueTask InitializeAsync()
    {
        // Start from an empty schema. The partition scheme and function are database-global and, since
        // archived partitioning became the 6.0 default (#1041), other schemas' pc_events tables sit on
        // them too, so they are left alone: Weasel reuses a scheme that is already there.
        await TestSchema.DropSchemaTablesAsync("arch_dcb_tags");
        await TestSchema.DropSequencesAsync("arch_dcb_tags");
        await base.InitializeAsync();
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
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
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
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
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
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var reader = theStore.LightweightSession();
        var exists = await reader.Events.EventsExistAsync(
            new EventTagQuery().Or<ArchStudentId>(studentId), TestContext.Current.CancellationToken);
        exists.ShouldBeTrue();
    }
}
