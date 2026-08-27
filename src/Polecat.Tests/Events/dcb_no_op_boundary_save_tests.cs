#nullable enable
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     gh-515: a DCB boundary is a condition on an APPEND, so a session that reads one and then saves
///     without appending anything must not assert it — and above all must not bump
///     <c>pc_dcb_tag_version</c>, because that would invalidate every concurrent session's boundary over
///     a save that changed nothing.
/// </summary>
public class dcb_no_op_boundary_save_tests : OneOffConfigurationsContext
{
    private async Task ConfigureAsync()
    {
        ConfigureStore(opts =>
        {
            opts.Events.RegisterTagType<StudentId>("student")
                .ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<CourseId>("course")
                .ForAggregate<StudentCourseEnrollment>();
        });

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task a_read_only_boundary_save_does_not_invalidate_a_concurrent_boundary()
    {
        await ConfigureAsync();

        var courseId = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(courseId);

        // Session 1 reads the boundary and holds it.
        await using var session1 = theStore.LightweightSession();
        var boundary1 = await session1.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            query, TestContext.Current.CancellationToken);

        // Session 2 reads the same boundary, decides there is nothing to do, and saves. Its save writes
        // nothing, so it must leave session 1's boundary intact.
        await using (var session2 = theStore.LightweightSession())
        {
            await session2.Events.FetchForWritingByTags<StudentCourseEnrollment>(
                query, TestContext.Current.CancellationToken);
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var enrolled = session1.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(new StudentId(Guid.NewGuid()), courseId);
        boundary1.AppendOne(enrolled);

        // Pre-gh-515 semantics asserted unconditionally; with a version-based check that would have
        // bumped the row out from under session 1 and thrown here.
        await Should.NotThrowAsync(() => session1.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task a_read_only_boundary_save_is_not_itself_a_violation()
    {
        await ConfigureAsync();

        var courseId = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(courseId);

        await using var session = theStore.LightweightSession();
        await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            query, TestContext.Current.CancellationToken);

        // Someone else commits under the same tag while this session holds a boundary it never uses.
        await using (var other = theStore.LightweightSession())
        {
            var submitted = other.Events.BuildEvent(new AssignmentSubmitted("HW1", 90));
            submitted.WithTag(new StudentId(Guid.NewGuid()), courseId);
            other.Events.Append(Guid.NewGuid(), submitted);
            await other.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Nothing was appended against the boundary, so there is no append for it to guard.
        await Should.NotThrowAsync(() => session.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task a_plain_tagged_append_still_invalidates_an_in_flight_boundary()
    {
        await ConfigureAsync();

        var courseId = new CourseId(Guid.NewGuid());
        var query = new EventTagQuery().Or<CourseId>(courseId);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            query, TestContext.Current.CancellationToken);

        // No boundary here -- just an ordinary tagged append. The producer-side bump is what makes this
        // visible to the boundary above; without it the side table would only ever reflect boundary saves.
        await using (var other = theStore.LightweightSession())
        {
            var submitted = other.Events.BuildEvent(new AssignmentSubmitted("HW1", 90));
            submitted.WithTag(new StudentId(Guid.NewGuid()), courseId);
            other.Events.Append(Guid.NewGuid(), submitted);
            await other.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var enrolled = session.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(new StudentId(Guid.NewGuid()), courseId);
        boundary.AppendOne(enrolled);

        await Should.ThrowAsync<DcbConcurrencyException>(
            () => session.SaveChangesAsync(TestContext.Current.CancellationToken));
    }
}
