#nullable enable
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #394: the exception shape <c>SaveChangesAsync</c> throws when DCB consistency boundaries lose a
///     concurrency race. A session with a single boundary — overwhelmingly the common case — throws the
///     <see cref="DcbConcurrencyException" /> directly, matching Marten, so the documented
///     <c>catch (DcbConcurrencyException)</c> retry pattern ports between the two stores. Only a
///     session that violates several boundaries at once still wraps them in an
///     <see cref="AggregateException" />, since there is more than one failure to carry.
/// </summary>
[Collection("integration")]
public class dcb_concurrency_exception_shape_tests : IntegrationContext
{
    public dcb_concurrency_exception_shape_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.Events.RegisterTagType<StudentId>("student")
                .ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<CourseId>("course")
                .ForAggregate<StudentCourseEnrollment>();
        });
    }

    [Fact]
    public async Task single_violated_boundary_throws_the_concurrency_exception_directly()
    {
        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        theSession.Events.Append(streamId, enrolled);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session1 = theStore.LightweightSession();
        var query = new EventTagQuery().Or<StudentId>(studentId);
        var boundary =
            await session1.Events.FetchForWritingByTags<StudentCourseEnrollment>(query,
                TestContext.Current.CancellationToken);

        // Another session slips a matching event in before session1 saves
        await using var session2 = theStore.LightweightSession();
        var conflicting = session2.Events.BuildEvent(new AssignmentSubmitted("HW-conflict", 50));
        conflicting.WithTag(studentId, courseId);
        session2.Events.Append(streamId, conflicting);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        var assignment = session1.Events.BuildEvent(new AssignmentSubmitted("HW1", 95));
        assignment.WithTag(studentId, courseId);
        boundary.AppendOne(assignment);

        // Not an AggregateException -- Should.ThrowAsync<T> is exact-type, so this also asserts the
        // absence of the wrapper the pre-#394 behavior added.
        var violation = await Should.ThrowAsync<DcbConcurrencyException>(
            () => session1.SaveChangesAsync(TestContext.Current.CancellationToken));

        violation.Query.ShouldNotBeNull();
    }

    [Fact]
    public async Task several_violated_boundaries_are_still_wrapped_in_an_aggregate_exception()
    {
        var student1 = new StudentId(Guid.NewGuid());
        var student2 = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        var first = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        first.WithTag(student1, courseId);
        theSession.Events.Append(stream1, first);

        var second = theSession.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        second.WithTag(student2, courseId);
        theSession.Events.Append(stream2, second);

        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // One session, two independent consistency boundaries
        await using var session1 = theStore.LightweightSession();
        var boundary1 = await session1.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<StudentId>(student1), TestContext.Current.CancellationToken);
        var boundary2 = await session1.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<StudentId>(student2), TestContext.Current.CancellationToken);

        // Another session invalidates both of them
        await using var session2 = theStore.LightweightSession();
        var conflict1 = session2.Events.BuildEvent(new AssignmentSubmitted("HW-conflict-1", 50));
        conflict1.WithTag(student1, courseId);
        session2.Events.Append(stream1, conflict1);

        var conflict2 = session2.Events.BuildEvent(new AssignmentSubmitted("HW-conflict-2", 60));
        conflict2.WithTag(student2, courseId);
        session2.Events.Append(stream2, conflict2);

        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        var append1 = session1.Events.BuildEvent(new AssignmentSubmitted("HW1", 95));
        append1.WithTag(student1, courseId);
        boundary1.AppendOne(append1);

        var append2 = session1.Events.BuildEvent(new AssignmentSubmitted("HW2", 85));
        append2.WithTag(student2, courseId);
        boundary2.AppendOne(append2);

        var aggregate = await Should.ThrowAsync<AggregateException>(
            () => session1.SaveChangesAsync(TestContext.Current.CancellationToken));

        aggregate.InnerExceptions.Count.ShouldBe(2);
        aggregate.InnerExceptions.ShouldAllBe(x => x is DcbConcurrencyException);
    }
}
