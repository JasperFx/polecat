#nullable enable
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

#region sample_polecat_dcb_tag_type_definitions
// Strong-typed tag identifiers
public record StudentId(Guid Value);
public record CourseId(Guid Value);
#endregion

#region sample_polecat_dcb_domain_events
// Domain events
public record StudentEnrolled(string StudentName, string CourseName);
public record AssignmentSubmitted(string AssignmentName, int Score);
public record StudentDropped(string Reason);
#endregion

// Event with tag-typed properties for inference testing
public record StudentGraded(StudentId StudentId, CourseId CourseId, int Grade);

// Event with NO tag-typed properties — should fail inference
public record SystemNotification(string Message);

#region sample_polecat_dcb_aggregate
// Aggregate for DCB
public partial class StudentCourseEnrollment
{
    public Guid Id { get; set; }
    public string StudentName { get; set; } = "";
    public string CourseName { get; set; } = "";
    public List<string> Assignments { get; set; } = new();
    public bool IsDropped { get; set; }

    public void Apply(StudentEnrolled e)
    {
        StudentName = e.StudentName;
        CourseName = e.CourseName;
    }

    public void Apply(AssignmentSubmitted e)
    {
        Assignments.Add(e.AssignmentName);
    }

    public void Apply(StudentDropped e)
    {
        IsDropped = true;
    }
}
#endregion

/// <summary>
///     The executable source of the DCB documentation samples in <c>docs/events/dcb.md</c>, and the
///     home of the tag/event/aggregate types Polecat's other DCB test fixtures share.
/// </summary>
/// <remarks>
///     The behavioral coverage that used to live here now runs once in
///     <see cref="JasperFx.Events.ComplianceTests.DcbTagQueryAndConsistencyCompliance{TFixture,TOperations,TQuerySession}"/>
///     against every Critter Stack event store, so it can no longer drift from Marten's copy. What
///     stays behind is the documentation: each test below backs a <c>sample_polecat_dcb_*</c> snippet
///     block, so it has to keep compiling and passing with Polecat-flavored API calls in it.
///
///     Polecat always uses Quick append (direct INSERT with OUTPUT seq_id). Tags are inserted
///     immediately after each event, so DCB works with the only append mode available.
/// </remarks>
[Collection("integration")]
public class dcb_documentation_samples : IntegrationContext
{
    public dcb_documentation_samples(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    #region sample_polecat_dcb_registering_tag_types
    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            // Register tag types -- each gets its own table (pc_event_tag_student, pc_event_tag_course)
            opts.Events.RegisterTagType<StudentId>("student")
                .ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<CourseId>("course")
                .ForAggregate<StudentCourseEnrollment>();
        });
    }
    #endregion

    [Fact]
    public async Task can_query_events_by_single_tag()
    {
        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        #region sample_polecat_dcb_tagging_events
        var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        theSession.Events.Append(streamId, enrolled);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
        #endregion

        await using var session2 = theStore.LightweightSession();
        #region sample_polecat_dcb_query_by_single_tag
        var query = new EventTagQuery().Or<StudentId>(studentId);
        var events = await session2.Events.QueryByTagsAsync(query, TestContext.Current.CancellationToken);
        #endregion

        events.Count.ShouldBe(1);
        events[0].Data.ShouldBeOfType<StudentEnrolled>().StudentName.ShouldBe("Alice");
    }

    [Fact]
    public async Task can_query_events_by_multiple_tags_with_or()
    {
        var student1 = new StudentId(Guid.NewGuid());
        var student2 = new StudentId(Guid.NewGuid());
        var course = new CourseId(Guid.NewGuid());
        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        var e1 = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        e1.WithTag(student1, course);
        theSession.Events.Append(stream1, e1);

        var e2 = theSession.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        e2.WithTag(student2, course);
        theSession.Events.Append(stream2, e2);

        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        #region sample_polecat_dcb_query_multiple_tags_or
        var query = new EventTagQuery()
            .Or<StudentId>(student1)
            .Or<StudentId>(student2);

        var events = await session2.Events.QueryByTagsAsync(query, TestContext.Current.CancellationToken);
        #endregion
        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task can_query_events_by_tag_with_event_type_filter()
    {
        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);

        var submitted = theSession.Events.BuildEvent(new AssignmentSubmitted("HW1", 95));
        submitted.WithTag(studentId, courseId);

        theSession.Events.Append(streamId, enrolled, submitted);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        #region sample_polecat_dcb_query_by_event_type
        var query = new EventTagQuery()
            .Or<AssignmentSubmitted, StudentId>(studentId);

        var events = await session2.Events.QueryByTagsAsync(query, TestContext.Current.CancellationToken);
        #endregion
        events.Count.ShouldBe(1);
        events[0].Data.ShouldBeOfType<AssignmentSubmitted>().AssignmentName.ShouldBe("HW1");
    }

    [Fact]
    public async Task can_aggregate_events_by_tags()
    {
        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);

        var submitted = theSession.Events.BuildEvent(new AssignmentSubmitted("HW1", 95));
        submitted.WithTag(studentId, courseId);

        theSession.Events.Append(streamId, enrolled, submitted);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        #region sample_polecat_dcb_aggregate_by_tags
        var query = new EventTagQuery()
            .Or<StudentId>(studentId)
            .Or<CourseId>(courseId);

        var aggregate = await session2.Events.AggregateByTagsAsync<StudentCourseEnrollment>(query, TestContext.Current.CancellationToken);
        #endregion
        aggregate.ShouldNotBeNull();
        aggregate.StudentName.ShouldBe("Alice");
        aggregate.CourseName.ShouldBe("Math");
        aggregate.Assignments.ShouldContain("HW1");
    }

    [Fact]
    public async Task can_fetch_for_writing_by_tags_happy_path()
    {
        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        theSession.Events.Append(streamId, enrolled);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        #region sample_polecat_dcb_fetch_for_writing_by_tags
        await using var session2 = theStore.LightweightSession();
        var query = new EventTagQuery().Or<StudentId>(studentId);
        var boundary = await session2.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, TestContext.Current.CancellationToken);

        // Read current state
        var aggregate = boundary.Aggregate; // may be null if no events yet
        var lastSequence = boundary.LastSeenSequence;

        // Append via boundary
        var assignment = session2.Events.BuildEvent(new AssignmentSubmitted("HW1", 95));
        assignment.WithTag(studentId, courseId);
        boundary.AppendOne(assignment);

        // Save -- will throw DcbConcurrencyException if another session
        // appended matching events after our read
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        #endregion

        boundary.Aggregate.ShouldNotBeNull();
        boundary.Aggregate!.StudentName.ShouldBe("Alice");
        boundary.Events.Count.ShouldBe(1);
    }

    [Fact]
    public async Task fetch_for_writing_by_tags_detects_concurrency_violation()
    {
        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        theSession.Events.Append(streamId, enrolled);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Session 1: fetch for writing
        await using var session1 = theStore.LightweightSession();
        var query = new EventTagQuery().Or<StudentId>(studentId);
        var boundary = await session1.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, TestContext.Current.CancellationToken);

        // Session 2: append a conflicting event BEFORE session 1 saves
        await using var session2 = theStore.LightweightSession();
        var conflicting = session2.Events.BuildEvent(new AssignmentSubmitted("HW-conflict", 50));
        conflicting.WithTag(studentId, courseId);
        session2.Events.Append(streamId, conflicting);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Session 1: try to save — should throw DcbConcurrencyException
        var assignment = session1.Events.BuildEvent(new AssignmentSubmitted("HW1", 95));
        assignment.WithTag(studentId, courseId);
        boundary.AppendOne(assignment);

        // A session with a single DCB boundary throws the DcbConcurrencyException directly (#394),
        // so the Marten-documented retry pattern below ports unchanged.
        var violation = await Should.ThrowAsync<DcbConcurrencyException>(
            () => session1.SaveChangesAsync(TestContext.Current.CancellationToken));
        violation.Query.ShouldNotBeNull();
    }

    #region sample_polecat_dcb_handling_concurrency
    public static async Task handling_a_concurrency_violation(IDocumentSession session)
    {
        try
        {
            await session.SaveChangesAsync();
        }
        catch (DcbConcurrencyException violation)
        {
            // Reload and retry -- the boundary's tag query had new matching events since the read.
            // violation.Query is the original tag query, violation.LastSeenSequence the sequence
            // the boundary was read at.
            Console.WriteLine($"DCB violation on {violation.Query} at {violation.LastSeenSequence}");
        }
    }
    #endregion

    #region sample_polecat_dcb_events_exist_async
    [Fact]
    public async Task events_exist_returns_true_when_matching_events_found()
    {
        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        var enrolled = theSession.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
        enrolled.WithTag(studentId, courseId);
        theSession.Events.Append(streamId, enrolled);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Check existence -- lightweight, no event loading
        await using var session2 = theStore.LightweightSession();
        var query = new EventTagQuery().Or<StudentId>(studentId);
        var exists = await session2.Events.EventsExistAsync(query, TestContext.Current.CancellationToken);
        exists.ShouldBeTrue();
    }
    #endregion
}
