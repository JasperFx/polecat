using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Tags;
using Polecat.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

#region sample_polecat_dcb_identity_less_boundary_aggregate

/// <summary>
///     The aggregate from the "Identity-less Boundary Aggregates" section of docs/events/dcb.md.
///     It spans streams by tag and is keyed to none, so it has no <c>Id</c> —
///     <see cref="BoundaryAggregateAttribute" /> is the explicit opt-out that makes the source
///     generator emit its evolver. Until polecat#521 this type existed only in the markdown, which
///     is how the documented model shipped broken.
/// </summary>
[BoundaryAggregate]
public partial class CourseEnrollmentSummary
{
    public int EnrolledCount { get; private set; }
    public List<string> Students { get; } = new();

    public void Apply(StudentEnrolled e)
    {
        Students.Add(e.StudentName);
        EnrolledCount++;
    }

    public void Apply(StudentDropped e)
    {
        EnrolledCount--;
    }
}

#endregion

/// <summary>
///     An identity-less aggregate WITHOUT the marker. Kept broken on purpose: the generator emits
///     nothing for one, and a missing <c>Id</c> is far more often an oversight than a deliberate
///     boundary aggregate, so this must still fail fast.
/// </summary>
public class UnmarkedIdentitylessAggregate
{
    public int Count { get; set; }

    public void Apply(StudentEnrolled e) => Count++;
}

public class identity_less_boundary_aggregate_tests : IntegrationContext
{
    public identity_less_boundary_aggregate_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.Events.RegisterTagType<CourseId>("course")
                .ForAggregate<CourseEnrollmentSummary>();
        });
    }

    // ---- the aggregator can be built at all -------------------------------------------------

    /// <summary>
    ///     The unit-level reproduction from polecat#521: this threw before any I/O, because
    ///     Build&lt;TDoc&gt; resolved the identity through DocumentMapping, which demands an Id.
    /// </summary>
    [Fact]
    public void aggregation_source_can_be_built_for_an_identity_less_boundary_aggregate()
    {
        var factory = (IAggregationSourceFactory<IQuerySession>)theStore.Options.EventGraph;

        var source = factory.Build<CourseEnrollmentSummary>();

        source.ShouldNotBeNull();
    }

    [Fact]
    public void an_identity_less_aggregate_without_the_marker_still_throws()
    {
        var factory = (IAggregationSourceFactory<IQuerySession>)theStore.Options.EventGraph;

        var ex = Should.Throw<InvalidOperationException>(() =>
            factory.Build<UnmarkedIdentitylessAggregate>());

        // The message has to name the marker that would fix it — DocumentMapping's own message is
        // phrased for documents and never mentions [BoundaryAggregate].
        ex.Message.ShouldContain("BoundaryAggregate");
        ex.Message.ShouldContain(nameof(UnmarkedIdentitylessAggregate));
    }

    // ---- and actually aggregates ------------------------------------------------------------

    /// <summary>
    ///     The coverage that matters, per polecat#521: FetchForWritingByTags only resolves the
    ///     aggregator when the query finds events, so a suite that exercises only the empty
    ///     "this must not exist yet" boundary is green over a model that breaks on first real use.
    ///     This one has events present.
    /// </summary>
    [Fact]
    public async Task fetch_for_writing_by_tags_aggregates_with_events_present()
    {
        var courseId = new CourseId(Guid.NewGuid());

        await AppendEnrollmentAsync(courseId, "Alice");
        await AppendEnrollmentAsync(courseId, "Bob");

        await using var session = theStore.LightweightSession();
        var query = new EventTagQuery().Or<CourseId>(courseId);
        var boundary = await session.Events.FetchForWritingByTags<CourseEnrollmentSummary>(
            query, TestContext.Current.CancellationToken);

        boundary.Aggregate.ShouldNotBeNull();
        boundary.Aggregate.EnrolledCount.ShouldBe(2);
        boundary.Aggregate.Students.OrderBy(x => x).ToList().ShouldBe(new List<string> { "Alice", "Bob" });
    }

    [Fact]
    public async Task events_that_decrement_are_applied_too()
    {
        var courseId = new CourseId(Guid.NewGuid());

        await AppendEnrollmentAsync(courseId, "Alice");
        await AppendEnrollmentAsync(courseId, "Bob");
        await AppendDropAsync(courseId);

        await using var session = theStore.LightweightSession();
        var query = new EventTagQuery().Or<CourseId>(courseId);
        var boundary = await session.Events.FetchForWritingByTags<CourseEnrollmentSummary>(
            query, TestContext.Current.CancellationToken);

        boundary.Aggregate.ShouldNotBeNull().EnrolledCount.ShouldBe(1);
    }

    /// <summary>
    ///     The empty case still works — it is the path that was already green, kept so the fix
    ///     cannot regress it.
    /// </summary>
    [Fact]
    public async Task fetch_for_writing_by_tags_over_no_events_yields_a_null_aggregate()
    {
        await using var session = theStore.LightweightSession();
        var query = new EventTagQuery().Or<CourseId>(new CourseId(Guid.NewGuid()));

        var boundary = await session.Events.FetchForWritingByTags<CourseEnrollmentSummary>(
            query, TestContext.Current.CancellationToken);

        boundary.Aggregate.ShouldBeNull();
    }

    /// <summary>
    ///     The boundary a marked aggregate establishes still enforces concurrency — the marker
    ///     exempts the type from needing an identity, not from the DCB contract.
    /// </summary>
    [Fact]
    public async Task the_boundary_still_enforces_concurrency()
    {
        var courseId = new CourseId(Guid.NewGuid());
        await AppendEnrollmentAsync(courseId, "Alice");

        await using var session = theStore.LightweightSession();
        var query = new EventTagQuery().Or<CourseId>(courseId);
        var boundary = await session.Events.FetchForWritingByTags<CourseEnrollmentSummary>(
            query, TestContext.Current.CancellationToken);

        // Someone else appends under the same tag after the boundary was captured.
        await AppendEnrollmentAsync(courseId, "Carol");

        var next = session.Events.BuildEvent(new StudentEnrolled("Dave", "Math"));
        next.WithTag(courseId);
        boundary.AppendOne(next);

        await Should.ThrowAsync<DcbConcurrencyException>(
            () => session.SaveChangesAsync(TestContext.Current.CancellationToken));
    }

    private async Task AppendEnrollmentAsync(CourseId courseId, string student)
    {
        await using var session = theStore.LightweightSession();
        var e = session.Events.BuildEvent(new StudentEnrolled(student, "Math"));
        e.WithTag(courseId);
        session.Events.Append(Guid.NewGuid(), e);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private async Task AppendDropAsync(CourseId courseId)
    {
        await using var session = theStore.LightweightSession();
        var e = session.Events.BuildEvent(new StudentDropped("moved away"));
        e.WithTag(courseId);
        session.Events.Append(Guid.NewGuid(), e);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }
}
