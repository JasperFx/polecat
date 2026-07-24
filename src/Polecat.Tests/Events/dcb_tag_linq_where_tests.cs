#nullable enable
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// Reuses the StudentId / CourseId tag types and the StudentEnrolled / AssignmentSubmitted events
// declared in dcb_tag_query_and_consistency_tests.cs (same namespace).
//
// #364 (marten#4999 parity): IEvent.HasTag<TTag>(value) is a marker method recognized by the LINQ
// provider, compiling to the same correlated tag-table subquery QueryByTagsAsync emits, so DCB tag
// predicates compose into the same Where() as ordinary event predicates. AND-of-tag-predicates +
// normal predicates is the v1 scope; EventTagQuery remains the OR/rich escape hatch.
public class dcb_tag_linq_where_tests : OneOffConfigurationsContext
{
    private async Task<DocumentStore> CreateStore()
    {
        ConfigureStore(opts =>
        {
            opts.Events.RegisterTagType<StudentId>("student");
            opts.Events.RegisterTagType<CourseId>("course");
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        return theStore;
    }

    private static async Task AppendTagged(IDocumentSession session, Guid streamId, object eventData,
        params object[] tags)
    {
        var wrapped = session.Events.BuildEvent(eventData);
        wrapped.WithTag(tags);
        session.Events.Append(streamId, wrapped);
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task has_tag_matches_only_events_carrying_that_tag_value()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        var alice = new StudentId(Guid.NewGuid());
        var bob = new StudentId(Guid.NewGuid());

        await AppendTagged(session, Guid.NewGuid(), new StudentEnrolled("Alice", "Math"), alice);
        await AppendTagged(session, Guid.NewGuid(), new StudentEnrolled("Bob", "Math"), bob);

        #region sample_polecat_dcb_has_tag_linq

        var events = await session.Events.QueryAllRawEvents()
            .Where(e => e.HasTag<StudentId>(alice))
            .ToListAsync();

        #endregion

        events.Count.ShouldBe(1);
        events.Single().Data.ShouldBeOfType<StudentEnrolled>().StudentName.ShouldBe("Alice");
    }

    [Fact]
    public async Task has_tag_composes_with_a_normal_event_predicate_in_one_where()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        var alice = new StudentId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        // Two events for Alice, both tagged with her StudentId, but of different event types.
        await AppendTagged(session, streamId, new StudentEnrolled("Alice", "Math"), alice);
        await AppendTagged(session, streamId, new AssignmentSubmitted("HW1", 95), alice);

        // "Alice's events, but only the enrollments" — the tag predicate AND a normal event predicate.
        var enrolledTypeName = store.Options.EventGraph
            .EventMappingFor(typeof(StudentEnrolled)).EventTypeName;

        var events = await session.Events.QueryAllRawEvents()
            .Where(e => e.HasTag<StudentId>(alice) && e.EventTypeName == enrolledTypeName)
            .ToListAsync();

        events.Count.ShouldBe(1);
        events.Single().Data.ShouldBeOfType<StudentEnrolled>();
    }

    [Fact]
    public async Task has_tag_composes_with_a_timestamp_predicate()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        var alice = new StudentId(Guid.NewGuid());
        await AppendTagged(session, Guid.NewGuid(), new StudentEnrolled("Alice", "Math"), alice);

        var cutoff = DateTimeOffset.UtcNow.AddDays(-1);

        var events = await session.Events.QueryAllRawEvents()
            .Where(e => e.HasTag<StudentId>(alice) && e.Timestamp > cutoff)
            .ToListAsync();

        events.Count.ShouldBe(1);
    }

    [Fact]
    public async Task and_of_two_tag_predicates_requires_both_tags()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        var alice = new StudentId(Guid.NewGuid());
        var math = new CourseId(Guid.NewGuid());

        // Event 1 carries BOTH tags; event 2 carries only the student tag.
        await AppendTagged(session, Guid.NewGuid(), new StudentEnrolled("Alice", "Math"), alice, math);
        await AppendTagged(session, Guid.NewGuid(), new StudentEnrolled("Alice", "Science"), alice);

        var events = await session.Events.QueryAllRawEvents()
            .Where(e => e.HasTag<StudentId>(alice) && e.HasTag<CourseId>(math))
            .ToListAsync();

        // Only the event tagged with both survives the AND.
        events.Count.ShouldBe(1);
        events.Single().Data.ShouldBeOfType<StudentEnrolled>().CourseName.ShouldBe("Math");
    }

    [Fact]
    public async Task has_tag_is_isolated_by_tenant_under_conjoined_tenancy()
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.RegisterTagType<StudentId>("student");
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var alice = new StudentId(Guid.NewGuid());

        // The SAME tag value is appended in two tenants; HasTag must only match the session's tenant.
        await using var redSession = theStore.LightweightSession(new SessionOptions { TenantId = "Red" });
        await AppendTagged(redSession, Guid.NewGuid(), new StudentEnrolled("Alice", "Math"), alice);

        await using var blueSession = theStore.LightweightSession(new SessionOptions { TenantId = "Blue" });
        await AppendTagged(blueSession, Guid.NewGuid(), new StudentEnrolled("Alice", "Science"), alice);

        var redEvents = await redSession.Events.QueryAllRawEvents()
            .Where(e => e.HasTag<StudentId>(alice))
            .ToListAsync();

        redEvents.Count.ShouldBe(1);
        redEvents.Single().Data.ShouldBeOfType<StudentEnrolled>().CourseName.ShouldBe("Math");
    }

    [Fact]
    public async Task has_tag_for_an_unregistered_tag_type_throws()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        var unknown = new UnregisteredTag(Guid.NewGuid());

        await Should.ThrowAsync<InvalidOperationException>(async () =>
        {
            await session.Events.QueryAllRawEvents()
                .Where(e => e.HasTag<UnregisteredTag>(unknown))
                .ToListAsync();
        });
    }

    public record UnregisteredTag(Guid Value);
}
