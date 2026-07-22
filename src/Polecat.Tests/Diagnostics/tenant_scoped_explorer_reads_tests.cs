using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Tags;
using Polecat.Tests.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.ExplorerApi;

/// <summary>
///     #353 / jasperfx#555 — the second pair of explorer read paths jasperfx#503 left untenanted, now
///     tenant-scoped for a conjoined store: the DCB tag query (QueryByTagsAsync) and the event/metadata
///     query (EventQuery.TenantId). On a conjoined store the same tag value / the same event can exist
///     under two tenants, so an untenanted query reads an ambiguous cross-tenant union; these overloads
///     isolate each tenant's slice via a tenant_id predicate.
/// </summary>
public class tenant_scoped_explorer_reads_tests : OneOffConfigurationsContext
{
    private const string RedTenant = "Red";
    private const string BlueTenant = "Blue";

    private void ConfigureConjoinedStoreWithTags()
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.RegisterTagType<StudentId>("student")
                .ForAggregate<StudentCourseEnrollment>();
            opts.Events.RegisterTagType<CourseId>("course")
                .ForAggregate<StudentCourseEnrollment>();
        });
    }

    [Fact]
    public async Task query_by_tags_is_isolated_per_tenant_on_conjoined_store()
    {
        ConfigureConjoinedStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        // The SAME tag values are attached to an event under each tenant — the exact cross-tenant
        // ambiguity #353 closes for the DCB tag query.
        var studentId = new StudentId(Guid.NewGuid());
        var courseId = new CourseId(Guid.NewGuid());

        var redStream = Guid.NewGuid();
        var blueStream = Guid.NewGuid();

        await using (var red = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant }))
        {
            var e = red.Events.BuildEvent(new StudentEnrolled("Alice", "Math"));
            e.WithTag(studentId, courseId);
            red.Events.Append(redStream, e);
            await red.SaveChangesAsync();
        }

        await using (var blue = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant }))
        {
            var e = blue.Events.BuildEvent(new StudentEnrolled("Bob", "Science"));
            e.WithTag(studentId, courseId);
            blue.Events.Append(blueStream, e);
            await blue.SaveChangesAsync();
        }

        var explorer = (IEventStore)theStore;
        var tags = new Dictionary<string, string> { ["StudentId"] = studentId.Value.ToString() };

        var redEvents = new List<EventRecord>();
        await foreach (var e in explorer.QueryByTagsAsync(tags, RedTenant, CancellationToken.None))
            redEvents.Add(e);

        var blueEvents = new List<EventRecord>();
        await foreach (var e in explorer.QueryByTagsAsync(tags, BlueTenant, CancellationToken.None))
            blueEvents.Add(e);

        redEvents.Count.ShouldBe(1);
        redEvents.ShouldAllBe(e => e.TenantId == RedTenant);
        redEvents[0].StreamId.ShouldBe(redStream.ToString());

        blueEvents.Count.ShouldBe(1);
        blueEvents.ShouldAllBe(e => e.TenantId == BlueTenant);
        blueEvents[0].StreamId.ShouldBe(blueStream.ToString());
    }

    [Fact]
    public async Task query_events_honours_event_query_tenant_id_on_conjoined_store()
    {
        ConfigureConjoinedStoreWithTags();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await using (var red = theStore.LightweightSession(new SessionOptions { TenantId = RedTenant }))
        {
            red.Events.StartStream<StudentCourseEnrollment>(Guid.NewGuid(), new StudentEnrolled("Alice", "Math"));
            red.Events.StartStream<StudentCourseEnrollment>(Guid.NewGuid(), new StudentEnrolled("Amy", "Math"));
            await red.SaveChangesAsync();
        }

        await using (var blue = theStore.LightweightSession(new SessionOptions { TenantId = BlueTenant }))
        {
            blue.Events.StartStream<StudentCourseEnrollment>(Guid.NewGuid(), new StudentEnrolled("Bob", "Science"));
            await blue.SaveChangesAsync();
        }

        // EventQuery.TenantId scopes the read-store query. TenantIsOneOf overrides the implicit
        // session-tenant filter, so a default read store isolates whichever tenant the query names.
        var readStore = ((IEventStore)theStore).OpenReadOnlyEventStore();

        var redPage = await readStore.QueryEventsAsync(
            new EventQuery { TenantId = RedTenant, PageSize = 100 }, CancellationToken.None);
        redPage.TotalCount.ShouldBe(2);
        redPage.Events.ShouldAllBe(e => e.TenantId == RedTenant);

        var bluePage = await readStore.QueryEventsAsync(
            new EventQuery { TenantId = BlueTenant, PageSize = 100 }, CancellationToken.None);
        bluePage.TotalCount.ShouldBe(1);
        bluePage.Events.ShouldAllBe(e => e.TenantId == BlueTenant);
    }
}
