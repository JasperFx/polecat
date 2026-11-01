using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events;
using Polecat.Events.Dcb;
using Polecat.Linq.SqlGeneration;
using Polecat.Tests.Harness;
using Weasel.SqlServer;

namespace Polecat.Tests.Events;

/// <summary>
///     #834. A tag query bound one parameter per condition's value, and one more for its event type,
///     so a boundary naming a couple of thousand tags was refused. Measured on 5.47.0:
///     <c>QueryByTagsAsync</c> and <c>EventsExistAsync</c> failed past 2098 conditions, and
///     <c>FetchForWritingByTags</c> past 1048, because its version capture costs two parameters a tag.
/// </summary>
public class tag_queries_past_the_parameter_limit_tests : OneOffConfigurationsContext
{
    private const int Conditions = 5000;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public record Enrolled(string Student);

    public record Dropped(string Student);

    public record StudentTag(Guid Value);

    public record CourseTag(string Value);

    public record RoomTag(int Value);

    public record TermTag(long Value);

    private async Task UseStoreAsync(TenancyStyle tenancy = TenancyStyle.Single)
    {
        Configure(tenancy);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private void Configure(TenancyStyle tenancy = TenancyStyle.Single)
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = tenancy;
            opts.Events.AddEventType<Enrolled>();
            opts.Events.AddEventType<Dropped>();
            opts.Events.RegisterTagType<StudentTag>("student");
            opts.Events.RegisterTagType<CourseTag>("course");
            opts.Events.RegisterTagType<RoomTag>("room");
            opts.Events.RegisterTagType<TermTag>("term");
        });
    }

    private async Task AppendAsync(object data, object tag, string? tenantId = null)
    {
        await using var session = theStore.LightweightSession(new SessionOptions
        {
            TenantId = tenantId ?? StorageConstants.DefaultTenantId
        });

        // The params overload, because it tags by the runtime type. AddTag(object) would tag as object.
        var built = session.Events.BuildEvent(data);
        built.WithTag(new[] { tag });
        session.Events.StartStream(Guid.NewGuid(), built);
        await session.SaveChangesAsync(Ct);
    }

    private static EventTagQuery Decoys(int count)
    {
        var query = new EventTagQuery();
        for (var i = 0; i < count; i++) query.Or(new StudentTag(Guid.NewGuid()));
        return query;
    }

    [Fact]
    public async Task a_long_query_returns_what_it_names_in_sequence_order()
    {
        await UseStoreAsync();

        var first = new StudentTag(Guid.NewGuid());
        var second = new StudentTag(Guid.NewGuid());
        await AppendAsync(new Enrolled("first"), first);
        await AppendAsync(new Enrolled("unnamed"), new StudentTag(Guid.NewGuid()));
        await AppendAsync(new Enrolled("second"), second);

        var query = Decoys(Conditions).Or(second).Or(first);

        await using var session = theStore.LightweightSession();
        var events = await session.Events.QueryByTagsAsync(query, Ct);

        events.Select(x => ((Enrolled)x.Data).Student).ShouldBe(["first", "second"]);
    }

    [Fact]
    public async Task every_tag_value_type_unpacks_to_its_own_column_type()
    {
        await UseStoreAsync();

        var student = new StudentTag(Guid.NewGuid());
        await AppendAsync(new Enrolled("guid"), student);
        await AppendAsync(new Enrolled("string"), new CourseTag("Rhetoric"));
        await AppendAsync(new Enrolled("int"), new RoomTag(7));
        await AppendAsync(new Enrolled("long"), new TermTag(8_000_000_000));

        var query = Decoys(Conditions).Or(student);
        for (var i = 0; i < Conditions; i++)
        {
            query.Or(new CourseTag($"not-a-course-{i}"));
            query.Or(new RoomTag(1000 + i));
            query.Or(new TermTag(1000L + i));
        }

        query.Or(new CourseTag("Rhetoric")).Or(new RoomTag(7)).Or(new TermTag(8_000_000_000));

        await using var session = theStore.LightweightSession();
        var events = await session.Events.QueryByTagsAsync(query, Ct);

        events.Select(x => ((Enrolled)x.Data).Student).ShouldBe(["guid", "string", "int", "long"]);
    }

    /// <summary>A condition's event type is part of the condition, so a group cannot be merged across it.</summary>
    [Fact]
    public async Task an_event_type_filter_still_applies_to_only_its_own_conditions()
    {
        await UseStoreAsync();

        var onlyEnrollments = new StudentTag(Guid.NewGuid());
        var everything = new StudentTag(Guid.NewGuid());
        await AppendAsync(new Enrolled("wanted"), onlyEnrollments);
        await AppendAsync(new Dropped("unwanted"), onlyEnrollments);
        await AppendAsync(new Dropped("wanted too"), everything);

        var query = Decoys(Conditions).Or(everything).Or<Enrolled, StudentTag>(onlyEnrollments);
        for (var i = 0; i < Conditions; i++)
        {
            query.Or<Enrolled, StudentTag>(new StudentTag(Guid.NewGuid()));
        }

        await using var session = theStore.LightweightSession();
        var events = await session.Events.QueryByTagsAsync(query, Ct);

        events.Count.ShouldBe(2);
        events[0].Data.ShouldBe(new Enrolled("wanted"));
        events[1].Data.ShouldBe(new Dropped("wanted too"));
    }

    [Fact]
    public async Task a_long_existence_check_answers_both_ways()
    {
        await UseStoreAsync();

        var tag = new StudentTag(Guid.NewGuid());
        await AppendAsync(new Enrolled("present"), tag);

        await using var session = theStore.LightweightSession();
        (await session.Events.EventsExistAsync(Decoys(Conditions), Ct)).ShouldBeFalse();
        (await session.Events.EventsExistAsync(Decoys(Conditions).Or(tag), Ct)).ShouldBeTrue();
    }

    [Fact]
    public async Task a_long_existence_check_stays_inside_its_tenant()
    {
        await UseStoreAsync(TenancyStyle.Conjoined);

        var tag = new StudentTag(Guid.NewGuid());
        await AppendAsync(new Enrolled("present"), tag, "alpha");

        await using var alpha = theStore.LightweightSession(new SessionOptions { TenantId = "alpha" });
        (await alpha.Events.EventsExistAsync(Decoys(Conditions).Or(tag), Ct)).ShouldBeTrue();

        await using var beta = theStore.LightweightSession(new SessionOptions { TenantId = "beta" });
        (await beta.Events.EventsExistAsync(Decoys(Conditions).Or(tag), Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task the_batched_flavours_take_the_same_path()
    {
        await UseStoreAsync();

        var tag = new StudentTag(Guid.NewGuid());
        await AppendAsync(new Enrolled("present"), tag);

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();

        var exists = batch.EventsExist(Decoys(Conditions).Or(tag));
        var missing = batch.EventsExist(Decoys(Conditions));
        var boundary = batch.FetchForWritingByTags<EnrollmentCount>(Decoys(Conditions).Or(tag));

        await batch.Execute(Ct);

        (await exists).ShouldBeTrue();
        (await missing).ShouldBeFalse();
        (await boundary).Events.ShouldHaveSingleItem();
    }

    public class EnrollmentCount
    {
        public Guid Id { get; set; }
        public int Count { get; set; }

        public void Apply(Enrolled _) => Count++;
    }

    /// <summary>
    ///     What the capture is for: a large boundary still guards every tag it names, including one
    ///     that came back through a JSON array rather than its own parameter.
    /// </summary>
    [Fact]
    public async Task a_long_boundary_still_catches_a_conflict_on_any_of_its_tags()
    {
        await UseStoreAsync();

        var tag = new StudentTag(Guid.NewGuid());
        await AppendAsync(new Enrolled("seed"), tag);

        await using var session = theStore.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<EnrollmentCount>(Decoys(Conditions).Or(tag), Ct);
        boundary.Aggregate!.Count.ShouldBe(1);

        await AppendAsync(new Enrolled("concurrent"), tag);

        var built = session.Events.BuildEvent(new Enrolled("mine"));
        built.AddTag(tag);
        boundary.AppendOne(built);

        await Should.ThrowAsync<DcbConcurrencyException>(session.SaveChangesAsync(Ct));
    }

    [Fact]
    public async Task a_long_boundary_commits_when_nothing_moved()
    {
        await UseStoreAsync();

        var tag = new StudentTag(Guid.NewGuid());
        await AppendAsync(new Enrolled("seed"), tag);

        await using (var session = theStore.LightweightSession())
        {
            var boundary = await session.Events.FetchForWritingByTags<EnrollmentCount>(
                Decoys(Conditions).Or(tag), Ct);

            var built = session.Events.BuildEvent(new Enrolled("mine"));
            built.AddTag(tag);
            boundary.AppendOne(built);

            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.LightweightSession();
        (await query.Events.QueryByTagsAsync(new EventTagQuery().Or(tag), Ct)).Count.ShouldBe(2);
    }

    // ---- the rendered shape ---------------------------------------------------------------------

    private string Render(EventTagQuery query)
    {
        var builder = new BatchBuilder();
        EventOperations.WriteTagQuerySql(builder, theStore.Options.EventGraph, query);
        return builder.Compile().BatchCommands[0].CommandText;
    }

    [Fact]
    public void a_short_query_keeps_its_per_condition_predicate()
    {
        Configure();

        var sql = Render(Decoys(3));

        sql.ShouldContain("t0.value = @");
        sql.ShouldNotContain("OPENJSON");
    }

    [Fact]
    public void a_long_query_is_one_parameter_per_tag_type_and_event_type()
    {
        Configure();

        var query = Decoys(Conditions).Or(new CourseTag("Rhetoric"));
        for (var i = 0; i < Conditions; i++)
        {
            query.Or<Enrolled, StudentTag>(new StudentTag(Guid.NewGuid()));
        }

        var builder = new BatchBuilder();
        EventOperations.WriteTagQuerySql(builder, theStore.Options.EventGraph, query);
        var command = builder.Compile().BatchCommands[0];

        // Three JSON arrays, and the one event type name.
        command.Parameters.Count.ShouldBe(4);
        command.Parameters.Count.ShouldBeLessThan(JsonValueList.Budget);
    }

    [Fact]
    public void the_capture_groups_its_targets_by_tag_table()
    {
        Configure();

        var targets = Enumerable.Range(0, Conditions)
            .Select(i => (TagTable: i % 2 == 0 ? "student" : "course", TagValue: $"value-{i}"))
            .ToList();

        var builder = new BatchBuilder();
        DcbTagVersionCapture.WriteSql(builder, theStore.Options.EventGraph, targets, "tenant");
        var command = builder.Compile().BatchCommands[0];

        // The tenant, and a table name and a JSON array for each of the two tag tables.
        command.Parameters.Count.ShouldBe(5);
    }
}
