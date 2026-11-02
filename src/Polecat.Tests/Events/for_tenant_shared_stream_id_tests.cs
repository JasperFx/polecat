using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

public record SharedIdHappened(string Note);

public class SharedIdAggregate
{
    public Guid Id { get; set; }
    public List<string> Notes { get; set; } = [];

    public void Apply(SharedIdHappened e) => Notes.Add(e.Note);
}

public class SharedKeyAggregate
{
    public string Id { get; set; } = "";
    public List<string> Notes { get; set; } = [];

    public void Apply(SharedIdHappened e) => Notes.Add(e.Note);
}

public class SharedIdProjected
{
    public Guid Id { get; set; }
    public List<string> Notes { get; set; } = [];

    public void Apply(SharedIdHappened e) => Notes.Add(e.Note);
}

/// <summary>
///     #905: under conjoined tenancy a stream id is unique within a tenant and no further, and <c>ForTenant</c>
///     puts several tenants in one unit of work, so anything in that unit of work that finds a stream has
///     to find it by both. Finding it by id alone put one tenant's events in another tenant's stream, at
///     that tenant's versions, with no error of any kind.
/// </summary>
public abstract class for_tenant_shared_stream_id_context : IAsyncLifetime
{
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    protected abstract string Schema { get; }

    protected DocumentStore Store = null!;

    protected virtual StreamIdentity Identity => StreamIdentity.AsGuid;
    protected virtual void Configure(StoreOptions opts) => opts.Events.TenancyStyle = TenancyStyle.Conjoined;
    protected virtual Task CleanUpAsync() => Task.CompletedTask;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await TestSchema.DropSequencesAsync(Schema);
        await CleanUpAsync();

        Store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.StreamIdentity = Identity;
            opts.Events.AddEventType<SharedIdHappened>();
            Configure(opts);
        });

        await Store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    public ValueTask DisposeAsync()
    {
        Store.Dispose();
        return ValueTask.CompletedTask;
    }

    protected IDocumentSession SessionFor(string tenant)
        => Store.LightweightSession(new SessionOptions { TenantId = tenant });

    protected async Task<IReadOnlyList<(long version, string note)>> ReadAsync(string tenant, Guid id)
    {
        await using var query = Store.QuerySession(new SessionOptions { TenantId = tenant });
        var events = await query.Events.FetchStreamAsync(id, token: Ct);
        return events.Select(x => (x.Version, ((SharedIdHappened)x.Data).Note)).ToList();
    }

    protected async Task<IReadOnlyList<(long version, string note)>> ReadAsync(string tenant, string key)
    {
        await using var query = Store.QuerySession(new SessionOptions { TenantId = tenant });
        var events = await query.Events.FetchStreamAsync(key, token: Ct);
        return events.Select(x => (x.Version, ((SharedIdHappened)x.Data).Note)).ToList();
    }

    protected async Task<long?> StreamVersionAsync(string tenant, object id)
    {
        await using var query = Store.QuerySession(new SessionOptions { TenantId = tenant });
        var state = id is Guid guid
            ? await query.Events.FetchStreamStateAsync(guid, Ct)
            : await query.Events.FetchStreamStateAsync((string)id, Ct);
        return state?.Version;
    }
}

public class for_tenant_shared_guid_stream_id_tests : for_tenant_shared_stream_id_context
{
    protected override string Schema => "for_tenant_guid_id";

    [Fact]
    public async Task two_tenants_starting_from_an_append_get_a_stream_each()
    {
        var id = Guid.NewGuid();

        await using (var session = SessionFor("root"))
        {
            session.ForTenant("red").Events.Append(id, new SharedIdHappened("red 1"));
            session.ForTenant("blue").Events.Append(id, new SharedIdHappened("blue 1"));
            await session.SaveChangesAsync(Ct);
        }

        (await ReadAsync("red", id)).ShouldBe([(1L, "red 1")]);
        (await ReadAsync("blue", id)).ShouldBe([(1L, "blue 1")]);
    }

    [Fact]
    public async Task the_reported_shape_with_two_appends_for_one_tenant()
    {
        var id = Guid.NewGuid();

        await using (var session = SessionFor("root"))
        {
            session.ForTenant("red").Events.Append(id, new SharedIdHappened("red 1"), new SharedIdHappened("red 2"));
            session.ForTenant("blue").Events.Append(id, new SharedIdHappened("blue 1"));
            session.ForTenant("red").Events.Append(id, new SharedIdHappened("red 3"));
            await session.SaveChangesAsync(Ct);
        }

        (await ReadAsync("red", id)).ShouldBe([(1L, "red 1"), (2L, "red 2"), (3L, "red 3")]);
        (await ReadAsync("blue", id)).ShouldBe([(1L, "blue 1")]);
        (await StreamVersionAsync("red", id)).ShouldBe(3);
        (await StreamVersionAsync("blue", id)).ShouldBe(1);
    }

    [Fact]
    public async Task the_sessions_own_tenant_and_a_for_tenant_view()
    {
        var id = Guid.NewGuid();

        await using (var session = SessionFor("red"))
        {
            session.Events.Append(id, new SharedIdHappened("red 1"));
            session.ForTenant("blue").Events.Append(id, new SharedIdHappened("blue 1"));
            session.Events.Append(id, new SharedIdHappened("red 2"));
            await session.SaveChangesAsync(Ct);
        }

        (await ReadAsync("red", id)).ShouldBe([(1L, "red 1"), (2L, "red 2")]);
        (await ReadAsync("blue", id)).ShouldBe([(1L, "blue 1")]);
    }

    [Fact]
    public async Task an_existing_stream_in_each_tenant_keeps_its_own_version()
    {
        var id = Guid.NewGuid();

        await using (var red = SessionFor("red"))
        {
            red.Events.StartStream(id, new SharedIdHappened("red 1"), new SharedIdHappened("red 2"),
                new SharedIdHappened("red 3"));
            await red.SaveChangesAsync(Ct);
        }

        await using (var blue = SessionFor("blue"))
        {
            blue.Events.StartStream(id, new SharedIdHappened("blue 1"));
            await blue.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor("root"))
        {
            session.ForTenant("red").Events.Append(id, new SharedIdHappened("red 4"));
            session.ForTenant("blue").Events.Append(id, new SharedIdHappened("blue 2"));
            await session.SaveChangesAsync(Ct);
        }

        (await ReadAsync("red", id)).Select(x => x.version).ShouldBe([1L, 2L, 3L, 4L]);
        (await ReadAsync("blue", id)).ShouldBe([(1L, "blue 1"), (2L, "blue 2")]);
    }

    [Fact]
    public async Task an_expected_version_is_only_asserted_against_its_own_tenant()
    {
        var id = Guid.NewGuid();

        await using (var red = SessionFor("red"))
        {
            red.Events.StartStream(id, new SharedIdHappened("red 1"), new SharedIdHappened("red 2"));
            await red.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor("root"))
        {
            // Counted against the action it lands on: with blue's event in red's stream this came out
            // negative and threw for a tenant that had done nothing wrong
            session.ForTenant("red").Events.Append(id, 3, new SharedIdHappened("red 3"));
            session.ForTenant("blue").Events.Append(id, new SharedIdHappened("blue 1"));
            session.ForTenant("blue").Events.Append(id, 2, new SharedIdHappened("blue 2"));
            await session.SaveChangesAsync(Ct);
        }

        (await ReadAsync("red", id)).Select(x => x.version).ShouldBe([1L, 2L, 3L]);
        (await ReadAsync("blue", id)).ShouldBe([(1L, "blue 1"), (2L, "blue 2")]);
    }

    [Fact]
    public async Task a_start_for_one_tenant_and_an_append_for_another()
    {
        var id = Guid.NewGuid();

        await using (var session = SessionFor("root"))
        {
            session.ForTenant("red").Events.StartStream(id, new SharedIdHappened("red 1"));
            session.ForTenant("blue").Events.Append(id, new SharedIdHappened("blue 1"));
            await session.SaveChangesAsync(Ct);
        }

        (await ReadAsync("red", id)).ShouldBe([(1L, "red 1")]);
        (await ReadAsync("blue", id)).ShouldBe([(1L, "blue 1")]);
    }

    [Fact]
    public async Task one_tenant_starting_and_appending_is_still_one_stream()
    {
        var id = Guid.NewGuid();

        await using (var session = SessionFor("root"))
        {
            var red = session.ForTenant("red");
            var started = red.Events.StartStream(id, new SharedIdHappened("red 1"));
            red.Events.Append(id, new SharedIdHappened("red 2")).ShouldBeSameAs(started);
            session.PendingChanges.Streams.Count.ShouldBe(1);
            await session.SaveChangesAsync(Ct);
        }

        (await ReadAsync("red", id)).ShouldBe([(1L, "red 1"), (2L, "red 2")]);
    }

    [Fact]
    public async Task reading_through_for_tenant_reads_that_tenant()
    {
        var id = Guid.NewGuid();

        await using (var red = SessionFor("red"))
        {
            red.Events.StartStream(id, new SharedIdHappened("red 1"), new SharedIdHappened("red 2"));
            await red.SaveChangesAsync(Ct);
        }

        await using (var blue = SessionFor("blue"))
        {
            blue.Events.StartStream(id, new SharedIdHappened("blue 1"));
            await blue.SaveChangesAsync(Ct);
        }

        await using var session = SessionFor("red");
        var view = session.ForTenant("blue").Events;

        (await view.FetchStreamAsync(id, token: Ct)).Select(x => ((SharedIdHappened)x.Data).Note)
            .ShouldBe(["blue 1"]);
        (await view.FetchStreamStateAsync(id, Ct))!.Version.ShouldBe(1);
        (await view.AggregateStreamAsync<SharedIdAggregate>(id, token: Ct))!.Notes.ShouldBe(["blue 1"]);
    }

    [Fact]
    public async Task fetch_for_writing_through_for_tenant_folds_that_tenants_events()
    {
        var id = Guid.NewGuid();

        await using (var red = SessionFor("red"))
        {
            red.Events.StartStream(id, new SharedIdHappened("red 1"), new SharedIdHappened("red 2"));
            await red.SaveChangesAsync(Ct);
        }

        await using (var blue = SessionFor("blue"))
        {
            blue.Events.StartStream(id, new SharedIdHappened("blue 1"));
            await blue.SaveChangesAsync(Ct);
        }

        await using var session = SessionFor("root");
        var forRed = await session.ForTenant("red").Events.FetchForWriting<SharedIdAggregate>(id, Ct);
        var forBlue = await session.ForTenant("blue").Events.FetchForWriting<SharedIdAggregate>(id, Ct);

        forRed.Aggregate!.Notes.ShouldBe(["red 1", "red 2"]);
        forBlue.Aggregate!.Notes.ShouldBe(["blue 1"]);

        forRed.AppendOne(new SharedIdHappened("red 3"));
        forBlue.AppendOne(new SharedIdHappened("blue 2"));
        await session.SaveChangesAsync(Ct);

        (await ReadAsync("red", id)).Select(x => x.version).ShouldBe([1L, 2L, 3L]);
        (await ReadAsync("blue", id)).ShouldBe([(1L, "blue 1"), (2L, "blue 2")]);
    }

    [Fact]
    public async Task project_latest_only_folds_pending_events_of_its_own_tenant()
    {
        var id = Guid.NewGuid();

        await using var session = SessionFor("root");
        session.ForTenant("red").Events.Append(id, new SharedIdHappened("red 1"));

        // Nothing is pending for blue, so what comes back for blue must not contain red's event
        var blue = await session.ForTenant("blue").Events.ProjectLatest<SharedIdAggregate>(id, Ct);
        blue.ShouldBeNull();

        session.ForTenant("blue").Events.Append(id, new SharedIdHappened("blue 1"));

        blue = await session.ForTenant("blue").Events.ProjectLatest<SharedIdAggregate>(id, Ct);
        blue!.Notes.ShouldBe(["blue 1"]);

        var red = await session.ForTenant("red").Events.ProjectLatest<SharedIdAggregate>(id, Ct);
        red!.Notes.ShouldBe(["red 1"]);
    }
}

public class for_tenant_shared_string_stream_id_tests : for_tenant_shared_stream_id_context
{
    protected override string Schema => "for_tenant_string_id";
    protected override StreamIdentity Identity => StreamIdentity.AsString;

    [Fact]
    public async Task two_tenants_appending_to_one_key_get_a_stream_each()
    {
        var key = Guid.NewGuid().ToString();

        await using (var session = SessionFor("root"))
        {
            session.ForTenant("red").Events.Append(key, new SharedIdHappened("red 1"), new SharedIdHappened("red 2"));
            session.ForTenant("blue").Events.Append(key, new SharedIdHappened("blue 1"));
            session.ForTenant("red").Events.Append(key, new SharedIdHappened("red 3"));
            await session.SaveChangesAsync(Ct);
        }

        (await ReadAsync("red", key)).ShouldBe([(1L, "red 1"), (2L, "red 2"), (3L, "red 3")]);
        (await ReadAsync("blue", key)).ShouldBe([(1L, "blue 1")]);
        (await StreamVersionAsync("red", key)).ShouldBe(3);
        (await StreamVersionAsync("blue", key)).ShouldBe(1);
    }

    [Fact]
    public async Task project_latest_only_folds_pending_events_of_its_own_tenant()
    {
        var key = Guid.NewGuid().ToString();

        await using var session = SessionFor("root");
        session.ForTenant("red").Events.Append(key, new SharedIdHappened("red 1"));

        (await session.ForTenant("blue").Events.ProjectLatest<SharedKeyAggregate>(key, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task reading_through_for_tenant_reads_that_tenant()
    {
        var key = Guid.NewGuid().ToString();

        await using (var red = SessionFor("red"))
        {
            red.Events.StartStream(key, new SharedIdHappened("red 1"));
            await red.SaveChangesAsync(Ct);
        }

        await using (var blue = SessionFor("blue"))
        {
            blue.Events.StartStream(key, new SharedIdHappened("blue 1"));
            await blue.SaveChangesAsync(Ct);
        }

        await using var session = SessionFor("red");
        (await session.ForTenant("blue").Events.FetchStreamAsync(key, token: Ct))
            .Select(x => ((SharedIdHappened)x.Data).Note).ShouldBe(["blue 1"]);
    }
}

/// <summary>
///     The rest of what a <c>ForTenant</c> view hands back. All of it ran as the session's tenant, because the
///     view shares the session and the reads asked the session who it was.
/// </summary>
public class for_tenant_event_view_scope_tests : for_tenant_shared_stream_id_context
{
    protected override string Schema => "for_tenant_view_scope";

    protected override void Configure(StoreOptions opts)
    {
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.Projections.UseIdentityMapForAggregates = true;
        opts.Projections.Snapshot<SharedIdProjected>(SnapshotLifecycle.Inline);
        opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
    }

    private async Task<Guid> SharedStreamAsync()
    {
        var id = Guid.NewGuid();

        await using (var red = SessionFor("red"))
        {
            red.Events.StartStream(id, new SharedIdHappened("red 1"), new SharedIdHappened("red 2"),
                new SharedIdHappened("red 3"));
            await red.SaveChangesAsync(Ct);
        }

        await using (var blue = SessionFor("blue"))
        {
            blue.Events.StartStream(id, new SharedIdHappened("blue 1"), new SharedIdHappened("blue 2"));
            await blue.SaveChangesAsync(Ct);
        }

        return id;
    }

    [Fact]
    public async Task event_queries_are_scoped_to_the_view()
    {
        var id = await SharedStreamAsync();

        await using var session = SessionFor("red");
        var view = session.ForTenant("blue").Events;

        (await view.QueryAllRawEvents().Where(x => x.StreamId == id).ToListAsync(Ct)).Count.ShouldBe(2);
        (await view.QueryRawEventDataOnly<SharedIdHappened>().ToListAsync(Ct))
            .Select(x => x.Note).ShouldAllBe(x => x.StartsWith("blue"));
    }

    [Fact]
    public async Task fetch_latest_of_an_inline_aggregate_loads_the_views_document()
    {
        var id = await SharedStreamAsync();

        await using var session = SessionFor("red");
        (await session.ForTenant("blue").Events.FetchLatest<SharedIdProjected>(id, Ct))!.Notes
            .ShouldBe(["blue 1", "blue 2"]);
        (await session.Events.FetchLatest<SharedIdProjected>(id, Ct))!.Notes
            .ShouldBe(["red 1", "red 2", "red 3"]);
    }

    [Fact]
    public async Task the_aggregate_identity_map_does_not_cross_tenants()
    {
        var id = await SharedStreamAsync();

        await using var session = SessionFor("red");

        // Fills the session's map, keyed by id alone
        await session.Events.FetchForWriting<SharedIdAggregate>(id, Ct);

        (await session.ForTenant("blue").Events.FetchLatest<SharedIdAggregate>(id, Ct))!.Notes
            .ShouldBe(["blue 1", "blue 2"]);

        await session.ForTenant("blue").Events.FetchForWriting<SharedIdAggregate>(id, Ct);

        (await session.Events.FetchLatest<SharedIdAggregate>(id, Ct))!.Notes
            .ShouldBe(["red 1", "red 2", "red 3"]);
    }

    [Fact]
    public async Task fetch_for_writing_by_natural_key_folds_the_views_events()
    {
        var id = Guid.NewGuid();
        var number = new OrderNumber($"ORD-{id}");

        await using (var red = SessionFor("red"))
        {
            red.Events.StartStream<OrderAggregate>(id, new NkOrderCreated(number, "Alice"));
            await red.SaveChangesAsync(Ct);
        }

        await using (var blue = SessionFor("blue"))
        {
            blue.Events.StartStream<OrderAggregate>(id, new NkOrderCreated(number, "Bob"));
            await blue.SaveChangesAsync(Ct);
        }

        await using var session = SessionFor("red");
        var stream = await session.ForTenant("blue").Events.FetchForWriting<OrderAggregate, OrderNumber>(number, Ct);

        stream.Aggregate!.CustomerName.ShouldBe("Bob");
    }

    [Fact]
    public async Task compacting_through_a_view_compacts_the_views_stream()
    {
        var id = await SharedStreamAsync();

        await using (var session = SessionFor("red"))
        {
            await session.ForTenant("blue").Events.CompactStreamAsync<SharedIdAggregate>(id);
            await session.SaveChangesAsync(Ct);
        }

        // Deleting is why this one matters most: it took the three events the view had no business reaching
        (await ReadRawCountAsync("red", id)).ShouldBe(3);
        (await ReadRawCountAsync("blue", id)).ShouldBe(1);
    }

    private async Task<int> ReadRawCountAsync(string tenant, Guid id)
    {
        await using var query = Store.QuerySession(new SessionOptions { TenantId = tenant });
        return (await query.Events.FetchStreamAsync(id, token: Ct)).Count;
    }
}

/// <summary>
///     A tag boundary routes its events to a stream named by the tag, so it finds a pending stream by id
///     the same way an append does, and it is one more thing <c>ForTenant</c> hands back.
/// </summary>
public class for_tenant_shared_stream_id_with_a_tag_boundary_tests : for_tenant_shared_stream_id_context
{
    protected override string Schema => "for_tenant_dcb_id";

    protected override void Configure(StoreOptions opts)
    {
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.Events.RegisterTagType<StudentId>("student").ForAggregate<StudentCourseEnrollment>();
        opts.Events.RegisterTagType<CourseId>("course").ForAggregate<StudentCourseEnrollment>();
    }

    [Fact]
    public async Task a_boundary_taken_through_for_tenant_writes_to_that_tenant()
    {
        var student = new StudentId(Guid.NewGuid());
        var query = new EventTagQuery().Or<StudentId>(student);

        await using (var session = SessionFor("red"))
        {
            var boundary = await session.ForTenant("blue").Events
                .FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);

            var enrolled = session.Events.BuildEvent(new StudentEnrolled("Bob", "Science"));
            enrolled.WithTag(student);
            boundary.AppendOne(enrolled);

            await session.SaveChangesAsync(Ct);
        }

        (await StreamVersionAsync("red", student.Value)).ShouldBeNull();
        (await StreamVersionAsync("blue", student.Value)).ShouldBe(1);
    }

    [Fact]
    public async Task a_boundary_does_not_join_another_tenants_pending_stream()
    {
        var student = new StudentId(Guid.NewGuid());
        var query = new EventTagQuery().Or<StudentId>(student);

        await using (var session = SessionFor("root"))
        {
            session.ForTenant("red").Events.Append(student.Value, new SharedIdHappened("red 1"));

            var boundary = await session.ForTenant("blue").Events
                .FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);

            var enrolled = session.Events.BuildEvent(new StudentEnrolled("Bob", "Science"));
            enrolled.WithTag(student);
            boundary.AppendOne(enrolled);

            await session.SaveChangesAsync(Ct);
        }

        (await StreamVersionAsync("red", student.Value)).ShouldBe(1);
        (await StreamVersionAsync("blue", student.Value)).ShouldBe(1);
    }
}

/// <summary>
///     Without conjoined tenancy the id is the whole key of pc_streams, so there is one stream however many
///     tenants name it, and telling them apart would turn a save that works into a primary key violation.
/// </summary>
public class for_tenant_stream_id_without_conjoined_tenancy_tests : for_tenant_shared_stream_id_context
{
    protected override string Schema => "for_tenant_single_id";
    protected override void Configure(StoreOptions opts) { }

    [Fact]
    public async Task two_views_on_one_id_are_one_stream()
    {
        var id = Guid.NewGuid();

        await using (var session = SessionFor("root"))
        {
            session.ForTenant("red").Events.Append(id, new SharedIdHappened("one"));
            session.ForTenant("blue").Events.Append(id, new SharedIdHappened("two"));
            session.PendingChanges.Streams.Count.ShouldBe(1);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = Store.QuerySession(new SessionOptions { TenantId = "red" });
        (await query.Events.FetchStreamAsync(id, token: Ct)).Select(x => x.Version).ShouldBe([1L, 2L]);
    }
}

[Collection("tenant-partitioning")]
public class for_tenant_shared_stream_id_with_partitioned_events_tests : for_tenant_shared_stream_id_context
{
    protected override string Schema => "for_tenant_parts_id";

    protected override void Configure(StoreOptions opts)
    {
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
    }

    protected override Task CleanUpAsync() => PartitionTestCleanup.DropEventsPartitionObjectsAsync();

    [Fact]
    public async Task two_tenants_appending_to_one_id_land_in_their_own_partitions()
    {
        var id = Guid.NewGuid();

        await using (var session = SessionFor("root"))
        {
            session.ForTenant("red").Events.Append(id, new SharedIdHappened("red 1"), new SharedIdHappened("red 2"));
            session.ForTenant("blue").Events.Append(id, new SharedIdHappened("blue 1"));
            session.ForTenant("red").Events.Append(id, new SharedIdHappened("red 3"));
            await session.SaveChangesAsync(Ct);
        }

        (await ReadAsync("red", id)).ShouldBe([(1L, "red 1"), (2L, "red 2"), (3L, "red 3")]);
        (await ReadAsync("blue", id)).ShouldBe([(1L, "blue 1")]);

        var rows = await TestSchema.QueryAsync($"""
            SELECT tenant_id, tenant_ordinal, version FROM {Schema}.pc_streams WHERE id = '{id}' ORDER BY tenant_id
            """);

        rows.Count.ShouldBe(2);
        rows[0][0].ShouldBe("blue");
        rows[0][2].ShouldBe(1L);
        rows[1][0].ShouldBe("red");
        rows[1][2].ShouldBe(3L);
        rows[0][1].ShouldNotBe(rows[1][1]);
    }
}

/// <summary>
///     The same save seen from the two things downstream of it: an inline projection, which is handed the
///     streams in the transaction, and the async daemon, which only ever sees the rows.
/// </summary>
public class for_tenant_shared_stream_id_projection_tests : for_tenant_shared_stream_id_context
{
    protected override string Schema => "for_tenant_proj_id";

    protected override void Configure(StoreOptions opts)
    {
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.Projections.Snapshot<SharedIdProjected>(SnapshotLifecycle.Inline);
        opts.Projections.Snapshot<SharedIdAggregate>(SnapshotLifecycle.Async);
    }

    [Fact]
    public async Task an_inline_projection_and_the_daemon_each_see_two_streams()
    {
        var id = Guid.NewGuid();

        await using (var session = SessionFor("root"))
        {
            session.ForTenant("red").Events.Append(id, new SharedIdHappened("red 1"), new SharedIdHappened("red 2"));
            session.ForTenant("blue").Events.Append(id, new SharedIdHappened("blue 1"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var red = Store.QuerySession(new SessionOptions { TenantId = "red" }))
        await using (var blue = Store.QuerySession(new SessionOptions { TenantId = "blue" }))
        {
            (await red.LoadAsync<SharedIdProjected>(id, Ct))!.Notes.ShouldBe(["red 1", "red 2"]);
            (await blue.LoadAsync<SharedIdProjected>(id, Ct))!.Notes.ShouldBe(["blue 1"]);
        }

        using (var daemon = (IProjectionDaemon)await Store.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();
            await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(60));
        }

        await using (var red = Store.QuerySession(new SessionOptions { TenantId = "red" }))
        await using (var blue = Store.QuerySession(new SessionOptions { TenantId = "blue" }))
        {
            (await red.LoadAsync<SharedIdAggregate>(id, Ct))!.Notes.ShouldBe(["red 1", "red 2"]);
            (await blue.LoadAsync<SharedIdAggregate>(id, Ct))!.Notes.ShouldBe(["blue 1"]);
        }
    }
}
