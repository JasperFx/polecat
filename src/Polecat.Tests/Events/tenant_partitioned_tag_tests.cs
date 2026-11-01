using JasperFx;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using JasperFx.Events.Tags;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon.Coordination;
using Polecat.Events.Dcb;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Weasel.Core;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Tests.Events;

/// <summary>
///     DCB tags and natural keys under <c>UseTenantPartitionedEvents</c> (#842). Every tenant numbers its
///     events from 1 there, so <c>seq_id</c> alone identifies nothing, and that is what these keep
///     testing: the same sequence numbers in two tenants, with a tag on only one of them.
/// </summary>
[Collection("tenant-partitioning")]
public class tenant_partitioned_tag_tests : IAsyncLifetime
{
    private const string Schema = "pt_tags";
    private const string TagTable = $"[{Schema}].[pc_event_tag_pt_course]";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async Task<DocumentStore> CreateStoreAsync(Action<StoreOptions>? configure = null)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Events.AddEventType<StudentEnrolled>();
            opts.Events.RegisterTagType<CourseId>("pt_course").ForAggregate<StudentCourseEnrollment>();
            configure?.Invoke(opts);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return store;
    }

    private static IDocumentSession SessionFor(DocumentStore store, string tenant)
        => store.LightweightSession(new SessionOptions { TenantId = tenant });

    private static async Task<Guid> AppendAsync(DocumentStore store, string tenant, params object[] tags)
    {
        var streamId = Guid.NewGuid();
        await using var session = SessionFor(store, tenant);
        var e = session.Events.BuildEvent(new StudentEnrolled(tenant, "Math"));
        e.WithTag(tags);
        session.Events.StartStream(streamId, e);
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private static async Task<int> CountAsync(string sql)
        => (int)(await TestSchema.QueryAsync(sql)).Single()[0];

    private static Task<int> TagRowsAsync(string tenant)
        => CountAsync($"SELECT COUNT(*) FROM {TagTable} WHERE tenant_id = '{tenant}'");

    private static async Task<long> SequenceOfOnlyEventAsync(DocumentStore store, string tenant, Guid streamId)
    {
        await using var session = SessionFor(store, tenant);
        return (await session.Events.FetchStreamAsync(streamId, token: Ct)).Single().Sequence;
    }

    [Fact]
    public async Task the_combination_can_be_provisioned_and_a_second_apply_changes_nothing()
    {
        using var store = await CreateStoreAsync();

        (await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct))
            .ShouldBe(SchemaPatchDifference.None);
        await store.Database.AssertDatabaseMatchesConfigurationAsync();
    }

    /// <summary>
    ///     The premise of everything below, and what a join on bare <c>seq_id</c> gets wrong: event 1
    ///     exists in both tenants, and the tag is only on one of them.
    /// </summary>
    [Fact]
    public async Task a_tag_read_finds_only_its_own_tenants_event_at_a_shared_sequence()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        var red = await AppendAsync(store, "red", course);
        var blue = await AppendAsync(store, "blue", new CourseId(Guid.NewGuid()));

        (await SequenceOfOnlyEventAsync(store, "red", red))
            .ShouldBe(await SequenceOfOnlyEventAsync(store, "blue", blue));

        var query = new EventTagQuery().Or<CourseId>(course);

        await using (var session = SessionFor(store, "red"))
        {
            (await session.Events.QueryByTagsAsync(query, Ct)).Single().TenantId.ShouldBe("red");
            (await session.Events.EventsExistAsync(query, Ct)).ShouldBeTrue();
        }

        await using (var session = SessionFor(store, "blue"))
        {
            (await session.Events.QueryByTagsAsync(query, Ct)).ShouldBeEmpty();
            (await session.Events.EventsExistAsync(query, Ct)).ShouldBeFalse();

            var batch = session.CreateBatchQuery();
            var exists = batch.EventsExist(query);
            var boundary = batch.FetchForWritingByTags<StudentCourseEnrollment>(query);
            await batch.Execute(Ct);

            (await exists).ShouldBeFalse();
            (await boundary).Events.ShouldBeEmpty();
        }
    }

    [Fact]
    public async Task an_event_type_condition_stays_in_its_tenant_too()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        await AppendAsync(store, "red", course);
        await AppendAsync(store, "blue", new CourseId(Guid.NewGuid()));

        var query = new EventTagQuery().Or<StudentEnrolled, CourseId>(course);

        await using var session = SessionFor(store, "blue");
        (await session.Events.QueryByTagsAsync(query, Ct)).ShouldBeEmpty();
        (await session.Events.EventsExistAsync(query, Ct)).ShouldBeFalse();
    }

    [Fact]
    public async Task the_same_tag_value_in_two_tenants_reads_back_per_tenant()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        await AppendAsync(store, "red", course);
        await AppendAsync(store, "blue", course);

        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var session = SessionFor(store, tenant);
            var events = await session.Events.QueryByTagsAsync(new EventTagQuery().Or<CourseId>(course), Ct);
            events.Single().TenantId.ShouldBe(tenant);
        }
    }

    [Fact]
    public async Task a_linq_tag_filter_stays_in_its_tenant()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        await AppendAsync(store, "red", course);
        await AppendAsync(store, "blue", new CourseId(Guid.NewGuid()));

        await using var session = SessionFor(store, "blue");
        (await session.Events.QueryAllRawEvents().Where(e => e.HasTag(course)).ToListAsync(Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task the_explorer_stays_in_its_tenant()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        await AppendAsync(store, "red", course);
        await AppendAsync(store, "blue", new CourseId(Guid.NewGuid()));

        var found = new List<EventRecord>();
        var tags = new Dictionary<string, string> { ["pt_course"] = course.Value.ToString() };
        await foreach (var e in ((IEventStore)store).QueryByTagsAsync(tags, "blue", Ct))
        {
            found.Add(e);
        }

        found.ShouldBeEmpty();
    }

    [Fact]
    public async Task a_boundary_works_and_detects_a_conflict()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());
        await AppendAsync(store, "red", course);

        var query = new EventTagQuery().Or<CourseId>(course);

        await using var session = SessionFor(store, "red");
        var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(query, Ct);
        boundary.Events.Count.ShouldBe(1);

        await AppendAsync(store, "red", course);

        var tagged = session.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        tagged.WithTag(course);
        session.Events.Append(Guid.NewGuid(), tagged);

        await Should.ThrowAsync<DcbConcurrencyException>(() => session.SaveChangesAsync(Ct));
    }

    /// <summary>
    ///     A tag row costs one parameter more here than anywhere else, for its ordinal, and #834's split
    ///     of a long append has to know: at 153 events to a slice, one uncounted parameter per tag is
    ///     past SQL Server's limit.
    /// </summary>
    [Fact]
    public async Task a_long_tagged_append_fits_the_parameter_limit()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        await using (var session = SessionFor(store, "red"))
        {
            var events = Enumerable.Range(0, 500).Select(i =>
            {
                var e = session.Events.BuildEvent(new StudentEnrolled($"student-{i}", "Math"));
                e.WithTag(course);
                return (object)e;
            }).ToArray();

            session.Events.StartStream(Guid.NewGuid(), events);
            await session.SaveChangesAsync(Ct);
        }

        (await TagRowsAsync("red")).ShouldBe(500);
    }

    [Fact]
    public async Task an_archive_conflicts_only_within_its_tenant()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());
        var red = await AppendAsync(store, "red", course);
        await AppendAsync(store, "blue", course);

        await using var session = SessionFor(store, "blue");
        await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
            new EventTagQuery().Or<CourseId>(course), Ct);

        await using (var archiver = SessionFor(store, "red"))
        {
            archiver.Events.ArchiveStream(red);
            await archiver.SaveChangesAsync(Ct);
        }

        var tagged = session.Events.BuildEvent(new StudentEnrolled("Bob", "Math"));
        tagged.WithTag(course);
        session.Events.Append(Guid.NewGuid(), tagged);

        await session.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task assign_tag_where_tags_only_the_sessions_tenant()
    {
        using var store = await CreateStoreAsync();

        await AppendAsync(store, "red", new CourseId(Guid.NewGuid()));
        await AppendAsync(store, "blue", new CourseId(Guid.NewGuid()));

        var assigned = new CourseId(Guid.NewGuid());

        await using (var session = SessionFor(store, "red"))
        {
            session.Events.AssignTagWhere(e => e.Version > 0, assigned);
            await session.SaveChangesAsync(Ct);
        }

        var query = new EventTagQuery().Or<CourseId>(assigned);

        await using (var session = SessionFor(store, "red"))
        {
            (await session.Events.QueryByTagsAsync(query, Ct)).Single().TenantId.ShouldBe("red");
        }

        await using (var session = SessionFor(store, "blue"))
        {
            (await session.Events.EventsExistAsync(query, Ct)).ShouldBeFalse();
        }
    }

    [Fact]
    public async Task tombstoning_takes_only_its_own_tenants_tag_rows()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        var red = await AppendAsync(store, "red", course);
        await AppendAsync(store, "blue", course);

        await using (var session = SessionFor(store, "red"))
        {
            session.Events.TombstoneStream(red);
            await session.SaveChangesAsync(Ct);
        }

        (await TagRowsAsync("red")).ShouldBe(0);
        (await TagRowsAsync("blue")).ShouldBe(1);
    }

    [Fact]
    public async Task compacting_takes_only_its_own_tenants_tag_rows()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());
        var streamId = Guid.NewGuid();

        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var session = SessionFor(store, tenant);
            var first = session.Events.BuildEvent(new StudentEnrolled("one", "Math"));
            first.WithTag(course);
            var second = session.Events.BuildEvent(new StudentEnrolled("two", "Math"));
            second.WithTag(course);
            session.Events.StartStream<StudentCourseEnrollment>(streamId, first, second);
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(store, "red"))
        {
            await session.Events.CompactStreamAsync<StudentCourseEnrollment>(streamId);
            await session.SaveChangesAsync(Ct);
        }

        (await TagRowsAsync("red")).ShouldBe(1);
        (await TagRowsAsync("blue")).ShouldBe(2);
    }

    [Fact]
    public async Task removing_a_tenant_with_delete_data_takes_its_tags_and_leaves_another_tenant_alone()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        await AppendAsync(store, "red", course);
        var blue = await AppendAsync(store, "blue", course);

        var boundaries = await PartitionBoundariesAsync();

        await store.Advanced.RemovePolecatManagedTenantsAsync(["red"], TenantDropBehavior.DeleteData, Ct);

        (await TagRowsAsync("red")).ShouldBe(0);
        (await TagRowsAsync("blue")).ShouldBe(1);

        // pc_dcb_tag_version holds the tag values as text, which a hard delete has to reach as well
        (await CountAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE tenant_id = 'red'"))
            .ShouldBe(0);
        (await CountAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE tenant_id = 'blue'"))
            .ShouldBe(1);

        // The foreign key must not have stopped the boundary going. Weasel logs a failed MERGE RANGE
        // and carries on, so nothing would have surfaced it.
        (await PartitionBoundariesAsync()).ShouldBe(boundaries - 1);

        await using var session = SessionFor(store, "blue");
        (await session.Events.FetchStreamAsync(blue, token: Ct)).Count.ShouldBe(1);
        (await session.Events.EventsExistAsync(new EventTagQuery().Or<CourseId>(course), Ct)).ShouldBeTrue();
    }

    [Fact]
    public async Task a_removed_tenant_can_come_back_and_be_tagged_again()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        await AppendAsync(store, "red", course);

        await store.Advanced.RemovePolecatManagedTenantsAsync(["red"], TenantDropBehavior.DeleteData, Ct);
        await store.Advanced.AddPolecatManagedTenantsAsync(Ct, "red");

        await using (var session = SessionFor(store, "red"))
        {
            (await session.Events.EventsExistAsync(new EventTagQuery().Or<CourseId>(course), Ct)).ShouldBeFalse();
        }

        await AppendAsync(store, "red", course);

        await using var after = SessionFor(store, "red");
        (await after.Events.QueryByTagsAsync(new EventTagQuery().Or<CourseId>(course), Ct)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task removing_a_tenant_while_retaining_data_keeps_its_tags()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        await AppendAsync(store, "red", course);

        await store.Advanced.RemovePolecatManagedTenantsAsync(["red"], TenantDropBehavior.RetainData, Ct);

        (await TagRowsAsync("red")).ShouldBe(1);
        (await CountAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_dcb_tag_version] WHERE tenant_id = 'red'"))
            .ShouldBe(1);
    }

    [Fact]
    public async Task deleting_a_tenants_data_without_removing_it_takes_its_tags()
    {
        using var store = await CreateStoreAsync();
        var course = new CourseId(Guid.NewGuid());

        await AppendAsync(store, "red", course);
        await AppendAsync(store, "blue", course);

        await store.Advanced.DeleteAllTenantDataAsync("red", Ct);

        (await TagRowsAsync("red")).ShouldBe(0);
        (await TagRowsAsync("blue")).ShouldBe(1);
    }

    private static Task<int> PartitionBoundariesAsync() => CountAsync("""
        SELECT COUNT(*) FROM sys.partition_range_values v
        JOIN sys.partition_functions f ON f.function_id = v.function_id
        WHERE f.name = 'pf_pc_events_tenant_ordinal'
        """);

    // ── natural keys ─────────────────────────────────────────────────────────────────────────────

    private static Task<DocumentStore> CreateNaturalKeyStoreAsync() => CreateStoreAsync(opts =>
        opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline));

    private static async Task StartOrderAsync(DocumentStore store, string tenant, string number)
    {
        await using var session = SessionFor(store, tenant);
        session.Events.StartStream(Guid.NewGuid(), new NkOrderCreated(new OrderNumber(number), "A"));
        await session.SaveChangesAsync(Ct);
    }

    private static Task<int> NaturalKeyRowsAsync(string tenant) => CountAsync(
        $"SELECT COUNT(*) FROM [{Schema}].[pc_natural_key_orderaggregate] WHERE tenant_id = '{tenant}'");

    [Fact]
    public async Task a_removed_tenants_natural_key_can_be_reused()
    {
        using var store = await CreateNaturalKeyStoreAsync();
        await store.Advanced.AddPolecatManagedTenantsAsync(Ct, "alpha");

        await StartOrderAsync(store, "alpha", "K");

        await store.Advanced.RemovePolecatManagedTenantsAsync(["alpha"], TenantDropBehavior.DeleteData, Ct);
        await store.Advanced.AddPolecatManagedTenantsAsync(Ct, "alpha");

        await StartOrderAsync(store, "alpha", "K");
    }

    [Fact]
    public async Task a_hard_delete_erases_the_natural_key_values_of_that_tenant_only()
    {
        using var store = await CreateNaturalKeyStoreAsync();

        await StartOrderAsync(store, "alpha", "K");
        await StartOrderAsync(store, "beta", "K");

        await store.Advanced.RemovePolecatManagedTenantsAsync(["alpha"], TenantDropBehavior.DeleteData, Ct);

        (await NaturalKeyRowsAsync("alpha")).ShouldBe(0);
        (await NaturalKeyRowsAsync("beta")).ShouldBe(1);
    }

    [Fact]
    public async Task retaining_a_tenants_data_keeps_its_natural_keys_with_the_streams_they_name()
    {
        using var store = await CreateNaturalKeyStoreAsync();

        await StartOrderAsync(store, "alpha", "K");

        await store.Advanced.RemovePolecatManagedTenantsAsync(["alpha"], TenantDropBehavior.RetainData, Ct);

        (await NaturalKeyRowsAsync("alpha")).ShouldBe(1);
    }

    /// <summary>
    ///     With bucketing, Weasel neither merges nor purges an ordinal another tenant still uses, so the
    ///     removed tenant's streams stay and its natural keys have to stay with them.
    /// </summary>
    [Fact]
    public async Task a_tenant_on_a_shared_ordinal_keeps_what_its_events_keep()
    {
        using var store = await CreateNaturalKeyStoreAsync();
        store.Events.TenantPartitionManager.AllowOrdinalSharing = true;

        await store.Advanced.AddPolecatManagedTenantsAsync(
            new Dictionary<string, int> { ["alpha"] = 7, ["beta"] = 7 }, Ct);

        await StartOrderAsync(store, "alpha", "K");

        await store.Advanced.RemovePolecatManagedTenantsAsync(["alpha"], TenantDropBehavior.DeleteData, Ct);

        (await CountAsync($"SELECT COUNT(*) FROM [{Schema}].[pc_events] WHERE tenant_id = 'alpha'")).ShouldBe(1);
        (await NaturalKeyRowsAsync("alpha")).ShouldBe(1);
    }

    // ── progression ──────────────────────────────────────────────────────────────────────────────

    private static Task<DocumentStore> CreateAsyncProjectionStoreAsync() => CreateStoreAsync(opts =>
    {
        opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
        opts.DaemonSettings.AsyncMode = DaemonMode.HotCold;
    });

    private static async Task StartQuestsAsync(DocumentStore store, string tenant, int count)
    {
        for (var i = 0; i < count; i++)
        {
            await using var session = SessionFor(store, tenant);
            session.Events.StartStream(Guid.NewGuid(),
                new QuestStarted($"{tenant}-{i}"), new MembersJoined(1, "Town", ["Hero"]));
            await session.SaveChangesAsync(Ct);
        }
    }

    private static async Task WaitForPartiesAsync(DocumentStore store, string tenant, int expected)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromSeconds(45));

        var parties = 0;
        try
        {
            while (true)
            {
                await using var query = store.QuerySession(tenant);
                parties = await query.Query<QuestParty>().CountAsync(cts.Token);
                if (parties >= expected) return;
                await Task.Delay(250, cts.Token);
            }
        }
        catch (OperationCanceledException) when (!Ct.IsCancellationRequested)
        {
            parties.ShouldBe(expected, $"tenant {tenant}'s events never reached the async projection");
        }
    }

    /// <summary>
    ///     A tenant's sequence is dropped with it and restarts at 1 if it comes back. A surviving
    ///     <c>HighWaterMark:{tenant}</c> row is then already past every new event, and they never project.
    ///     Driven through the real coordinator rather than asserted on the rows alone, because that
    ///     outcome is the bug and the rows are only how it happens.
    /// </summary>
    [Theory]
    [InlineData(TenantDropBehavior.DeleteData)]
    [InlineData(TenantDropBehavior.RetainData)]
    public async Task a_returning_tenants_new_events_reach_async_projections(TenantDropBehavior behavior)
    {
        using var store = await CreateAsyncProjectionStoreAsync();

        await StartQuestsAsync(store, "alpha", 3);
        await StartQuestsAsync(store, "beta", 2);

        var first = new ProjectionCoordinator(store, NullLoggerFactory.Instance);
        await first.StartAsync(Ct);
        try
        {
            await WaitForPartiesAsync(store, "alpha", 3);
            await WaitForPartiesAsync(store, "beta", 2);

            // The rows this is about, and that the daemon really did leave them behind it
            await WaitForProgressionAsync("alpha");
        }
        finally
        {
            await first.StopAsync(CancellationToken.None);
        }

        await store.Advanced.RemovePolecatManagedTenantsAsync(["alpha"], behavior, Ct);

        (await ProgressionRowsAsync("alpha")).ShouldBe(0);
        (await ProgressionRowsAsync("beta")).ShouldBeGreaterThan(0);

        await store.Advanced.AddPolecatManagedTenantsAsync(Ct, "alpha");
        await using (var deletion = SessionFor(store, "alpha"))
        {
            deletion.DeleteWhere<QuestParty>(x => true);
            await deletion.SaveChangesAsync(Ct);
        }

        await StartQuestsAsync(store, "alpha", 1);

        var second = new ProjectionCoordinator(store, NullLoggerFactory.Instance);
        await second.StartAsync(Ct);
        try
        {
            await WaitForPartiesAsync(store, "alpha", 1);
        }
        finally
        {
            await second.StopAsync(CancellationToken.None);
        }
    }

    private static Task<int> ProgressionRowsAsync(string tenant) => CountAsync(
        $"SELECT COUNT(*) FROM [{Schema}].[pc_event_progression] WHERE name LIKE '%:{tenant}'");

    private static async Task WaitForProgressionAsync(string tenant)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        while (await ProgressionRowsAsync(tenant) < 2)
        {
            await Task.Delay(250, cts.Token);
        }
    }
}
