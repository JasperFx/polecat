using System.Data;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Tests.Harness;
using Polecat.Tests.Session;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

public record UnseenTenantHappened(int Number);

/// <summary>
///     #864: the first write a tenant ever makes under <c>UseTenantPartitionedEvents</c>. Provisioning that
///     tenant SPLITs the partition functions on a connection of its own, and SPLIT needs a
///     schema-modification lock on <c>pc_streams</c>, so it can never run behind a lock the same session
///     holds on that table.
/// </summary>
/// <remarks>
///     What goes wrong is not only that the save stalls for the 30 second command timeout. Weasel isolates
///     a failed SPLIT per table and carries on, so the save then succeeds, with the stream row in the
///     previous tenant's partition. Each of these therefore asserts where the rows are, which fails however
///     fast the machine is, and none of them measures how long anything took.
/// </remarks>
[Collection("tenant-partitioning")]
public class tenant_partitioned_unseen_tenant_tests : IAsyncLifetime
{
    private const string Schema = "pt_unseen";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore(Action<StoreOptions>? configure = null) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
        opts.Events.AddEventType<UnseenTenantHappened>();
        opts.Events.AddEventType<StudentEnrolled>();
        configure?.Invoke(opts);
    });

    private static async Task<DocumentStore> CreateProvisionedStoreAsync(Action<StoreOptions>? configure = null)
    {
        var store = CreateStore(configure);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return store;
    }

    private static IDocumentSession SessionFor(DocumentStore store, string tenant)
        => store.LightweightSession(new SessionOptions { TenantId = tenant });

    private static async Task<Guid> StartStreamAsync(DocumentStore store, string tenant)
    {
        var streamId = Guid.NewGuid();
        await using var session = SessionFor(store, tenant);
        session.Events.StartStream(streamId, new UnseenTenantHappened(0));
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private static async Task<int> OrdinalOfAsync(string tenant)
    {
        var rows = await TestSchema.QueryAsync(
            $"SELECT ordinal FROM [{Schema}].[pc_tenant_partitions] WHERE tenant_id = '{tenant}'");
        return (int)rows.Single()[0];
    }

    private static async Task<IReadOnlyList<int>> BoundariesOfAsync(string table)
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT CAST(v.value AS int)
            FROM sys.partition_range_values v
            JOIN sys.partition_functions f ON f.function_id = v.function_id
            WHERE f.name = 'pf_{table}_tenant_ordinal'
            """);
        return rows.Select(x => (int)x[0]).ToList();
    }

    /// <summary>
    ///     The boundary that opens the partition each of the tenant's rows is in. The functions are RANGE
    ///     RIGHT, so partition N starts at boundary N - 1, and a row is in its own tenant's partition
    ///     exactly when that boundary is the tenant's ordinal.
    /// </summary>
    private static async Task<IReadOnlyList<object>> PartitionStartsOfRowsAsync(string table, string tenant)
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT CAST(v.value AS int)
            FROM [{Schema}].[{table}] r
            LEFT JOIN sys.partition_range_values v
                ON v.boundary_id = $PARTITION.pf_{table}_tenant_ordinal(r.tenant_ordinal) - 1
               AND v.function_id = (SELECT function_id FROM sys.partition_functions
                                    WHERE name = 'pf_{table}_tenant_ordinal')
            WHERE r.tenant_id = '{tenant}'
            """);
        return rows.Select(x => x[0]).ToList();
    }

    private static async Task ShouldBeInItsOwnPartitionsAsync(string tenant, int streams, int events)
    {
        var ordinal = await OrdinalOfAsync(tenant);

        (await BoundariesOfAsync("pc_events")).ShouldContain(ordinal);
        (await BoundariesOfAsync("pc_streams")).ShouldContain(ordinal);

        (await PartitionStartsOfRowsAsync("pc_streams", tenant))
            .ShouldBe(Enumerable.Repeat<object>(ordinal, streams));
        (await PartitionStartsOfRowsAsync("pc_events", tenant))
            .ShouldBe(Enumerable.Repeat<object>(ordinal, events));
    }

    /// <summary>
    ///     One stream and several go through different locking reads, the point lookup and the OPENJSON
    ///     batch, and both ran ahead of the tenant.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task append_to_a_new_stream_splits_the_partition_and_lands_in_it(int streamCount)
    {
        using var store = await CreateProvisionedStoreAsync();
        await StartStreamAsync(store, "incumbent");

        var streamIds = Enumerable.Range(0, streamCount).Select(_ => Guid.NewGuid()).ToList();

        await using (var session = SessionFor(store, "newcomer"))
        {
            foreach (var streamId in streamIds)
            {
                session.Events.Append(streamId, new UnseenTenantHappened(1), new UnseenTenantHappened(2));
            }

            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("newcomer", streamCount, streamCount * 2);

        await using var query = store.QuerySession(new SessionOptions { TenantId = "newcomer" });
        foreach (var streamId in streamIds)
        {
            (await query.Events.FetchStreamAsync(streamId, token: Ct)).Count.ShouldBe(2);
        }
    }

    [Fact]
    public async Task append_as_the_first_thing_a_store_ever_does()
    {
        using var store = CreateStore();

        await using (var session = SessionFor(store, "newcomer"))
        {
            session.Events.Append(Guid.NewGuid(), new UnseenTenantHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("newcomer", 1, 1);
    }

    [Fact]
    public async Task a_tagged_append_to_a_new_stream()
    {
        using var store = await CreateProvisionedStoreAsync(opts =>
            opts.Events.RegisterTagType<CourseId>("pt_unseen_course").ForAggregate<StudentCourseEnrollment>());

        var course = new CourseId(Guid.NewGuid());

        await using (var session = SessionFor(store, "newcomer"))
        {
            var tagged = session.Events.BuildEvent(new StudentEnrolled("Ann", "Math"));
            tagged.WithTag(course);
            session.Events.Append(Guid.NewGuid(), tagged);
            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("newcomer", 1, 1);

        await using var reader = SessionFor(store, "newcomer");
        (await reader.Events.QueryByTagsAsync(new EventTagQuery().Or<CourseId>(course), Ct)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task an_append_under_a_boundary_fetched_for_a_tenant_with_nothing_in_it()
    {
        using var store = await CreateProvisionedStoreAsync(opts =>
            opts.Events.RegisterTagType<CourseId>("pt_unseen_course").ForAggregate<StudentCourseEnrollment>());

        var course = new CourseId(Guid.NewGuid());

        await using (var session = SessionFor(store, "newcomer"))
        {
            var boundary = await session.Events.FetchForWritingByTags<StudentCourseEnrollment>(
                new EventTagQuery().Or<CourseId>(course), Ct);
            boundary.Events.ShouldBeEmpty();

            var tagged = session.Events.BuildEvent(new StudentEnrolled("Ann", "Math"));
            tagged.WithTag(course);
            session.Events.Append(Guid.NewGuid(), tagged);
            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("newcomer", 1, 1);
    }

    /// <summary>
    ///     <c>StartStream</c> reads nothing of its own, which is why it never hit this alone. It did as soon
    ///     as anything else in the same save took the lock first, because the tenant it needed was not the
    ///     session's.
    /// </summary>
    [Fact]
    public async Task start_stream_for_another_tenant_in_a_save_that_appends()
    {
        using var store = await CreateProvisionedStoreAsync();
        var existing = await StartStreamAsync(store, "incumbent");

        var streamId = Guid.NewGuid();

        await using (var session = SessionFor(store, "incumbent"))
        {
            session.Events.Append(existing, new UnseenTenantHappened(1));
            session.ForTenant("newcomer").Events.StartStream(streamId, new UnseenTenantHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("newcomer", 1, 1);
    }

    /// <summary>
    ///     An exclusive fetch takes its lock and holds it from here to the save, so resolving the tenant at
    ///     the start of the save would still be too late.
    /// </summary>
    [Fact]
    public async Task fetch_for_exclusive_writing_of_a_new_stream()
    {
        using var store = await CreateProvisionedStoreAsync();
        await StartStreamAsync(store, "incumbent");

        var streamId = Guid.NewGuid();

        await using (var session = SessionFor(store, "newcomer"))
        {
            var stream = await session.Events.FetchForExclusiveWriting<StudentCourseEnrollment>(streamId, Ct);
            stream.AppendOne(new StudentEnrolled("Ann", "Math"));
            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("newcomer", 1, 1);
    }

    [Fact]
    public async Task fetch_for_exclusive_writing_through_a_tenant_view_of_the_session()
    {
        using var store = await CreateProvisionedStoreAsync();
        await StartStreamAsync(store, "incumbent");

        var streamId = Guid.NewGuid();

        await using (var session = SessionFor(store, "incumbent"))
        {
            var stream = await session.ForTenant("newcomer").Events
                .FetchForExclusiveWriting<StudentCourseEnrollment>(streamId, Ct);
            stream.AppendOne(new StudentEnrolled("Ann", "Math"));
            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("newcomer", 1, 1);
    }

    /// <summary>
    ///     These sessions begin their transaction when they open, so a plain <c>FetchForWriting</c> is
    ///     already inside it. Serializable and Repeatable Read keep the read's locks to the end of it, which
    ///     is what an exclusive fetch does on purpose. Snapshot keeps none, but its next read refuses a table
    ///     that had DDL since its first one, so the failure there is a thrown save, not a stall.
    /// </summary>
    [Theory]
    [InlineData(IsolationLevel.Serializable)]
    [InlineData(IsolationLevel.RepeatableRead)]
    [InlineData(IsolationLevel.Snapshot)]
    public async Task fetch_for_writing_of_a_new_stream_in_a_session_holding_its_transaction(IsolationLevel isolation)
    {
        await session_isolation_level_tests.AllowSnapshotIsolationAsync(Ct);

        using var store = await CreateProvisionedStoreAsync();
        await StartStreamAsync(store, "incumbent");

        var streamId = Guid.NewGuid();

        await using (var session = await store.OpenSessionAsync(new SessionOptions
                     {
                         TenantId = "newcomer", IsolationLevel = isolation
                     }, Ct))
        {
            var stream = await session.Events.FetchForWriting<StudentCourseEnrollment>(streamId, Ct);
            stream.AppendOne(new StudentEnrolled("Ann", "Math"));
            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("newcomer", 1, 1);
    }

    [Fact]
    public async Task fetch_many_for_writing_of_new_streams_in_a_serializable_session()
    {
        using var store = await CreateProvisionedStoreAsync();
        await StartStreamAsync(store, "incumbent");

        var streamIds = new[] { Guid.NewGuid(), Guid.NewGuid() };

        await using (var session = await store.OpenSessionAsync(new SessionOptions
                     {
                         TenantId = "newcomer", IsolationLevel = IsolationLevel.Serializable
                     }, Ct))
        {
            foreach (var stream in await session.Events.FetchManyForWriting<StudentCourseEnrollment>(streamIds, Ct))
            {
                stream.AppendOne(new StudentEnrolled("Ann", "Math"));
            }

            await session.SaveChangesAsync(Ct);
        }

        await ShouldBeInItsOwnPartitionsAsync("newcomer", 2, 2);
    }
}
