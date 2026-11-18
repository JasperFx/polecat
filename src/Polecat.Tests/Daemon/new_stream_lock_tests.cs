using System.Data.SqlTypes;
using JasperFx;
using JasperFx.Events;

namespace Polecat.Tests.Daemon;

public sealed class NewStreamLockSingleTenantDatabases : HighWaterDatabases;

public sealed class NewStreamLockConjoinedDatabases : HighWaterDatabases;

public sealed class NewStreamLockTenantPartitionedDatabases : HighWaterDatabases;

public enum NewStreamLockShape
{
    SingleTenant,
    Conjoined,
    TenantPartitioned
}

public class new_stream_lock_single_tenant_tests(NewStreamLockSingleTenantDatabases databases)
    : new_stream_lock_tests(databases, NewStreamLockShape.SingleTenant), IClassFixture<NewStreamLockSingleTenantDatabases>;

public class new_stream_lock_conjoined_tests(NewStreamLockConjoinedDatabases databases)
    : new_stream_lock_tests(databases, NewStreamLockShape.Conjoined), IClassFixture<NewStreamLockConjoinedDatabases>;

public class new_stream_lock_tenant_partitioned_tests(NewStreamLockTenantPartitionedDatabases databases)
    : new_stream_lock_tests(databases, NewStreamLockShape.TenantPartitioned),
        IClassFixture<NewStreamLockTenantPartitionedDatabases>;

/// <summary>
///     #1072. The read an append's version check rests on used <c>UPDLOCK, HOLDLOCK</c>, and for a stream
///     that doesn't exist yet <c>HOLDLOCK</c> locks the range up to the next key in the index. An append
///     creating stream B then waited on an open append creating an unrelated stream A whenever A was B's next
///     key, which with random ids happened about one run in four of
///     <c>DcbAppendConditionCompliance.an_event_in_flight_below_what_the_decision_read_still_conflicts</c>.
///     These facts make B A's neighbour on purpose. A missing stream is now locked by its own id, so the
///     appends that have to take turns, the ones creating the same stream, still do. Once for each shape
///     of store, each in databases of its own.
/// </summary>
public abstract class new_stream_lock_tests(HighWaterDatabases databases, NewStreamLockShape shape)
    : HighWaterLockingContext(databases)
{
    private const string Red = "Red";

    // Plenty for a save that takes no notice of the open one. Anything blocked by it never finishes.
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan LongEnoughToHaveWaited = TimeSpan.FromMilliseconds(500);

    private Task<DocumentStore> CreateStoreAsync(DatabaseLocking locking)
        => CreateStoreAsync(locking, opts =>
        {
            if (shape == NewStreamLockShape.SingleTenant) return;

            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = shape == NewStreamLockShape.TenantPartitioned;
        });

    private string? Tenant => shape == NewStreamLockShape.SingleTenant ? null : Red;

    /// <summary>
    ///     Two new ids with nothing between them in SQL Server's <c>uniqueidentifier</c> order, the first
    ///     just below the second. SQL Server compares the last group of a Guid first and the first group
    ///     last, so they differ only in the least significant byte, and no other id can sort between them.
    /// </summary>
    private static (Guid Below, Guid Above) Neighbours()
    {
        var bytes = Guid.NewGuid().ToByteArray();
        if (bytes[3] == 0) bytes[3] = 1;

        var above = new Guid(bytes);
        bytes[3]--;
        var below = new Guid(bytes);

        new SqlGuid(below).CompareTo(new SqlGuid(above)).ShouldBe(-1);
        return (below, above);
    }

    private static async Task<long[]> VersionsAsync(DocumentStore store, Guid stream, string? tenant)
    {
        await using var query = tenant == null
            ? store.QuerySession()
            : store.QuerySession(new SessionOptions { TenantId = tenant });
        var events = await query.Events.FetchStreamAsync(stream, token: Ct);
        return events.Select(x => x.Version).ToArray();
    }

    private static IDocumentSession SessionFor(DocumentStore store, string? tenant)
        => tenant == null ? store.LightweightSession() : store.LightweightSession(new SessionOptions { TenantId = tenant });

    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task creating_a_stream_does_not_wait_on_an_open_creation_of_its_neighbour(DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking);
        var tenant = Tenant;
        var (below, above) = Neighbours();

        await using var inFlight = await InFlightAppend.StartAsync(store, above, tenant); // creates it

        await AppendAsync(store, below, tenant, start: true).WaitAsync(Bound, Ct);

        await inFlight.CommitAsync();
        (await VersionsAsync(store, below, tenant)).ShouldBe([1]);
        (await VersionsAsync(store, above, tenant)).ShouldBe([1]);
    }

    /// <summary>The batched form of the read, a save across several streams.</summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task a_save_creating_several_streams_does_not_wait_on_an_open_creation_of_a_neighbour(
        DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking);
        var tenant = Tenant;
        var (below, above) = Neighbours();
        var existing = await StartStreamsAsync(store, 1, tenant);

        await using var inFlight = await InFlightAppend.StartAsync(store, above, tenant); // creates it

        await using (var session = SessionFor(store, tenant))
        {
            session.Events.StartStream(below, new Tick());
            session.Events.StartStream(Guid.NewGuid(), new Tick());
            session.Events.Append(existing[0], new Tick());
            await session.SaveChangesAsync(Ct).WaitAsync(Bound, Ct);
        }

        await inFlight.CommitAsync();
        (await VersionsAsync(store, below, tenant)).ShouldBe([1]);
        (await VersionsAsync(store, existing[0], tenant)).ShouldBe([1, 2]);
    }

    [Fact]
    public async Task an_exclusive_fetch_of_a_missing_stream_does_not_wait_on_an_open_creation_of_its_neighbour()
    {
        var store = await CreateStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var (below, above) = Neighbours();

        await using var inFlight = await InFlightAppend.StartAsync(store, above, Tenant); // creates it

        await using var fetching = SessionFor(store, Tenant);
        var stream = await fetching.Events.FetchForExclusiveWriting<TickTotal>(below, Ct).WaitAsync(Bound, Ct);
        stream.Aggregate.ShouldBeNull();
    }

    /// <summary>
    ///     What the lock is for (#852): two appends creating the same stream take turns, and the second one
    ///     appends to the stream the first one made rather than making it again.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task two_appends_that_both_create_a_stream_still_take_turns(DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking);
        var tenant = Tenant;
        var stream = Guid.NewGuid();

        await using var inFlight = await InFlightAppend.StartAsync(store, stream, tenant); // creates it

        var second = AppendAsync(store, stream, tenant);

        await Task.Delay(LongEnoughToHaveWaited, Ct);
        second.IsCompleted.ShouldBeFalse();

        await inFlight.CommitAsync();
        await second.WaitAsync(Bound, Ct);

        (await VersionsAsync(store, stream, tenant)).ShouldBe([1, 2]);
    }

    /// <summary>The same through the batched read, with a second new stream in the save.</summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task a_save_across_several_streams_still_waits_for_an_open_creation_of_one_of_them(
        DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking);
        var tenant = Tenant;
        var stream = Guid.NewGuid();
        var other = Guid.NewGuid();

        await using var inFlight = await InFlightAppend.StartAsync(store, stream, tenant); // creates it

        var second = Task.Run(async () =>
        {
            await using var session = SessionFor(store, tenant);
            session.Events.Append(stream, new Tick());
            session.Events.Append(other, new Tick());
            await session.SaveChangesAsync(Ct);
        }, Ct);

        await Task.Delay(LongEnoughToHaveWaited, Ct);
        second.IsCompleted.ShouldBeFalse();

        await inFlight.CommitAsync();
        await second.WaitAsync(Bound, Ct);

        (await VersionsAsync(store, stream, tenant)).ShouldBe([1, 2]);
        (await VersionsAsync(store, other, tenant)).ShouldBe([1]);
    }

    /// <summary>
    ///     Saves creating the same new streams in opposite orders lock them in the same order, so they take
    ///     turns rather than deadlock, and every append lands once.
    /// </summary>
    [Fact]
    public async Task saves_creating_the_same_streams_in_opposite_orders_both_land()
    {
        var store = await CreateStoreAsync(DatabaseLocking.LockingReadCommitted);

        for (var round = 0; round < 5; round++)
        {
            var streams = Enumerable.Range(0, 6).Select(_ => Guid.NewGuid()).ToArray();

            async Task SaveAcrossAllOfThem(IEnumerable<Guid> order)
            {
                await using var session = SessionFor(store, Tenant);
                foreach (var stream in order) session.Events.Append(stream, new Tick());
                await session.SaveChangesAsync(Ct);
            }

            await Task.WhenAll(Task.Run(() => SaveAcrossAllOfThem(streams), Ct),
                Task.Run(() => SaveAcrossAllOfThem(Enumerable.Reverse(streams)), Ct));

            foreach (var stream in streams)
            {
                (await VersionsAsync(store, stream, Tenant)).ShouldBe([1, 2]);
            }
        }
    }
}
