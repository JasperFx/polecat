using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.Tests.Events;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Tests.Daemon;

public sealed class TenantPartitionedAppendDatabases : HighWaterDatabases;

/// <summary>
///     #852. Under <c>UseTenantPartitionedEvents</c> the read a version check rests on seeked
///     <c>(tenant_id, id)</c> on a key of <c>(tenant_id, id, tenant_ordinal)</c>, which under <c>HOLDLOCK</c>
///     locks the end of every partition it finds nothing in. Every other such read needs one of those same
///     keys, so one open append held up every append to an existing stream in the store, 24 of 24 in its
///     own tenant and 24 of 24 in another. These facts hold that open and look to both sides of it: nobody
///     else waits, and everyone who should wait still does.
/// </summary>
public class tenant_partitioned_append_lock_tests(TenantPartitionedAppendDatabases databases)
    : HighWaterLockingContext(databases), IClassFixture<TenantPartitionedAppendDatabases>
{
    private const string Red = "Red";
    private const string Blue = "Blue";

    // Plenty for a save that takes no notice of the open one. Anything blocked by it never finishes.
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan LongEnoughToHaveWaited = TimeSpan.FromMilliseconds(500);

    private Task<DocumentStore> CreateTenantedStoreAsync(DatabaseLocking locking,
        Action<StoreOptions>? configure = null) => CreateStoreAsync(locking, opts =>
    {
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
        configure?.Invoke(opts);
    });

    private static IDocumentSession SessionFor(DocumentStore store, string tenant)
        => store.LightweightSession(new SessionOptions { TenantId = tenant });

    private static async Task<long[]> VersionsAsync(DocumentStore store, Guid stream, string tenant)
    {
        await using var query = store.QuerySession(new SessionOptions { TenantId = tenant });
        var events = await query.Events.FetchStreamAsync(stream, token: Ct);
        return events.Select(x => x.Version).ToArray();
    }

    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_open_append_does_not_hold_up_the_rest_of_its_tenant(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 6, Red);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Red);

        foreach (var stream in streams.Skip(1))
        {
            await AppendAsync(store, stream, Red).WaitAsync(Bound, Ct);
        }
    }

    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_open_append_does_not_hold_up_another_tenant(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var reds = await StartStreamsAsync(store, 1, Red);
        var blues = await StartStreamsAsync(store, 5, Blue);

        await using var inFlight = await InFlightAppend.StartAsync(store, reds[0], Red);

        foreach (var stream in blues)
        {
            await AppendAsync(store, stream, Blue).WaitAsync(Bound, Ct);
        }
    }

    /// <summary>
    ///     The batched form of the read, which is a different statement with a different plan available to
    ///     it. See <see cref="conjoined_append_lock_tests" /> for the same thing without partitioning.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_open_append_does_not_hold_up_a_save_across_several_streams(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 6, Red);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Red);

        await using var session = SessionFor(store, Red);
        foreach (var stream in streams.Skip(1)) session.Events.Append(stream, new Tick());

        await session.SaveChangesAsync(Ct).WaitAsync(Bound, Ct);
    }

    // ReadCommittedSnapshot from here on because these go on to load the stream. That read doesn't name
    // tenant_ordinal, which leads ix_pc_events_stream_and_version here, so it is a scan of pc_events, and with
    // locking reads it waits on the open append's rows. That is a different lock from the one under test.
    [Fact]
    public async Task an_open_append_does_not_hold_up_the_exclusive_paths()
    {
        var store = await CreateTenantedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 3, Red);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Red);

        await using var fetching = SessionFor(store, Red);
        await fetching.Events.FetchForExclusiveWriting<TickTotal>(streams[1], Ct).WaitAsync(Bound, Ct);

        await using var appending = SessionFor(store, Red);
        await appending.Events.AppendExclusive(streams[2], Ct, new Tick()).WaitAsync(Bound, Ct);
    }

    [Fact]
    public async Task an_open_append_does_not_hold_up_a_fetch_by_natural_key()
    {
        var store = await CreateTenantedStoreAsync(DatabaseLocking.ReadCommittedSnapshot,
            opts => opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline));

        var number = new OrderNumber(Guid.NewGuid().ToString("N"));
        await using (var session = SessionFor(store, Red))
        {
            session.Events.StartStream(Guid.NewGuid(), new NkOrderCreated(number, "A"));
            await session.SaveChangesAsync(Ct);
        }

        var streams = await StartStreamsAsync(store, 1, Red);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Red);

        await using var fetching = SessionFor(store, Red);
        var fetched = await fetching.Events.FetchForWriting<OrderAggregate, OrderNumber>(number, Ct)
            .WaitAsync(Bound, Ct);

        fetched.Aggregate.ShouldNotBeNull().OrderNum.ShouldBe(number);
    }

    /// <summary>
    ///     What the lock is for. The second append has to see the version the first one wrote, which it
    ///     can only do by waiting for it.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_append_to_the_same_stream_still_waits_and_takes_the_next_version(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 1, Red);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Red); // version 2

        var second = AppendAsync(store, streams[0], Red); // version 3

        await Task.Delay(LongEnoughToHaveWaited, Ct);
        second.IsCompleted.ShouldBeFalse("nothing has told it what version comes next");

        await inFlight.CommitAsync();
        await second.WaitAsync(Bound, Ct);

        (await VersionsAsync(store, streams[0], Red)).ShouldBe([1, 2, 3]);
    }

    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task a_version_check_made_behind_an_open_append_still_fails_once_it_lands(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 1, Red);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Red); // version 2

        await using var stale = SessionFor(store, Red);
        stale.Events.Append(streams[0], 2, new Tick()); // expects to find version 1
        var save = stale.SaveChangesAsync(Ct);

        await Task.Delay(LongEnoughToHaveWaited, Ct);
        save.IsCompleted.ShouldBeFalse();

        await inFlight.CommitAsync();

        await Should.ThrowAsync<JasperFx.Events.EventStreamUnexpectedMaxEventIdException>(
            () => save.WaitAsync(Bound, Ct));

        (await VersionsAsync(store, streams[0], Red)).ShouldBe([1, 2]);
    }

    /// <summary>
    ///     The half that a key lock alone would lose: there is no row yet to hold one on. The whole key
    ///     still locks the gap the row would go in, in the only partition it can go in.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task two_appends_that_both_create_a_stream_still_take_turns(DatabaseLocking locking)
    {
        var store = await CreateTenantedStoreAsync(locking);
        await StartStreamsAsync(store, 2, Red);

        var stream = Guid.NewGuid();

        await using var inFlight = await InFlightAppend.StartAsync(store, stream, Red); // creates it

        var second = AppendAsync(store, stream, Red);

        await Task.Delay(LongEnoughToHaveWaited, Ct);
        second.IsCompleted.ShouldBeFalse();

        await inFlight.CommitAsync();
        await second.WaitAsync(Bound, Ct);

        (await VersionsAsync(store, stream, Red)).ShouldBe([1, 2]);
    }

    /// <summary>
    ///     A tenant removed with its data kept, and added back once its ordinal has gone to somebody else.
    ///     The row stays under the old ordinal, so a read of the new one alone finds nothing, the stream
    ///     looks new, and the append numbers from 1 again in a second row.
    /// </summary>
    [Fact]
    public async Task a_stream_left_under_an_old_ordinal_is_not_forked()
    {
        var store = await CreateTenantedStoreAsync(DatabaseLocking.LockingReadCommitted);
        await StartStreamsAsync(store, 1, Blue);
        var streams = await StartStreamsAsync(store, 1, Red);
        await AppendAsync(store, streams[0], Red);

        await store.Advanced.RemovePolecatManagedTenantsAsync([Red], TenantDropBehavior.RetainData, Ct);
        await store.Advanced.AddPolecatManagedTenantsAsync(Ct, "Green");

        await AppendAsync(store, streams[0], Red);

        (await VersionsAsync(store, streams[0], Red)).ShouldBe([1, 2, 3]);

        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM dbo.pc_streams WHERE id = @id;";
        cmd.Parameters.AddWithValue("@id", streams[0]);
        ((int)(await cmd.ExecuteScalarAsync(Ct))!).ShouldBe(1);
    }
}

public sealed class ConjoinedAppendDatabases : HighWaterDatabases;

/// <summary>
///     #852, without partitioning. A save across several streams read them as <c>id IN (...)</c>, and on a
///     conjoined store that plan walked the whole tenant under <c>HOLDLOCK</c>, so it waited on any open
///     append in the tenant, and any open one of its own held up every other.
/// </summary>
public class conjoined_append_lock_tests(ConjoinedAppendDatabases databases)
    : HighWaterLockingContext(databases), IClassFixture<ConjoinedAppendDatabases>
{
    private const string Red = "Red";

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task an_open_append_does_not_hold_up_a_save_across_several_streams(DatabaseLocking locking)
    {
        var store = await CreateStoreAsync(locking, opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);
        var streams = await StartStreamsAsync(store, 6, Red);

        await using var inFlight = await InFlightAppend.StartAsync(store, streams[0], Red);

        await using var session = store.LightweightSession(new SessionOptions { TenantId = Red });
        foreach (var stream in streams.Skip(1)) session.Events.Append(stream, new Tick());

        await session.SaveChangesAsync(Ct).WaitAsync(Bound, Ct);
    }

    /// <summary>
    ///     A session waiting on a lock it can't have has nothing to do with the order it asks in, unless two
    ///     of them want the same streams. Then the sorted ids are what stops them deadlocking.
    /// </summary>
    [Fact]
    public async Task saves_across_the_same_streams_take_turns_and_both_land()
    {
        var store = await CreateStoreAsync(DatabaseLocking.LockingReadCommitted,
            opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);
        var streams = await StartStreamsAsync(store, 6, Red);

        async Task SaveAcrossAllOfThem(IEnumerable<Guid> order)
        {
            for (var i = 0; i < 10; i++)
            {
                await using var session = store.LightweightSession(new SessionOptions { TenantId = Red });
                foreach (var stream in order) session.Events.Append(stream, new Tick());
                await session.SaveChangesAsync(Ct);
            }
        }

        await Task.WhenAll(Task.Run(() => SaveAcrossAllOfThem(streams), Ct),
            Task.Run(() => SaveAcrossAllOfThem(streams.Reverse()), Ct));

        await using var query = store.QuerySession(new SessionOptions { TenantId = Red });
        foreach (var stream in streams)
        {
            var events = await query.Events.FetchStreamAsync(stream, token: Ct);
            events.Select(x => x.Version).ShouldBe(Enumerable.Range(1, 21).Select(x => (long)x));
        }
    }
}
