using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     polecat#1049 (port of marten#5799 and marten#6073): the Async fetch scenarios that
///     <see cref="async_fetch_reads_in_a_safe_order" /> does not run per store setup.
/// </summary>
[Collection("async-fetch")]
public class async_fetch_edge_cases : OneOffConfigurationsContext
{
    public async_fetch_edge_cases()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Snapshot<FetchedAggregate>(SnapshotLifecycle.Async);
            opts.Projections.UseIdentityMapForAggregates = false;
        });
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task catchTheDaemonUpAsync()
    {
        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(60), Ct);
        await daemon.StopAllAsync();
    }

    private async Task<Guid> aStreamAsync(params object[] events)
    {
        await using var session = theStore.LightweightSession();
        var id = Guid.NewGuid();
        session.Events.StartStream<FetchedAggregate>(id, events);
        await session.SaveChangesAsync(Ct);
        return id;
    }

    private Task<Guid> aStreamOfABCAsync() => aStreamAsync(new FetchedA(), new FetchedB(), new FetchedC());

    private async Task appendAsync(Guid id, params object[] events)
    {
        await using var session = theStore.LightweightSession();
        session.Events.Append(id, events);
        await session.SaveChangesAsync(Ct);
    }

    private async Task deleteTheStreamAsync(Guid id)
    {
        var events = theStore.Options.EventGraph;
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {events.EventsTableName} WHERE stream_id = @id; " +
                          $"DELETE FROM {events.StreamsTableName} WHERE id = @id;";
        cmd.Parameters.AddWithValue("@id", id);
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static void shouldBeTheWholeStream(FetchedAggregate? aggregate)
    {
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(1);
        aggregate.BCount.ShouldBe(1);
        aggregate.CCount.ShouldBe(1);
    }

    [Fact]
    public async Task fetch_many_for_writing_reads_every_stream_in_two_round_trips_while_the_daemon_writes()
    {
        var ids = new[] { await aStreamOfABCAsync(), await aStreamOfABCAsync(), await aStreamOfABCAsync() };

        await using var session = theStore.LightweightSession();
        var pause = PauseBeforeTheEventsRead.Attach(session, catchTheDaemonUpAsync);

        var streams = await session.Events.FetchManyForWriting<FetchedAggregate>(ids, Ct);

        pause.Paused.ShouldBe(1);
        session.RequestCount.ShouldBe(2);
        streams.Count.ShouldBe(3);
        for (var i = 0; i < ids.Length; i++)
        {
            streams[i].Id.ShouldBe(ids[i]);
            streams[i].CurrentVersion.ShouldBe(3);
            shouldBeTheWholeStream(streams[i].Aggregate);
        }
    }

    [Fact]
    public async Task fetch_many_for_writing_answers_each_stream_at_the_version_its_events_reached()
    {
        var ids = new[] { await aStreamOfABCAsync(), await aStreamOfABCAsync() };

        await using var session = theStore.LightweightSession();
        PauseBeforeTheEventsRead.Attach(session, () => appendAsync(ids[1], new FetchedD()));

        var streams = await session.Events.FetchManyForWriting<FetchedAggregate>(ids, Ct);

        streams[0].CurrentVersion.ShouldBe(3);
        streams[0].Aggregate!.DCount.ShouldBe(0);
        streams[1].CurrentVersion.ShouldBe(4);
        streams[1].Aggregate!.DCount.ShouldBe(1);

        // And the handle writes at that version
        streams[1].AppendOne(new FetchedD());
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamStateAsync(ids[1], Ct))!.Version.ShouldBe(5);
    }

    [Fact]
    public async Task the_stream_handle_from_a_fetch_that_saw_an_append_writes_at_the_newer_version()
    {
        var id = await aStreamOfABCAsync();
        await catchTheDaemonUpAsync();

        await using var session = theStore.LightweightSession();
        var pause = PauseBeforeTheEventsRead.Attach(session, async () =>
        {
            await appendAsync(id, new FetchedD());
            await catchTheDaemonUpAsync();
        });

        var stream = await session.Events.FetchForWriting<FetchedAggregate>(id, Ct);
        pause.Paused.ShouldBe(1);
        stream.CurrentVersion.ShouldBe(4);

        stream.AppendOne(new FetchedD());
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamStateAsync(id, Ct))!.Version.ShouldBe(5);
    }

    // A write-cache hit reads the stream version and then the events after the cached version. An append
    // between the two is in the events, so the fetch is at the newer version, and is cached at it.

    [Fact]
    public async Task a_write_cache_hit_answers_at_an_event_appended_between_its_reads()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Snapshot<FetchedAggregate>(SnapshotLifecycle.Async);
            opts.Projections.UseIdentityMapForAggregates = false;
            opts.Events.CacheAggregatesForWriting<FetchedAggregate>();
        });

        var id = await aStreamOfABCAsync();

        // Fills the cache, which takes its entry when the session commits
        await using (var warming = theStore.LightweightSession())
        {
            (await warming.Events.FetchForWriting<FetchedAggregate>(id, Ct)).CurrentVersion.ShouldBe(3);
            await warming.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            var pause = PauseBeforeTheEventsRead.Attach(session, () => appendAsync(id, new FetchedD()));

            var stream = await session.Events.FetchForWriting<FetchedAggregate>(id, Ct);
            pause.Paused.ShouldBe(1);

            stream.CurrentVersion.ShouldBe(4);
            shouldBeTheWholeStream(stream.Aggregate);
            stream.Aggregate!.DCount.ShouldBe(1);

            await session.SaveChangesAsync(Ct);
        }

        // The cached aggregate is labelled with the version it is at, so D is not applied to it again
        await using var next = theStore.LightweightSession();
        var nextStream = await next.Events.FetchForWriting<FetchedAggregate>(id, Ct);
        nextStream.CurrentVersion.ShouldBe(4);
        shouldBeTheWholeStream(nextStream.Aggregate);
        nextStream.Aggregate!.DCount.ShouldBe(1);
    }

    // A snapshot whose stream is gone is refused, however it is fetched. async_fetch_reads_in_a_safe_order
    // runs every kind of fetch against one; these are the ways in that it does not cover.

    [Fact]
    public async Task a_snapshot_left_behind_by_delete_all_event_data_is_refused()
    {
        var id = await aStreamOfABCAsync();
        await catchTheDaemonUpAsync();
        await theStore.Advanced.Clean.DeleteAllEventDataAsync(Ct);

        await using var session = theStore.LightweightSession();
        (await Should.ThrowAsync<SnapshotWithoutStreamException>(() =>
            session.Events.FetchForWriting<FetchedAggregate>(id, Ct))).Id.ShouldBe(id);
        (await Should.ThrowAsync<SnapshotWithoutStreamException>(() =>
            session.Events.FetchForWriting<FetchedAggregate>(id, 0, Ct))).Id.ShouldBe(id);
        (await Should.ThrowAsync<SnapshotWithoutStreamException>(async () =>
            await session.Events.FetchLatest<FetchedAggregate>(id, Ct))).Id.ShouldBe(id);

        // Deleting the snapshot documents too, as the docs say to, makes it a new stream again
        await theStore.Advanced.CleanAsync<FetchedAggregate>(Ct);
        var stream = await session.Events.FetchForWriting<FetchedAggregate>(id, Ct);
        stream.Aggregate.ShouldBeNull();
        stream.CurrentVersion.ShouldBe(0);
    }

    [Fact]
    public async Task an_exclusive_fetch_refusing_a_snapshot_without_its_stream_releases_its_lock_with_the_session()
    {
        var orphanId = await aStreamOfABCAsync();
        await catchTheDaemonUpAsync();
        await deleteTheStreamAsync(orphanId);

        await using (var session = theStore.LightweightSession())
        {
            var ex = await Should.ThrowAsync<SnapshotWithoutStreamException>(() =>
                session.Events.FetchForExclusiveWriting<FetchedAggregate>(orphanId, Ct));
            ex.Id.ShouldBe(orphanId);
        }

        // Disposing the session rolled its transaction back, so the stream can be started at once
        await theStore.Advanced.CleanAsync<FetchedAggregate>(Ct);
        await using var other = theStore.LightweightSession();
        other.Events.StartStream<FetchedAggregate>(orphanId, new FetchedA());
        await other.SaveChangesAsync(Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);
    }

    [Fact]
    public async Task fetch_many_for_writing_refuses_a_snapshot_without_its_stream()
    {
        var ids = new[] { await aStreamOfABCAsync(), await aStreamOfABCAsync() };
        await catchTheDaemonUpAsync();
        await deleteTheStreamAsync(ids[1]);

        await using var session = theStore.LightweightSession();
        var ex = await Should.ThrowAsync<SnapshotWithoutStreamException>(() =>
            session.Events.FetchManyForWriting<FetchedAggregate>(ids, Ct));
        ex.Id.ShouldBe(ids[1]);
    }

    [Fact]
    public async Task a_new_stream_costs_one_more_round_trip_only_for_an_async_aggregate()
    {
        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<FetchedAggregate>(Guid.NewGuid(), Ct);
        stream.CurrentVersion.ShouldBe(0);

        // The version read, then the snapshot check
        session.RequestCount.ShouldBe(2);
    }
}

/// <summary>
///     marten#6073 / polecat#1049: <c>FetchManyForWriting</c> over a mix of existing and new streams, then an
///     append through <c>session.Events.Append</c> rather than through the handle. The new stream must be
///     started once, with its events, and not twice.
/// </summary>
[Collection("async-fetch")]
public class fetch_many_for_writing_then_append_to_a_new_stream : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task by_guid()
    {
        ConfigureStore(opts => opts.Projections.Snapshot<FetchedAggregate>(SnapshotLifecycle.Async));

        var existing = Guid.NewGuid();
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<FetchedAggregate>(existing, new FetchedA());
            await start.SaveChangesAsync(Ct);
        }

        var newId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            var streams = await session.Events.FetchManyForWriting<FetchedAggregate>([existing, newId], Ct);
            streams[1].CurrentVersion.ShouldBe(0);

            session.Events.Append(newId, new FetchedA(), new FetchedB());
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(newId, token: Ct);
        events.Select(x => x.Version).ShouldBe([1L, 2L]);
        (await query.Events.FetchStreamStateAsync(newId, Ct))!.Version.ShouldBe(2);
    }

    [Fact]
    public async Task by_string()
    {
        ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<FetchedAggregateAsString>(SnapshotLifecycle.Async);
        });

        var existing = Guid.NewGuid().ToString();
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<FetchedAggregateAsString>(existing, new FetchedA());
            await start.SaveChangesAsync(Ct);
        }

        var newKey = Guid.NewGuid().ToString();
        await using (var session = theStore.LightweightSession())
        {
            var streams = await session.Events.FetchManyForWriting<FetchedAggregateAsString>([existing, newKey], Ct);
            streams[1].CurrentVersion.ShouldBe(0);

            session.Events.Append(newKey, new FetchedA(), new FetchedB());
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(newKey, token: Ct);
        events.Select(x => x.Version).ShouldBe([1L, 2L]);
        (await query.Events.FetchStreamStateAsync(newKey, Ct))!.Version.ShouldBe(2);
    }
}
