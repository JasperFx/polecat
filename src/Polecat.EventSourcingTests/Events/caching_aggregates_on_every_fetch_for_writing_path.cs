using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Fetching;
using JasperFx.Events.Projections;
using Polecat.Internal;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     polecat#1073 (port of marten#6086): <c>CacheAggregatesForWriting&lt;T&gt;()</c> on the fetches other than the
///     plain <c>FetchForWriting(id)</c>: the expected-version overload (what Wolverine's aggregate handler workflow
///     calls when the command carries a version) and <c>FetchManyForWriting</c>, under all three lifecycles. The
///     rules are the plain fetch's (jasperfx#1050): take the entry on read, fold the newer events onto it, put it
///     back after the session's own commit, and fetch uncached when the stream is archived or behind the cached
///     aggregate.
/// </summary>
/// <remarks>
///     <para>
///         Polecat has no batched <c>FetchForWriting</c>, so Marten's three batched paths have nothing to port to.
///     </para>
///     <para>
///         Polecat's <c>FetchForWriting</c> folds the aggregate from its events under every lifecycle and never
///         loads a stored snapshot, so an Inline or Async aggregate is cached and served exactly as a Live one is:
///         at the version it was fetched at, with the next fetch reading the events after it. Marten serves an
///         Inline aggregate at the committed version, so there the chain replays nothing and the saving is the
///         snapshot load, which Polecat doesn't make.
///     </para>
/// </remarks>
public class caching_aggregates_on_every_fetch_for_writing_path : OneOffConfigurationsContext
{
    public enum FetchPath
    {
        ExpectedVersion,
        FetchMany
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly RecordingWriteCache theCache = new();

    private async Task ConfigureAsync(ProjectionLifecycle lifecycle, Action<StoreOptions>? more = null)
    {
        ConfigureStore(opts =>
        {
            switch (lifecycle)
            {
                case ProjectionLifecycle.Inline:
                    opts.Projections.Snapshot<CachedTally>(SnapshotLifecycle.Inline);
                    break;
                case ProjectionLifecycle.Async:
                    opts.Projections.Snapshot<CachedTally>(SnapshotLifecycle.Async);
                    break;
            }

            opts.Events.AggregateWriteCaching.Cache = theCache;
            opts.Events.CacheAggregatesForWriting<CachedTally>();
            more?.Invoke(opts);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private static async Task<IEventStream<CachedTally>> FetchAsync(IDocumentSession session, FetchPath path,
        Guid streamId, long expectedVersion)
        => path switch
        {
            FetchPath.ExpectedVersion => await session.Events.FetchForWriting<CachedTally>(streamId, expectedVersion, Ct),
            FetchPath.FetchMany => (await session.Events.FetchManyForWriting<CachedTally>([streamId], Ct)).Single(),
            _ => throw new ArgumentOutOfRangeException(nameof(path))
        };

    private async Task<Guid> StartAsync(int count)
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<CachedTally>(streamId,
            Enumerable.Range(0, count).Select(_ => (object)new TallyA()).ToArray());
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task AppendAsync(Guid streamId, params object[] events)
    {
        await using var session = theStore.LightweightSession();
        session.Events.Append(streamId, events);
        await session.SaveChangesAsync(Ct);
    }

    // A fetch, an append and a commit through the path, which leaves the aggregate in the cache
    private async Task WarmUpAsync(FetchPath path, Guid streamId, long version)
    {
        await using var session = theStore.LightweightSession();
        var stream = await FetchAsync(session, path, streamId, version);
        stream.AppendOne(new TallyB());
        await session.SaveChangesAsync(Ct);
    }

    private AggregateCacheKey SingleKey() => theCache.Keys.ShouldHaveSingleItem();

    private (CachedTally Aggregate, long Version) SingleEntry()
    {
        theCache.TryPeek(SingleKey(), out var aggregate, out var version).ShouldBeTrue();
        return ((CachedTally)aggregate!, version);
    }

    public static TheoryData<ProjectionLifecycle, FetchPath> EveryPath()
    {
        var data = new TheoryData<ProjectionLifecycle, FetchPath>();
        foreach (var lifecycle in new[] { ProjectionLifecycle.Live, ProjectionLifecycle.Inline, ProjectionLifecycle.Async })
        {
            foreach (var path in Enum.GetValues<FetchPath>())
            {
                data.Add(lifecycle, path);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(EveryPath))]
    public async Task each_hop_of_a_chain_is_served_from_the_cache(ProjectionLifecycle lifecycle, FetchPath path)
    {
        await ConfigureAsync(lifecycle);
        var streamId = await StartAsync(10);

        // Six handlers, one stream, each in its own session and transaction, each appending two events
        var replayed = new List<int>();
        for (var hop = 0; hop < 6; hop++)
        {
            await using var session = theStore.LightweightSession();

            IEventStream<CachedTally> stream;
            using (var folded = CachedTally.CountFolds())
            {
                stream = await FetchAsync(session, path, streamId, 10 + 2 * hop);
                replayed.Add(folded.Count);
            }

            stream.Aggregate!.ACount.ShouldBe(10 + hop);
            stream.Aggregate.BCount.ShouldBe(hop);
            stream.CurrentVersion.ShouldBe(10 + 2 * hop);

            stream.AppendMany(new TallyA(), new TallyB());
            await session.SaveChangesAsync(Ct);
        }

        theCache.Hits.ShouldBe(5);

        // The first hop reads the whole stream, and every hop after it only the two events the hop before appended
        replayed.ShouldBe([10, 2, 2, 2, 2, 2]);
    }

    [Theory]
    [MemberData(nameof(EveryPath))]
    public async Task a_cache_hit_folds_onto_the_cached_aggregate(ProjectionLifecycle lifecycle, FetchPath path)
    {
        await ConfigureAsync(lifecycle);
        var streamId = await StartAsync(3);
        await WarmUpAsync(path, streamId, 3);

        // Poison the cached aggregate. Nothing read from the database lands on 100 or more. It is cached at the
        // version it was fetched at, so the warm-up's own event is folded too.
        var (_, cachedVersion) = SingleEntry();
        cachedVersion.ShouldBe(3);
        theCache.Seed(SingleKey(), new CachedTally { Id = streamId, ACount = 100 }, cachedVersion);
        await AppendAsync(streamId, new TallyA(), new TallyA());

        await using var session = theStore.LightweightSession();
        var stream = await FetchAsync(session, path, streamId, 6);

        theCache.Hits.ShouldBe(1);
        stream.CurrentVersion.ShouldBe(6);
        stream.Aggregate!.ACount.ShouldBe(102);
        stream.Aggregate.BCount.ShouldBe(1);

        stream.AppendOne(new TallyA());
        await session.SaveChangesAsync(Ct);

        var (aggregate, written) = SingleEntry();
        aggregate.ACount.ShouldBe(102);
        written.ShouldBe(6);
    }

    [Theory]
    [MemberData(nameof(EveryPath))]
    public async Task a_cache_ahead_of_the_stream_is_not_used(ProjectionLifecycle lifecycle, FetchPath path)
    {
        await ConfigureAsync(lifecycle);
        var streamId = await StartAsync(3);
        await WarmUpAsync(path, streamId, 3);

        // A database restored out from under a cache that had run ahead
        theCache.Seed(SingleKey(), new CachedTally { Id = streamId, ACount = 100 }, 99);

        await using var session = theStore.LightweightSession();
        var stream = await FetchAsync(session, path, streamId, 4);

        theCache.Hits.ShouldBe(1);
        stream.CurrentVersion.ShouldBe(4);
        stream.Aggregate!.ACount.ShouldBe(3);
        stream.Aggregate.BCount.ShouldBe(1);

        // and the cache heals on the same round, once the session commits
        stream.AppendOne(new TallyA());
        await session.SaveChangesAsync(Ct);

        var (aggregate, version) = SingleEntry();
        version.ShouldBe(4);
        aggregate.ACount.ShouldBe(3);
    }

    [Theory]
    [MemberData(nameof(EveryPath))]
    public async Task a_stale_entry_still_yields_the_correct_aggregate(ProjectionLifecycle lifecycle, FetchPath path)
    {
        await ConfigureAsync(lifecycle);
        var streamId = await StartAsync(3);
        await WarmUpAsync(path, streamId, 3);

        // The stream moves on without the cache
        await AppendAsync(streamId, new TallyA(), new TallyB(), new TallyA());

        await using var session = theStore.LightweightSession();
        var stream = await FetchAsync(session, path, streamId, 7);

        theCache.Hits.ShouldBe(1);
        stream.CurrentVersion.ShouldBe(7);
        stream.Aggregate!.ACount.ShouldBe(5);
        stream.Aggregate.BCount.ShouldBe(2);
    }

    [Theory]
    [InlineData(ProjectionLifecycle.Live)]
    [InlineData(ProjectionLifecycle.Inline)]
    [InlineData(ProjectionLifecycle.Async)]
    public async Task the_expected_version_is_checked_against_the_stream_not_the_cache(ProjectionLifecycle lifecycle)
    {
        await ConfigureAsync(lifecycle);
        var streamId = await StartAsync(3);
        await WarmUpAsync(FetchPath.ExpectedVersion, streamId, 3);

        // The cached aggregate says the stream is at the version the caller expects. The stream says otherwise.
        var key = SingleKey();
        theCache.Seed(key, new CachedTally { Id = streamId, ACount = 3, BCount = 1 }, 4);
        await AppendAsync(streamId, new TallyA());

        await using (var session = theStore.LightweightSession())
        {
            await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
                () => FetchAsync(session, FetchPath.ExpectedVersion, streamId, 4));
        }

        // The version is checked before the events are read, so the failed fetch never took the entry. Marten's
        // takes it as the batch is built, which is the one difference: the entry here is still a good baseline.
        theCache.Hits.ShouldBe(0);

        await using (var session = theStore.LightweightSession())
        {
            var stream = await FetchAsync(session, FetchPath.ExpectedVersion, streamId, 5);
            stream.CurrentVersion.ShouldBe(5);
            stream.Aggregate!.ACount.ShouldBe(4);
            stream.Aggregate.BCount.ShouldBe(1);
        }

        theCache.Hits.ShouldBe(1);
    }

    [Theory]
    [MemberData(nameof(EveryPath))]
    public async Task cached_aggregates_do_not_cross_tenants(ProjectionLifecycle lifecycle, FetchPath path)
    {
        await ConfigureAsync(lifecycle, opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Schema.For<CachedTally>().MultiTenanted();
        });

        // Same stream id, two tenants, different events
        var streamId = Guid.NewGuid();
        foreach (var (tenant, count) in new[] { ("one", 3), ("two", 5) })
        {
            await using var session = theStore.LightweightSession(tenant);
            session.Events.StartStream<CachedTally>(streamId,
                Enumerable.Range(0, count).Select(_ => (object)new TallyA()).ToArray());
            await session.SaveChangesAsync(Ct);
        }

        var versions = new Dictionary<string, long> { ["one"] = 3, ["two"] = 5 };
        foreach (var tenant in new[] { "one", "two", "one", "two", "one", "two" })
        {
            await using var session = theStore.LightweightSession(tenant);
            var stream = await FetchAsync(session, path, streamId, versions[tenant]);

            var appended = (int)versions[tenant] - (tenant == "one" ? 3 : 5);
            stream.Aggregate!.ACount.ShouldBe(tenant == "one" ? 3 : 5);
            stream.Aggregate.BCount.ShouldBe(appended);

            stream.AppendOne(new TallyB());
            await session.SaveChangesAsync(Ct);
            versions[tenant]++;
        }

        theCache.Hits.ShouldBe(4);
        theCache.Keys.Select(x => x.TenantId).OrderBy(x => x).ShouldBe(["one", "two"]);
    }

    [Theory]
    [InlineData(FetchPath.ExpectedVersion)]
    [InlineData(FetchPath.FetchMany)]
    public async Task an_archived_live_stream_is_not_served_from_the_cache(FetchPath path)
    {
        await ConfigureAsync(ProjectionLifecycle.Live);
        var streamId = await StartAsync(3);
        await WarmUpAsync(path, streamId, 3);

        await using (var archive = theStore.LightweightSession())
        {
            archive.Events.ArchiveStream(streamId);
            await archive.SaveChangesAsync(Ct);
        }

        // Its events are left out of the read, so the uncached fetch finds no aggregate, and the cached one must
        // not bring it back
        await using var session = theStore.LightweightSession();
        var stream = await FetchAsync(session, path, streamId, 4);

        theCache.Hits.ShouldBe(1);
        stream.Aggregate.ShouldBeNull();
        stream.CurrentVersion.ShouldBe(4);
    }

    [Theory]
    [MemberData(nameof(EveryPath))]
    public async Task a_tombstoned_stream_is_not_served_from_the_cache(ProjectionLifecycle lifecycle, FetchPath path)
    {
        await ConfigureAsync(lifecycle);
        var streamId = await StartAsync(3);
        await WarmUpAsync(path, streamId, 3);

        // Marked, so an aggregate that came from the cache can be told from one read from the database
        var (_, cachedVersion) = SingleEntry();
        theCache.Seed(SingleKey(), new CachedTally { Id = streamId, ACount = 100 }, cachedVersion);

        await using (var tombstone = theStore.LightweightSession())
        {
            tombstone.Events.TombstoneStream(streamId);
            await tombstone.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession();
        var stream = await FetchAsync(session, path, streamId, 0);

        // Taken, which is what removes it, and not used
        theCache.Hits.ShouldBe(1);
        theCache.Keys.ShouldBeEmpty();
        stream.CurrentVersion.ShouldBe(0);
        stream.Aggregate.ShouldBeNull();
    }

    [Theory]
    [InlineData(ProjectionLifecycle.Live)]
    [InlineData(ProjectionLifecycle.Inline)]
    [InlineData(ProjectionLifecycle.Async)]
    public async Task fetch_many_from_the_cache_still_locks_the_streams_together_at_the_save(ProjectionLifecycle lifecycle)
    {
        await ConfigureAsync(lifecycle);

        var one = await StartAsync(3);
        var two = await StartAsync(2);
        await WarmUpAsync(FetchPath.ExpectedVersion, one, 3);
        await WarmUpAsync(FetchPath.ExpectedVersion, two, 2);

        await using var session = theStore.LightweightSession();
        var streams = await session.Events.FetchManyForWriting<CachedTally>([two, one], Ct);

        theCache.Hits.ShouldBe(2);
        streams[0].Aggregate!.ACount.ShouldBe(2);
        streams[1].Aggregate!.ACount.ShouldBe(3);

        streams[0].AppendOne(new TallyA());
        streams[1].AppendOne(new TallyA());

        var executed = new List<string>();
        ((QuerySession)session).BeforeExecuteAsync = (sql, _) =>
        {
            executed.Add(sql);
            return Task.CompletedTask;
        };

        await session.SaveChangesAsync(Ct);

        // Two streams appended to in one save are read and locked in one statement, in the order of their ids
        // (EventGraph.LockedStreamReadSql), whatever the fetch got from the cache
        executed.Count(x => x.Contains("OPENJSON(@ids)") && x.Contains("UPDLOCK")).ShouldBe(1);

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamStateAsync(one, Ct))!.Version.ShouldBe(5);
        (await query.Events.FetchStreamStateAsync(two, Ct))!.Version.ShouldBe(4);
    }
}

public record TallyA;

public record TallyB;

public class CachedTally
{
    private static readonly AsyncLocal<FoldCounter?> Counter = new();

    public Guid Id { get; set; }
    public int ACount { get; set; }
    public int BCount { get; set; }

    public void Apply(TallyA _)
    {
        ACount++;
        Counter.Value?.Increment();
    }

    public void Apply(TallyB _)
    {
        BCount++;
        Counter.Value?.Increment();
    }

    /// <summary>
    ///     Counts the events folded by this flow until it is disposed. Polecat has no events-replayed metric, and an
    ///     inline projection applying the same events at the save runs outside it.
    /// </summary>
    public static FoldCounter CountFolds()
    {
        var counter = new FoldCounter(() => Counter.Value = null);
        Counter.Value = counter;
        return counter;
    }

    public sealed class FoldCounter(Action stop) : IDisposable
    {
        private int _count;
        public int Count => _count;
        public void Increment() => Interlocked.Increment(ref _count);
        public void Dispose() => stop();
    }
}
