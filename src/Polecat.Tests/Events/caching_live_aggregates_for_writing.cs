using JasperFx.Events;
using JasperFx.Events.Fetching;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     polecat#1035 (port of marten#5225): <c>CacheAggregatesForWriting&lt;T&gt;()</c> for an aggregate with no
///     registered projection, built Live on every fetch. The cache is only ever a baseline: the stream version
///     and every event after the cached version are read from the database on every fetch, so a stale or wrong
///     entry costs a bigger read and never a wrong aggregate. The facts marked as the fix are the cases where
///     the entry is not a baseline at all and has to be dropped: an archived stream, a stream that was deleted,
///     an entry at version zero, and a slower session's write-back landing after a newer one.
/// </summary>
public class caching_live_aggregates_for_writing : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly RecordingWriteCache theCache = new();

    private async Task UseCachedLiveAggregatesAsync(Action<StoreOptions>? more = null)
    {
        // No projection for Vault, so it is built Live
        ConfigureStore(opts =>
        {
            opts.Events.AggregateWriteCaching.Cache = theCache;
            opts.Events.CacheAggregatesForWriting<Vault>();
            more?.Invoke(opts);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private AggregateCacheKey KeyFor(Guid streamId, string? tenantId = null)
        => new(typeof(Vault), theStore.Database.Identifier, tenantId ?? theStore.Options.Tenancy!.DefaultTenantId,
            streamId);

    private async Task<Guid> StartAsync(params int[] amounts)
    {
        var streamId = Guid.NewGuid();
        await StartAsync(streamId, amounts);
        return streamId;
    }

    private async Task StartAsync(Guid streamId, params int[] amounts)
    {
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<Vault>(streamId,
            [new VaultOpened("Hilda"), .. amounts.Select(x => (object)new CoinsAdded(x))]);
        await session.SaveChangesAsync(Ct);
    }

    private async Task AppendAsync(Guid streamId, params int[] amounts)
    {
        await using var session = theStore.LightweightSession();
        session.Events.Append(streamId, amounts.Select(x => (object)new CoinsAdded(x)).ToArray());
        await session.SaveChangesAsync(Ct);
    }

    /// <summary>A hop: fetch, append one event, commit. The commit writes the aggregate back.</summary>
    private async Task HopAsync(Guid streamId, int amount = 1)
    {
        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);
        stream.AppendOne(new CoinsAdded(amount));
        await session.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task a_cold_fetch_matches_the_uncached_aggregate_and_seeds_the_cache_after_the_commit()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(10, 20);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);

        theCache.Hits.ShouldBe(0);
        stream.StartingVersion.ShouldBe(3);
        stream.Aggregate!.Coins.ShouldBe(30);
        stream.Aggregate.Id.ShouldBe(streamId);

        // Nothing is in the cache until the session commits...
        stream.AppendOne(new CoinsAdded(5));
        theCache.Stores.ShouldBeEmpty();
        await session.SaveChangesAsync(Ct);

        // ... and then the fetch has left its aggregate behind at the version it was fetched at
        var stored = theCache.Stores.ShouldHaveSingleItem();
        stored.Version.ShouldBe(3);
        stored.Aggregate.ShouldBeSameAs(stream.Aggregate);
    }

    [Fact]
    public async Task a_cache_hit_folds_only_the_events_after_the_cached_version()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);

        // Poison the baseline. Replaying the whole stream answers 4, folding events 4 and 5 onto it answers 1002.
        theCache.Seed(KeyFor(streamId), new Vault { Id = streamId, Owner = "Hilda", Coins = 1000 }, 3);
        await AppendAsync(streamId, 1, 1);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);

        theCache.Hits.ShouldBe(1);
        stream.StartingVersion.ShouldBe(5);
        stream.Aggregate!.Coins.ShouldBe(1002);
    }

    [Fact]
    public async Task an_exclusive_fetch_uses_the_cache_too()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);
        theCache.Seed(KeyFor(streamId), new Vault { Id = streamId, Owner = "Hilda", Coins = 1000 }, 3);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForExclusiveWriting<Vault>(streamId, Ct);

        theCache.Hits.ShouldBe(1);
        stream.StartingVersion.ShouldBe(3);
        stream.Aggregate!.Coins.ShouldBe(1000);
        stream.AppendOne(new CoinsAdded(1));
        await session.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task each_hop_of_a_chain_starts_from_the_hop_before()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1, 1, 1, 1, 1, 1, 1, 1);

        // Six handlers, one stream, each in its own session and transaction
        for (var hop = 0; hop < 6; hop++)
        {
            await using var session = theStore.LightweightSession();
            var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);
            stream.Aggregate!.Coins.ShouldBe(9 + 2 * hop);
            stream.StartingVersion.ShouldBe(10 + 2 * hop);

            stream.AppendMany(new CoinsAdded(1), new CoinsAdded(1));
            await session.SaveChangesAsync(Ct);
        }

        theCache.Hits.ShouldBe(5);
        theCache.Stores.Select(x => x.Version).ShouldBe([10, 12, 14, 16, 18, 20]);
    }

    [Fact]
    public async Task a_stale_entry_still_yields_the_correct_aggregate()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);
        await HopAsync(streamId);

        // The cache is now three versions behind
        await AppendAsync(streamId, 10, 10, 10);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);

        theCache.Hits.ShouldBe(1);
        stream.StartingVersion.ShouldBe(7);
        stream.Aggregate!.Coins.ShouldBe(33);
    }

    [Fact]
    public async Task optimistic_concurrency_still_fires_against_a_warm_cache_and_writes_nothing_back()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1);
        await HopAsync(streamId);
        var stores = theCache.Stores.Count;

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);
        theCache.Hits.ShouldBe(1);
        stream.AppendOne(new CoinsAdded(1));

        // Somebody else gets there first
        await AppendAsync(streamId, 1);

        await Should.ThrowAsync<Exception>(() => session.SaveChangesAsync(Ct));

        theCache.Stores.Count.ShouldBe(stores, "the failed commit wrote nothing back");
        theCache.Keys.ShouldBeEmpty("and the claim had already taken the entry");
    }

    [Fact]
    public async Task a_cache_ahead_of_the_database_falls_back_and_heals()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);

        // A database restored out from under a cache that had run ahead
        theCache.Seed(KeyFor(streamId), new Vault { Id = streamId, Owner = "Hilda", Coins = 1000 }, 99);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);

        stream.StartingVersion.ShouldBe(3);
        stream.Aggregate!.Coins.ShouldBe(2);

        // and the cache heals on the same round, once the session commits
        stream.AppendOne(new CoinsAdded(1));
        await session.SaveChangesAsync(Ct);
        theCache.TryPeek(KeyFor(streamId), out var aggregate, out var version).ShouldBeTrue();
        version.ShouldBe(3);
        ((Vault)aggregate!).Coins.ShouldBe(2);
    }

    /// <summary>
    ///     The fix. An entry at the stream's current version has no delta to read, so before polecat#1035 it
    ///     was served as it was, for a stream whose events a Live fetch leaves out.
    /// </summary>
    [Fact]
    public async Task an_archived_stream_is_not_served_from_the_cache()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);
        await HopAsync(streamId);
        await HopAsync(streamId);

        // The entry is at version 4 and the stream at 5. Bring the entry up to the stream's own version,
        // which is what a fetch for writing that appended nothing leaves behind.
        theCache.Seed(KeyFor(streamId), new Vault { Id = streamId, Owner = "Hilda", Coins = 4 }, 5);

        await using (var archive = theStore.LightweightSession())
        {
            archive.Events.ArchiveStream(streamId);
            await archive.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);

        theCache.Hits.ShouldBe(2, "the entry was claimed, and then not used");
        stream.Aggregate.ShouldBeNull();
        stream.StartingVersion.ShouldBe(5);
        theCache.Keys.ShouldBeEmpty();
    }

    [Fact]
    public async Task fetch_many_for_writing_does_not_serve_an_archived_stream_from_the_cache()
    {
        await UseCachedLiveAggregatesAsync();
        var archived = await StartAsync(1, 1);
        var healthy = await StartAsync(1, 1);

        theCache.Seed(KeyFor(archived), new Vault { Id = archived, Owner = "Hilda", Coins = 1000 }, 3);
        theCache.Seed(KeyFor(healthy), new Vault { Id = healthy, Owner = "Hilda", Coins = 7000 }, 3);

        await using (var archive = theStore.LightweightSession())
        {
            archive.Events.ArchiveStream(archived);
            await archive.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession();
        var streams = await session.Events.FetchManyForWriting<Vault>([archived, healthy], Ct);

        streams[0].Aggregate.ShouldBeNull();
        streams[1].Aggregate!.Coins.ShouldBe(7000, "the sibling keeps its own baseline");
    }

    [Fact]
    public async Task a_tombstoned_stream_is_not_served_from_the_cache()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);
        await HopAsync(streamId);

        await using (var tombstone = theStore.LightweightSession())
        {
            tombstone.Events.TombstoneStream(streamId);
            await tombstone.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);

        stream.Aggregate.ShouldBeNull();
        stream.StartingVersion.ShouldBe(0);
    }

    /// <summary>
    ///     The fix. A fetch of a stream that isn't there used to leave the entry alone, so a tombstoned
    ///     stream's aggregate outlived it and became the baseline of the next stream with that id once that
    ///     one got past its version: the new stream's fetch came back with the old one's state.
    /// </summary>
    [Fact]
    public async Task a_new_stream_with_a_tombstoned_streams_id_does_not_start_from_its_entry()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(500, 500);
        await HopAsync(streamId); // the entry: 1000 coins at version 3

        await using (var tombstone = theStore.LightweightSession())
        {
            tombstone.Events.TombstoneStream(streamId);
            await tombstone.SaveChangesAsync(Ct);
        }

        // Something checks whether the stream is there before starting it again
        await using (var check = theStore.LightweightSession())
        {
            var missing = await check.Events.FetchForWriting<Vault>(streamId, Ct);
            missing.Aggregate.ShouldBeNull();
        }

        await StartAsync(streamId, 1, 1, 1, 1);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);

        stream.StartingVersion.ShouldBe(5);
        stream.Aggregate!.Coins.ShouldBe(4);
    }

    [Fact]
    public async Task fetch_many_for_writing_drops_the_entry_of_a_stream_that_is_not_there()
    {
        await UseCachedLiveAggregatesAsync();
        var gone = Guid.NewGuid();
        theCache.Seed(KeyFor(gone), new Vault { Id = gone, Owner = "Hilda", Coins = 1000 }, 3);

        await using (var session = theStore.LightweightSession())
        {
            var streams = await session.Events.FetchManyForWriting<Vault>([gone], Ct);
            streams[0].Aggregate.ShouldBeNull();
        }

        theCache.Keys.ShouldBeEmpty();
    }

    /// <summary>
    ///     The fix. An entry at version zero describes no event, and starting from it folds the whole stream
    ///     onto whatever instance it is.
    /// </summary>
    [Fact]
    public async Task an_entry_at_version_zero_is_not_a_baseline()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);
        theCache.Seed(KeyFor(streamId), new Vault { Id = streamId, Owner = "Hilda", Coins = 1000 }, 0);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);

        stream.Aggregate!.Coins.ShouldBe(2);
    }

    /// <summary>
    ///     The fix. Commits finish in whatever order they finish, so a session that read the stream at an older
    ///     version can write back after one that published a newer entry, and replaced it with its own.
    /// </summary>
    [Fact]
    public async Task a_slower_session_does_not_replace_a_newer_entry()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);

        await using var slow = theStore.LightweightSession();
        var stream = await slow.Events.FetchForWriting<Vault>(streamId, Ct);
        stream.AppendOne(new CoinsAdded(1));

        // A newer writer publishes before the slow one finishes
        var newer = new Vault { Id = streamId, Owner = "Hilda", Coins = 3 };
        theCache.Seed(KeyFor(streamId), newer, 4);

        await slow.SaveChangesAsync(Ct);

        theCache.TryPeek(KeyFor(streamId), out var aggregate, out var version).ShouldBeTrue();
        version.ShouldBe(4);
        aggregate.ShouldBeSameAs(newer);
    }

    /// <summary>
    ///     The fix. The write-back runs after the commit, so a cache that throws there must not turn a commit
    ///     that happened into a save that reports failure and gets retried.
    /// </summary>
    [Fact]
    public async Task a_cache_that_fails_after_the_commit_does_not_fail_the_save()
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);
        theCache.FailStores = true;

        await HopAsync(streamId);

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(streamId, token: Ct)).Count.ShouldBe(4);
    }

    [Fact]
    public async Task folding_this_sessions_own_events_does_not_reach_the_cache()
    {
        await UseCachedLiveAggregatesAsync(opts => opts.Projections.UseIdentityMapForAggregates = true);
        var streamId = await StartAsync(1, 1);

        await using (var session = theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);
            stream.AppendOne(new CoinsAdded(1));

            // With the identity map on, the fetched instance is the one registered for the cache, and
            // folding the pending event onto it would leave it holding an event its version (3) says it does not
            var latest = await session.Events.ProjectLatest<Vault>(streamId, Ct);
            latest!.Coins.ShouldBe(3);

            await session.SaveChangesAsync(Ct);
        }

        await using var next = theStore.LightweightSession();
        var again = await next.Events.FetchForWriting<Vault>(streamId, Ct);
        again.StartingVersion.ShouldBe(4);
        again.Aggregate!.Coins.ShouldBe(3);
    }

    /// <summary>
    ///     marten#5799 / polecat#1049 for a Live aggregate: an append committed between the version read and the
    ///     events read is in the aggregate, so the fetch answers with its version, the commit isn't refused
    ///     against an event the aggregate already holds, and the entry written back is labelled with it.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task an_append_between_the_two_reads_is_the_version_the_fetch_answers_with(bool warm)
    {
        await UseCachedLiveAggregatesAsync();
        var streamId = await StartAsync(1, 1);
        if (warm) await HopAsync(streamId);
        var versionBefore = warm ? 4 : 3;
        var coinsBefore = warm ? 3 : 2;

        await using var session = theStore.LightweightSession();
        var pause = PauseBeforeTheEventsRead.Attach(session, () => AppendAsync(streamId, 10));

        var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);

        pause.Paused.ShouldBe(1);
        stream.StartingVersion.ShouldBe(versionBefore + 1);
        stream.Aggregate!.Coins.ShouldBe(coinsBefore + 10);

        stream.AppendOne(new CoinsAdded(100));
        await session.SaveChangesAsync(Ct);

        theCache.Stores[^1].Version.ShouldBe(versionBefore + 1);

        await using var next = theStore.LightweightSession();
        var again = await next.Events.FetchForWriting<Vault>(streamId, Ct);
        again.Aggregate!.Coins.ShouldBe(coinsBefore + 110);
    }

    [Fact]
    public async Task cached_aggregates_do_not_leak_across_tenants()
    {
        await UseCachedLiveAggregatesAsync(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);

        // Same stream id, two tenants, deliberately different states
        var streamId = Guid.NewGuid();
        await using (var one = theStore.LightweightSession("one"))
        {
            one.Events.StartStream<Vault>(streamId, new VaultOpened("One"), new CoinsAdded(100));
            await one.SaveChangesAsync(Ct);
        }

        await using (var two = theStore.LightweightSession("two"))
        {
            two.Events.StartStream<Vault>(streamId, new VaultOpened("Two"), new CoinsAdded(7));
            await two.SaveChangesAsync(Ct);
        }

        var expected = new Dictionary<string, int> { ["one"] = 100, ["two"] = 7 };
        foreach (var tenant in new[] { "one", "two", "one", "two" })
        {
            await using var session = theStore.LightweightSession(tenant);
            var stream = await session.Events.FetchForWriting<Vault>(streamId, Ct);
            stream.Aggregate!.Coins.ShouldBe(expected[tenant]);
            stream.AppendOne(new CoinsAdded(1));
            await session.SaveChangesAsync(Ct);
            expected[tenant]++;
        }

        theCache.Hits.ShouldBe(2);
        theCache.Keys.Select(x => x.TenantId).OrderBy(x => x).ShouldBe(["one", "two"]);
    }

    [Fact]
    public async Task caching_is_off_unless_the_aggregate_type_opts_in()
    {
        ConfigureStore(opts => opts.Events.AggregateWriteCaching.Cache = theCache);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        var streamId = await StartAsync(1, 1);

        await HopAsync(streamId);
        await HopAsync(streamId);

        theCache.Takes.ShouldBeEmpty();
        theCache.Stores.ShouldBeEmpty();
    }
}
