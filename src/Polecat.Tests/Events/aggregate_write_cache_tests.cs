using System.Diagnostics.CodeAnalysis;
using JasperFx.Events.Fetching;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

public record VaultOpened(string Owner);

public record CoinsAdded(int Amount);

public class Vault
{
    public Guid Id { get; set; }
    public string Owner { get; set; } = string.Empty;
    public int Coins { get; set; }

    public static Vault Create(VaultOpened e) => new() { Owner = e.Owner };

    public void Apply(CoinsAdded e) => Coins += e.Amount;
}

public partial class VaultProjection : SingleStreamProjection<Vault, Guid>;

/// <summary>
///     A cache that records every interaction and can be seeded with an arbitrary baseline, so a test
///     can prove which events a fetch actually read.
/// </summary>
public sealed class RecordingWriteCache : IAggregateWriteCache
{
    private readonly Dictionary<AggregateCacheKey, (object Aggregate, long Version)> _entries = new();

    public List<AggregateCacheKey> Takes { get; } = new();
    public List<(AggregateCacheKey Key, object Aggregate, long Version)> Stores { get; } = new();
    public int Hits { get; private set; }

    public bool TryTake(AggregateCacheKey key, [NotNullWhen(true)] out object? aggregate, out long version)
    {
        Takes.Add(key);

        if (_entries.Remove(key, out var entry))
        {
            aggregate = entry.Aggregate;
            version = entry.Version;
            Hits++;
            return true;
        }

        aggregate = null;
        version = 0;
        return false;
    }

    public void Store(AggregateCacheKey key, object aggregate, long version)
    {
        Stores.Add((key, aggregate, version));
        _entries[key] = (aggregate, version);
    }

    public void Evict(AggregateCacheKey key) => _entries.Remove(key);

    public void Seed(AggregateCacheKey key, object aggregate, long version) => _entries[key] = (aggregate, version);
}

/// <summary>
///     #478 / jasperfx#674, the store-specific half. The cross-store definition is
///     <c>AggregateWriteCacheCompliance</c> (enrolled in
///     <c>Compliance/polecat_aggregate_write_cache_compliance.cs</c>) and it deliberately leaves two
///     things to each store: <b>when</b> an entry is written back, and <b>what version</b> it claims.
///     Both are decisions here, and both are the kind that fail silently, so they are pinned locally.
/// </summary>
public class aggregate_write_cache_tests : OneOffConfigurationsContext
{
    private readonly RecordingWriteCache _cache = new();

    private async Task ConfigureAsync(bool enroll = true)
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Add<VaultProjection>(ProjectionLifecycle.Inline);
            opts.Events.AggregateWriteCaching.Cache = _cache;
            if (enroll)
            {
                opts.Events.CacheAggregatesForWriting<Vault>();
            }
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    private async Task<Guid> AnOpenVaultAsync()
    {
        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream<Vault>(streamId, new VaultOpened("Hilda"), new CoinsAdded(100));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return streamId;
    }

    private async Task WarmAsync(Guid streamId, int amount)
    {
        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, TestContext.Current.CancellationToken);
        stream.AppendOne(new CoinsAdded(amount));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task a_type_nobody_enrolled_never_reaches_the_cache()
    {
        await ConfigureAsync(enroll: false);
        var streamId = await AnOpenVaultAsync();

        await WarmAsync(streamId, 10);
        await WarmAsync(streamId, 5);

        // ResolveCache hands back the Nullo instance for an unenrolled type, so the store's fetch
        // path needs no branch of its own -- but it must also not have reached the configured cache.
        _cache.Takes.ShouldBeEmpty();
        _cache.Stores.ShouldBeEmpty();
    }

    [Fact]
    public async Task the_entry_is_written_back_on_commit_and_not_at_fetch()
    {
        await ConfigureAsync();
        var streamId = await AnOpenVaultAsync();

        await using (var session = theStore.LightweightSession())
        {
            var stream = await session.Events
                .FetchForWriting<Vault>(streamId, TestContext.Current.CancellationToken);

            // Take-on-read is why: storing here would republish the very instance this caller is
            // still holding, and a concurrent fetch could then take it. The commit is the point at
            // which this caller is done with it.
            _cache.Takes.ShouldHaveSingleItem();
            _cache.Stores.ShouldBeEmpty();

            stream.AppendOne(new CoinsAdded(10));
            _cache.Stores.ShouldBeEmpty("appending is not committing");

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        _cache.Stores.ShouldHaveSingleItem();
    }

    [Fact]
    public async Task a_fetch_that_appends_nothing_never_warms_the_cache()
    {
        // Not an omission -- a fetch with nothing appended has no work to commit, so
        // SaveChangesAsync returns before there is a transaction to hang the write-back off. The
        // shared suite allows either answer here, and this is the one that falls out of writing the
        // entry back on commit. It costs nothing: a stream nobody writes to is not a stream a write
        // cache exists for.
        await ConfigureAsync();
        var streamId = await AnOpenVaultAsync();

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.FetchForWriting<Vault>(streamId, TestContext.Current.CancellationToken);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        _cache.Takes.ShouldHaveSingleItem();
        _cache.Stores.ShouldBeEmpty();
    }

    [Fact]
    public async Task the_entry_claims_the_version_the_instance_actually_reflects()
    {
        // The decision most likely to be got wrong, because jasperfx#674 says to take the version
        // from the committed StreamAction -- which is right for a store whose inline projection
        // mutates the instance FetchForWriting handed out. Polecat's does not: it writes its own
        // document, and the instance stays at the version it was built from. Claiming the committed
        // version here would make the next fetch skip every event this session appended, and it
        // would do so silently -- an aggregate quietly missing writes, not an error.
        await ConfigureAsync();
        var streamId = await AnOpenVaultAsync(); // version 2

        await using (var session = theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<Vault>(streamId, TestContext.Current.CancellationToken);
            stream.AppendOne(new CoinsAdded(10)); // commits at version 3
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var stored = _cache.Stores.ShouldHaveSingleItem();
        stored.Version.ShouldBe(2, "the cached instance is the aggregate as of the fetch, not the commit");
        ((Vault)stored.Aggregate).Coins.ShouldBe(100);

        // And the round trip proves the pairing is coherent: the next fetch folds event 3 onto it.
        await using var next = theStore.LightweightSession();
        var refetched = await next.Events.FetchForWriting<Vault>(streamId, TestContext.Current.CancellationToken);
        refetched.Aggregate.ShouldNotBeNull();
        refetched.Aggregate!.Coins.ShouldBe(110);
    }

    [Fact]
    public async Task a_hit_reads_only_the_events_after_the_baseline()
    {
        // The saving, asserted directly rather than by timing. Polecat's FetchForWriting
        // live-aggregates, so what a hit removes is the re-read of the stream from version 1. A
        // doctored baseline is the cleanest proof: if the fetch had read events 1..2 it could not
        // possibly answer 1000, and if it ignored the baseline it would answer 100.
        await ConfigureAsync();
        var streamId = await AnOpenVaultAsync(); // Coins = 100 at version 2

        await WarmAsync(streamId, 10); // version 3, and the entry lands at version 2

        var key = _cache.Stores[^1].Key;
        _cache.Seed(key, new Vault { Id = streamId, Owner = "Hilda", Coins = 1000 }, 2);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, TestContext.Current.CancellationToken);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.Coins.ShouldBe(1010, "events 1 and 2 were replaced by the baseline; event 3 was folded on");
        stream.StartingVersion.ShouldBe(3);
    }

    [Fact]
    public async Task publishing_hands_the_instance_off_rather_than_sharing_it()
    {
        // With UseIdentityMapForAggregates also on, a published aggregate would otherwise be
        // reachable from two places at once: the cache, where another session can take it, and this
        // session's aggregate identity map, where ProjectLatest folds pending events onto whatever
        // it finds. One mutable object with two live owners is exactly what take-on-read prevents
        // between sessions, so publishing drops this session's handle.
        ConfigureStore(opts =>
        {
            opts.Projections.Add<VaultProjection>(ProjectionLifecycle.Inline);
            opts.Projections.UseIdentityMapForAggregates = true;
            opts.Events.AggregateWriteCaching.Cache = _cache;
            opts.Events.CacheAggregatesForWriting<Vault>();
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = await AnOpenVaultAsync();

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Vault>(streamId, TestContext.Current.CancellationToken);
        stream.AppendOne(new CoinsAdded(10));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var published = (Vault)_cache.Stores.ShouldHaveSingleItem().Aggregate;

        // Same session, after the hand-off: the identity map no longer answers with the published
        // instance, so nothing this session does next can mutate what the cache is holding.
        var latest = await session.Events.FetchLatest<Vault>(streamId, TestContext.Current.CancellationToken);
        latest.ShouldNotBeNull();
        latest.ShouldNotBeSameAs(published);
        latest!.Coins.ShouldBe(110, "and the re-read reflects the commit, which the stale handle would not have");
    }

    [Fact]
    public async Task the_key_carries_the_database_the_aggregate_was_read_from()
    {
        // In the key because database-per-tenant deployments genuinely have the same tenant id and
        // stream id in more than one physical database.
        await ConfigureAsync();
        var streamId = await AnOpenVaultAsync();

        await WarmAsync(streamId, 10);

        var key = _cache.Stores.ShouldHaveSingleItem().Key;
        key.DocumentType.ShouldBe(typeof(Vault));
        key.Id.ShouldBe(streamId);
        key.TenantId.ShouldBe(theStore.Options.Tenancy!.DefaultTenantId);
        key.DatabaseIdentifier.ShouldBe(theStore.Database.Identifier);
    }
}
