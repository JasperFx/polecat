using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using StronglyTypedIds;

namespace Polecat.Tests.Events;

public record Bug1135Bumped;

public class Bug1135Counter
{
    public Guid Id { get; set; }
    public int Count { get; set; }

    public void Apply(Bug1135Bumped _) => Count++;
}

public class Bug1135KeyedCounter
{
    public string Id { get; set; } = string.Empty;
    public int Count { get; set; }

    public void Apply(Bug1135Bumped _) => Count++;
}

[StronglyTypedId(Template.Guid)]
public readonly partial struct Bug1135CounterId;

public class Bug1135TypedCounter
{
    public Bug1135CounterId? Id { get; set; }
    public int Count { get; set; }

    public void Apply(Bug1135Bumped _) => Count++;
}

[StronglyTypedId(Template.String)]
public readonly partial struct Bug1135CounterKey;

public class Bug1135KeyedTypedCounter
{
    public Bug1135CounterKey? Id { get; set; }
    public int Count { get; set; }

    public void Apply(Bug1135Bumped _) => Count++;
}

/// <summary>
///     #1135: Polecat's half of marten#6147 and marten#6148 (fixed there in marten#6156), ported from Marten's
///     <c>Bug_6147_6148_fetch_plan_folds</c>.
///     <list type="bullet">
///         <item>
///             marten#6147: with the aggregate write cache on, reading back with <c>ProjectLatest</c> after
///             <c>FetchForWriting</c> and an append folded the pending events onto the instance the write cache was
///             holding for the commit, which still labelled it with the pre-append version, so the next
///             <c>FetchForWriting</c> applied the same events again.
///         </item>
///         <item>
///             marten#6148: an Inline aggregate with a strong-typed id, read by its stream's Guid or string. The
///             pending events were folded twice at commit, or <c>FetchLatest</c>/<c>ProjectLatest</c> refused the
///             id type with the aggregate identity map off.
///         </item>
///         <item>
///             The issue's unconfirmed risk: in an identity session, <c>ProjectLatest</c> on an Inline aggregate
///             folding the pending events onto the TRACKED document, which the inline projection's load at commit
///             then hands back and folds a second time.
///         </item>
///         <item>
///             marten#6145: <c>ProjectLatest</c> storing a folded Async aggregate the daemon then applies the same
///             events to. Polecat has no <c>ProjectLatest</c> by natural key, so only the stream-id shape exists.
///         </item>
///     </list>
///     None of the four fold shapes failed here: #916 had already moved <c>ProjectLatest</c> onto an uncached read
///     (<c>FetchUncachedForFolding</c>), so it never folds onto an instance the write cache, the aggregate identity
///     map or the document identity map holds. Putting <c>FetchLatest</c> back in its place fails the identity-map
///     facts with a count of 3. What did fail was the aggregate itself: Marten's declares a NULLABLE strong-typed id,
///     and Polecat refused to register one, see <c>DocumentMapping.AggregateIdType</c>.
/// </summary>
public class bug_1135_fetch_plan_folds : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task ConfigureAsync(Action<StoreOptions> configure)
    {
        ConfigureStore(configure);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private static void Register<T>(StoreOptions opts, ProjectionLifecycle lifecycle) where T : notnull
    {
        switch (lifecycle)
        {
            case ProjectionLifecycle.Live:
                // No projection at all: FetchForWriting builds it live
                break;
            case ProjectionLifecycle.Inline:
                opts.Projections.Snapshot<T>(SnapshotLifecycle.Inline);
                break;
            default:
                opts.Projections.Snapshot<T>(SnapshotLifecycle.Async);
                break;
        }
    }

    [Theory]
    [InlineData(ProjectionLifecycle.Async, false, false)]
    [InlineData(ProjectionLifecycle.Live, false, false)]
    [InlineData(ProjectionLifecycle.Inline, false, false)]
    [InlineData(ProjectionLifecycle.Async, true, false)]
    [InlineData(ProjectionLifecycle.Live, true, false)]
    [InlineData(ProjectionLifecycle.Inline, true, false)]
    [InlineData(ProjectionLifecycle.Async, true, true)]
    [InlineData(ProjectionLifecycle.Live, true, true)]
    [InlineData(ProjectionLifecycle.Inline, true, true)]
    public async Task reading_back_after_an_append_does_not_double_count_the_next_fetch_for_writing(
        ProjectionLifecycle lifecycle, bool projectLatest, bool identityMapForAggregates)
    {
        await ConfigureAsync(opts =>
        {
            Register<Bug1135Counter>(opts, lifecycle);
            opts.Projections.UseIdentityMapForAggregates = identityMapForAggregates;
            opts.Events.CacheAggregatesForWriting<Bug1135Counter>();
        });

        var id = Guid.NewGuid();
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<Bug1135Counter>(id, new Bug1135Bumped());
            await start.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<Bug1135Counter>(id, Ct);
            stream.AppendOne(new Bug1135Bumped());

            if (projectLatest)
            {
                (await session.Events.ProjectLatest<Bug1135Counter>(id, Ct))!.Count.ShouldBe(2);
            }
            else
            {
                await session.Events.FetchLatest<Bug1135Counter>(id, Ct);
            }

            await session.SaveChangesAsync(Ct);
        }

        // The write cache is in-process and survives the session: this fetch takes the entry the commit published
        await using var later = theStore.LightweightSession();
        var again = await later.Events.FetchForWriting<Bug1135Counter>(id, Ct);
        again.Aggregate!.Count.ShouldBe(2);
        again.StartingVersion.ShouldBe(2);
    }

    [Theory]
    [InlineData(ProjectionLifecycle.Async, false)]
    [InlineData(ProjectionLifecycle.Live, false)]
    [InlineData(ProjectionLifecycle.Inline, false)]
    [InlineData(ProjectionLifecycle.Async, true)]
    [InlineData(ProjectionLifecycle.Live, true)]
    [InlineData(ProjectionLifecycle.Inline, true)]
    public async Task reading_back_after_an_append_does_not_double_count_the_next_fetch_for_writing_by_string(
        ProjectionLifecycle lifecycle, bool identityMapForAggregates)
    {
        await ConfigureAsync(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            Register<Bug1135KeyedCounter>(opts, lifecycle);
            opts.Projections.UseIdentityMapForAggregates = identityMapForAggregates;
            opts.Events.CacheAggregatesForWriting<Bug1135KeyedCounter>();
        });

        var key = "bug-1135-" + Guid.NewGuid().ToString("N");
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<Bug1135KeyedCounter>(key, new Bug1135Bumped());
            await start.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<Bug1135KeyedCounter>(key, Ct);
            stream.AppendOne(new Bug1135Bumped());

            (await session.Events.ProjectLatest<Bug1135KeyedCounter>(key, Ct))!.Count.ShouldBe(2);

            await session.SaveChangesAsync(Ct);
        }

        await using var later = theStore.LightweightSession();
        var again = await later.Events.FetchForWriting<Bug1135KeyedCounter>(key, Ct);
        again.Aggregate!.Count.ShouldBe(2);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task project_latest_on_an_inline_strong_typed_id_aggregate_stores_the_committed_count(
        bool identityMapForAggregates, bool identitySession)
    {
        await ConfigureAsync(opts =>
        {
            opts.Projections.Snapshot<Bug1135TypedCounter>(SnapshotLifecycle.Inline);
            opts.Projections.UseIdentityMapForAggregates = identityMapForAggregates;
        });

        var id = Guid.NewGuid();
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<Bug1135TypedCounter>(id, new Bug1135Bumped());
            await start.SaveChangesAsync(Ct);
        }

        await using (var session = identitySession ? theStore.IdentitySession() : theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<Bug1135TypedCounter>(id, Ct);
            stream.AppendOne(new Bug1135Bumped());

            (await session.Events.ProjectLatest<Bug1135TypedCounter>(id, Ct))!.Count.ShouldBe(2);

            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var stored = await query.LoadAsync<Bug1135TypedCounter>(new Bug1135CounterId(id), Ct);
        stored!.Count.ShouldBe(2);
        stored.Id.ShouldBe(new Bug1135CounterId(id));

        var live = await query.Events.AggregateStreamAsync<Bug1135TypedCounter>(id, token: Ct);
        live!.Count.ShouldBe(2);
        live.Id.ShouldBe(new Bug1135CounterId(id));
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task project_latest_on_an_inline_string_strong_typed_id_aggregate_stores_the_committed_count(
        bool identityMapForAggregates, bool identitySession)
    {
        await ConfigureAsync(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<Bug1135KeyedTypedCounter>(SnapshotLifecycle.Inline);
            opts.Projections.UseIdentityMapForAggregates = identityMapForAggregates;
        });

        var key = "bug-1135-" + Guid.NewGuid().ToString("N");
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<Bug1135KeyedTypedCounter>(key, new Bug1135Bumped());
            await start.SaveChangesAsync(Ct);
        }

        await using (var session = identitySession ? theStore.IdentitySession() : theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<Bug1135KeyedTypedCounter>(key, Ct);
            stream.AppendOne(new Bug1135Bumped());

            (await session.Events.ProjectLatest<Bug1135KeyedTypedCounter>(key, Ct))!.Count.ShouldBe(2);

            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var stored = await query.LoadAsync<Bug1135KeyedTypedCounter>(new Bug1135CounterKey(key), Ct);
        stored!.Count.ShouldBe(2);
        stored.Id.ShouldBe(new Bug1135CounterKey(key));

        var live = await query.Events.AggregateStreamAsync<Bug1135KeyedTypedCounter>(key, token: Ct);
        live!.Count.ShouldBe(2);
        live.Id.ShouldBe(new Bug1135CounterKey(key));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task fetch_latest_on_an_inline_strong_typed_id_aggregate(bool identityMapForAggregates)
    {
        await ConfigureAsync(opts =>
        {
            opts.Projections.Snapshot<Bug1135TypedCounter>(SnapshotLifecycle.Inline);
            opts.Projections.UseIdentityMapForAggregates = identityMapForAggregates;
        });

        var id = Guid.NewGuid();
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<Bug1135TypedCounter>(id, new Bug1135Bumped());
            await start.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession();
        (await session.Events.FetchLatest<Bug1135TypedCounter>(id, Ct))!.Count.ShouldBe(1);
        (await session.Events.FetchLatest<Bug1135TypedCounter, Bug1135CounterId>(new Bug1135CounterId(id), Ct))!
            .Count.ShouldBe(1);

        session.Events.Append(id, new Bug1135Bumped());
        (await session.Events.ProjectLatest<Bug1135TypedCounter>(id, Ct))!.Count.ShouldBe(2);
    }

    /// <summary>
    ///     The issue's unconfirmed risk, settled: an identity session that already tracks the Inline aggregate's
    ///     document. If <c>ProjectLatest</c> folded the pending events onto that tracked instance, the inline
    ///     projection's load at commit would hand the same instance back and fold them again. Whatever the id type,
    ///     so a plain Guid id here; the strong-typed shapes are above.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task project_latest_in_an_identity_session_does_not_fold_onto_the_tracked_document(
        bool loadFirst, bool identityMapForAggregates)
    {
        await ConfigureAsync(opts =>
        {
            opts.Projections.Snapshot<Bug1135Counter>(SnapshotLifecycle.Inline);
            opts.Projections.UseIdentityMapForAggregates = identityMapForAggregates;
        });

        var id = Guid.NewGuid();
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<Bug1135Counter>(id, new Bug1135Bumped());
            await start.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.IdentitySession())
        {
            Bug1135Counter? tracked = null;
            if (loadFirst)
            {
                tracked = await session.LoadAsync<Bug1135Counter>(id, Ct);
                tracked!.Count.ShouldBe(1);
            }

            var stream = await session.Events.FetchForWriting<Bug1135Counter>(id, Ct);
            stream.AppendOne(new Bug1135Bumped());

            var projected = await session.Events.ProjectLatest<Bug1135Counter>(id, Ct);
            projected!.Count.ShouldBe(2);

            if (tracked != null)
            {
                tracked.Count.ShouldBe(1, "ProjectLatest folded the pending event onto the tracked document");
            }

            // And a second read in the same session does not find its own earlier fold
            (await session.Events.ProjectLatest<Bug1135Counter>(id, Ct))!.Count.ShouldBe(2);

            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Bug1135Counter>(id, Ct))!.Count.ShouldBe(2);
        (await query.Events.AggregateStreamAsync<Bug1135Counter>(id, token: Ct))!.Count.ShouldBe(2);
    }

    /// <summary>
    ///     marten#6145 by stream id: <c>ProjectLatest</c> on an Async aggregate must leave the snapshot to the daemon.
    ///     A stored fold would have the daemon apply the same events to it again. Polecat has no natural-key
    ///     <c>ProjectLatest</c>, so Marten's natural-key twin has no Polecat counterpart.
    /// </summary>
    [Fact]
    public async Task project_latest_on_an_async_aggregate_leaves_the_snapshot_to_the_daemon()
    {
        await ConfigureAsync(opts => opts.Projections.Snapshot<Bug1135Counter>(SnapshotLifecycle.Async));

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        var id = Guid.NewGuid();
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<Bug1135Counter>(id, new Bug1135Bumped(), new Bug1135Bumped());
            await start.SaveChangesAsync(Ct);
        }

        await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(30));

        await using (var session = theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<Bug1135Counter>(id, Ct);
            stream.AppendOne(new Bug1135Bumped());

            (await session.Events.ProjectLatest<Bug1135Counter>(id, Ct))!.Count.ShouldBe(3);

            await session.SaveChangesAsync(Ct);
        }

        await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(30));

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Bug1135Counter>(id, Ct))!.Count.ShouldBe(3);
        (await query.Events.AggregateStreamAsync<Bug1135Counter>(id, token: Ct))!.Count.ShouldBe(3);
    }

    /// <summary>
    ///     Found porting Marten's facts, whose aggregate declares <c>Bug6148CounterId? Id</c>: Polecat closed the
    ///     single-stream projection over <c>Nullable&lt;Bug1135CounterId&gt;</c>, which its identity source refuses
    ///     as a value type, so a nullable strong-typed id could not be registered or aggregated at all. Marten unwraps
    ///     the nullable. The Inline snapshot shape is covered above; this is the Live one, with nothing registered.
    /// </summary>
    [Fact]
    public async Task a_nullable_strong_typed_id_aggregate_can_be_aggregated_live()
    {
        await ConfigureAsync(_ => { });

        var id = Guid.NewGuid();
        await using (var start = theStore.LightweightSession())
        {
            start.Events.StartStream<Bug1135TypedCounter>(id, new Bug1135Bumped(), new Bug1135Bumped());
            await start.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<Bug1135TypedCounter>(id, Ct);
        stream.Aggregate!.Count.ShouldBe(2);
        stream.Aggregate.Id.ShouldBe(new Bug1135CounterId(id));

        (await session.Events.FetchLatest<Bug1135TypedCounter>(id, Ct))!.Count.ShouldBe(2);
        (await session.Events.AggregateStreamAsync<Bug1135TypedCounter>(id, token: Ct))!.Count.ShouldBe(2);
    }

    [Fact]
    public void a_nullable_strong_typed_id_aggregate_can_be_snapshotted_in_a_composite()
    {
        ConfigureStore(opts =>
            opts.Projections.CompositeProjectionFor("bug_1135", composite =>
                composite.Snapshot<Bug1135TypedCounter>()));

        theStore.Options.Projections.All.ShouldNotBeEmpty();
    }
}
