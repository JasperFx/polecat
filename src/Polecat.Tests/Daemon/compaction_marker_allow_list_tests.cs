using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Events.Daemon;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #557 — the daemon loader's <c>dotnet_type</c> allow list is built from the event types a
///     projection declares an <c>Apply</c>/<c>Create</c> for, and <see cref="Compacted{T}" /> is not one
///     of them. Nothing upstream in JasperFx adds it for the store, so before the fix a projection with
///     a declared event list never saw the compaction marker for a stream it owns.
/// </summary>
/// <remarks>
///     <para>
///         What makes this worse than an ordinary dropped event: <c>CompactStreamAsync</c> DELETES the
///         events it folds. The marker is not an extra event beside the history — it is the only thing
///         left of it. A shard that filters the marker out is not seeing a partial stream, it is seeing
///         a stream that appears to begin after the compaction point, and it says so with no error and
///         no log line.
///     </para>
///     <para>
///         <see cref="a_filtered_and_an_unfiltered_load_agree_on_a_compacted_stream" /> is the
///         statement of the bug: two loads over one stream, folded by the same aggregator, disagreeing
///         about what the stream means. The rest pin the two filtering paths (#550's SQL push-down and
///         the retained client-side fallback) and the end-to-end daemon consequence.
///     </para>
/// </remarks>
public class compaction_marker_allow_list_tests : OneOffConfigurationsContext
{
    private const string Owner = "Destroy the Ring";

    public override async ValueTask InitializeAsync()
    {
        // Drops this class's schema, then stands the default store back up on it. A test that needs a
        // different configuration (the daemon one) calls ConfigureStore again and re-applies.
        await base.InitializeAsync();
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    /// <summary>
    ///     THE test. A filtered shard and an unfiltered shard read the same compacted stream and must
    ///     reach the same aggregate. Before the fix the filtered load dropped the marker, so its fold
    ///     started from nothing and lost every member who joined before compaction — while the
    ///     unfiltered load over the identical rows had them all.
    /// </summary>
    [Fact]
    public async Task a_filtered_and_an_unfiltered_load_agree_on_a_compacted_stream()
    {
        await SeedCompactedStreamAsync();
        var highWater = await HighWaterAsync();

        var unfiltered = await FoldAsync(CreateLoader(filtering: null), highWater);
        var filtered = await FoldAsync(CreateLoader(SnapshotProjection()), highWater);

        // The control: this is what the stream actually means, and no shard has any business
        // disagreeing with it.
        unfiltered.ShouldNotBeNull();
        unfiltered.Name.ShouldBe(Owner);
        unfiltered.Members.ShouldBe(["Aragorn", "Legolas", "Gimli"]);

        filtered.ShouldNotBeNull();
        filtered.Name.ShouldBe(unfiltered.Name);
        filtered.Members.ShouldBe(unfiltered.Members);
        filtered.Location.ShouldBe(unfiltered.Location);
    }

    /// <summary>
    ///     #550 pushed the allow list into the SQL as <c>dotnet_type in (...)</c>, so a type absent from
    ///     it is never read off the wire at all. This is that path: a modest allow list, well under the
    ///     parameter cap, and the marker has to survive the <c>IN</c> clause.
    /// </summary>
    [Fact]
    public async Task the_marker_survives_the_pushed_down_sql_filter()
    {
        var streamId = await SeedCompactedStreamAsync();

        var page = await LoadAsync(CreateLoader(SnapshotProjection()));

        page.Select(x => x.Data.GetType())
            .ShouldBe([typeof(Compacted<QuestParty>), typeof(MembersJoined)]);
        page.ShouldAllBe(x => x.StreamId == streamId);
    }

    /// <summary>
    ///     The other path, and the one that would otherwise rot unnoticed. Above SQL Server's
    ///     2100-parameter ceiling the loader stops pushing the filter down and the client-side check
    ///     carries it alone — so a fix applied only to the rendered <c>IN</c> clause would leave a store
    ///     with very many event types still broken, and nothing else in the suite would say so.
    /// </summary>
    /// <remarks>
    ///     The fix is applied to the allow-list SET, from which the pushed-down parameter array is
    ///     taken, which is what makes one addition cover both paths. This test is what stops that being
    ///     refactored apart.
    /// </remarks>
    [Fact]
    public async Task the_marker_survives_the_client_side_fallback_filter()
    {
        await SeedCompactedStreamAsync();

        var projection = SnapshotProjection();
        foreach (var padding in PaddingEventTypes(2_100))
        {
            projection.IncludeType(padding);
        }

        // Well past the push-down cap, so the rendered SQL carries no type predicate at all and the
        // hydration-side check is the only filter running.
        projection.IncludedEventTypes.Count.ShouldBeGreaterThan(2_000);

        var page = await LoadAsync(CreateLoader(projection));

        page.Select(x => x.Data.GetType())
            .ShouldBe([typeof(Compacted<QuestParty>), typeof(MembersJoined)]);
    }

    /// <summary>
    ///     The daemon consequence, end to end: an async single-stream projection catching up over a
    ///     stream that was compacted before the shard ever reached it. Live aggregation is the
    ///     unfiltered control — it reads the same rows through <c>PcEventsRowReader</c>, which has no
    ///     allow list — so the two have to land on the same document.
    /// </summary>
    [Fact]
    public async Task an_async_shard_folds_a_compacted_stream_the_way_live_aggregation_does()
    {
        ConfigureStore(opts => opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = await SeedCompactedStreamAsync();

        await theStore.WaitForProjectionAsync();

        await using var query = theStore.QuerySession();
        var live = await query.Events.AggregateStreamAsync<QuestParty>(streamId,
            token: TestContext.Current.CancellationToken);
        var projected = await query.LoadAsync<QuestParty>(streamId, TestContext.Current.CancellationToken);

        live.ShouldNotBeNull();
        live.Members.ShouldBe(["Aragorn", "Legolas", "Gimli"]);

        projected.ShouldNotBeNull();
        projected.Name.ShouldBe(live.Name);
        projected.Members.ShouldBe(live.Members);
    }

    /// <summary>
    ///     The sibling marker, and the reason the fix admits a CATEGORY rather than one type.
    ///     <see cref="Archived" /> reaches a single-stream projection's declared event types on its own
    ///     from JasperFx 2.65.0 (jasperfx#784), so this passed before the fix as well — it is here to
    ///     pin that, because if the upstream behaviour ever goes away the failure mode is the same
    ///     silent one: an async shard that never folds the marker never calls
    ///     <c>PolecatProjectionStorage.ArchiveStream</c>, and the stream is never archived.
    /// </summary>
    [Fact]
    public async Task the_archived_marker_is_admitted_by_a_filtered_load()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted(Owner),
            new Archived("finished"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var page = await LoadAsync(CreateLoader(SnapshotProjection()));

        page.Select(x => x.Data.GetType())
            .ShouldBe([typeof(QuestStarted), typeof(Archived)]);
    }

    // ---- seeding and plumbing ----

    /// <summary>
    ///     A stream whose history is compacted away and then appended to: the marker at the sequence of
    ///     the last folded event (<c>ReplaceEventOperation</c> writes in place), and one ordinary event
    ///     above it. Aragorn and Legolas exist ONLY inside the marker's snapshot — their events are
    ///     deleted rows.
    /// </summary>
    private async Task<Guid> SeedCompactedStreamAsync()
    {
        var streamId = Guid.NewGuid();

        theSession.Events.StartStream(streamId,
            new QuestStarted(Owner),
            new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas"]));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using (var compacting = theStore.LightweightSession())
        {
            await compacting.Events.CompactStreamAsync<QuestParty>(streamId);
            await compacting.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var appending = theStore.LightweightSession())
        {
            appending.Events.Append(streamId, new MembersJoined(2, "Moria", ["Gimli"]));
            await appending.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        return streamId;
    }

    /// <summary>
    ///     The registered form of the projection the daemon would hand the loader: a real
    ///     <see cref="IAggregateProjection" /> over <see cref="QuestParty" />, with
    ///     <c>IncludedEventTypes</c> filled from its conventional methods exactly as registration does.
    ///     Notably absent from that list, and the whole point: <c>Compacted&lt;QuestParty&gt;</c>.
    /// </summary>
    private static SingleStreamProjection<QuestParty, Guid> SnapshotProjection()
    {
        var projection = new SingleStreamProjection<QuestParty, Guid>();
        projection.AssembleAndAssertValidity();

        projection.IncludedEventTypes.ShouldNotBeEmpty();
        projection.IncludedEventTypes.ShouldNotContain(typeof(Compacted<QuestParty>));

        return projection;
    }

    /// <summary>
    ///     Filler for the allow list, purely to push it past the push-down cap. These never match a
    ///     stored row; only the SIZE of the list matters, because that is what decides which of the two
    ///     filtering paths runs.
    /// </summary>
    private static IReadOnlyList<Type> PaddingEventTypes(int count)
    {
        // Self-feeding, so it cannot come up short: every type admitted is re-enqueued wrapped in a
        // List<>, which is always a legal type argument in turn. Seeding from the several hundred
        // public corelib types rather than a handful keeps the nesting one or two deep — seeding
        // from ten would need a 200-deep nest, whose type NAME grows exponentially.
        var padding = new List<Type>(count);
        var frontier = new Queue<Type>(PaddingSeeds());

        while (padding.Count < count)
        {
            var next = frontier.Dequeue();
            padding.Add(next);
            frontier.Enqueue(typeof(List<>).MakeGenericType(next));
        }

        return padding;
    }

    /// <summary>
    ///     <c>EventGraph.EventMappingFor</c> closes <c>PolecatEventType&lt;T&gt;</c> over whatever it is
    ///     handed and constructs it, so a seed has to survive both <c>MakeGenericType</c> AND
    ///     <c>Activator.CreateInstance</c>.
    /// </summary>
    /// <remarks>
    ///     Reflection over corelib turns up types that pass every obvious property check and still
    ///     fail one of those two: <c>System.Void</c> is public and concrete but is rejected as a type
    ///     argument outright, and <c>System.__Canon</c> — the shared generic instantiation — closes
    ///     fine and then cannot be instantiated. Restricting to top-level public types screens out
    ///     <c>__Canon</c> and its kin; the rest are asked directly rather than predicted.
    /// </remarks>
    private static IEnumerable<Type> PaddingSeeds()
    {
        return typeof(object).Assembly.GetTypes()
            .Where(t => t.IsPublic
                        && !t.IsAbstract
                        && !t.IsGenericTypeDefinition
                        && !t.ContainsGenericParameters
                        && !t.IsByRefLike
                        && t != typeof(void))
            .Where(t =>
            {
                try
                {
                    return Activator.CreateInstance(typeof(List<>).MakeGenericType(t)) != null;
                }
                catch (Exception)
                {
                    return false;
                }
            });
    }

    private PolecatEventLoader CreateLoader(EventFilterable? filtering) =>
        new(theStore.Database.Events, theStore.Options, theStore.Options.ConnectionString, filtering);

    private async Task<EventPage> LoadAsync(PolecatEventLoader loader)
    {
        var highWater = await HighWaterAsync();
        return await loader.LoadAsync(CreateRequest(0, highWater), TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     Fold a whole load through the aggregator the store itself would use, which is where
    ///     <c>Compacted&lt;T&gt;.MaybeFastForward</c> runs. Starting from a null snapshot is the
    ///     rebuild/fresh-shard case — the one where the marker is the only source of the pre-compaction
    ///     history.
    /// </summary>
    private async Task<QuestParty?> FoldAsync(PolecatEventLoader loader, long highWater)
    {
        var page = await loader.LoadAsync(CreateRequest(0, highWater), TestContext.Current.CancellationToken);

        var aggregator = theStore.Options.Projections.AggregatorFor<QuestParty>();
        await using var query = theStore.QuerySession();

        return await aggregator.BuildAsync(page, query, null, TestContext.Current.CancellationToken);
    }

    private Task<long> HighWaterAsync() =>
        theStore.Database.FetchHighestEventSequenceNumber(TestContext.Current.CancellationToken);

    private static EventRequest CreateRequest(long floor, long highWater) =>
        new()
        {
            Floor = floor,
            HighWater = highWater,
            BatchSize = 100,
            Name = new ShardName("TestLoader"),
            ErrorOptions = new ErrorHandlingOptions(),
            Runtime = null!,
            Metrics = null!
        };

    private IDocumentSession? _session;

    private IDocumentSession theSession
    {
        get
        {
            if (_session == null)
            {
                _session = theStore.LightweightSession();
                AsyncDisposables.Add(_session);
            }

            return _session;
        }
    }
}
