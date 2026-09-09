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
///     #557 / #568 — a filtered async shard has to see <see cref="Compacted{T}" /> for a stream it
///     owns. #557 fixed that store-locally, by widening the daemon loader's <c>dotnet_type</c> allow
///     list past the event types the projection declares an <c>Apply</c>/<c>Create</c> for.
///     jasperfx#796 (JasperFx 2.66.1) then closed the same gap one level up, appending the marker to
///     a single-stream projection's own event types in
///     <c>JasperFxSingleStreamProjectionBase.determineEventTypes()</c> for every store at once, and
///     #568 removed the store-local patch as redundant.
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
///         about what the stream means. It holds for a different REASON after #568 than before it, and
///         that is the point of keeping it — the ruling outlives whichever layer enforces it.
///     </para>
///     <para>
///         The rest are not made redundant by the upstream fix either. What jasperfx#796 guarantees is
///         that the marker reaches <c>IncludedEventTypes</c>;
///         <see cref="the_upstream_event_type_list_now_carries_the_marker_too" /> is the pin for that,
///         and everything else here depends on it. What it says nothing about is what Polecat's loader
///         then DOES with that list — and there are two answers, because #550's push-down bows out
///         above SQL Server's parameter ceiling and hands the filtering to a client-side check. Both
///         are Polecat's own code, nothing else pins them, and a marker dropped by either one fails
///         exactly as silently as it did before #557.
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
    ///     The pushed-down parameter array is taken FROM the allow-list set rather than built beside
    ///     it, which is what keeps the two paths agreeing about which types are admitted. This test is
    ///     what stops that being refactored apart.
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
    ///     The sibling marker, and the reason this is about a CATEGORY rather than one type.
    ///     <see cref="Archived" /> reaches a single-stream projection's declared event types from
    ///     JasperFx 2.65.0 (jasperfx#784), a release ahead of <see cref="Compacted{T}" /> — which is
    ///     why #557's local patch admitted it redundantly, and why #568 dropped both halves at once
    ///     rather than only the one the newer release covered. It is
    ///     pinned here because if the upstream behaviour ever goes away the failure mode is the same
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
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The list carries <see cref="Compacted{T}" /> because JasperFx 2.66.1 puts it there —
    ///         jasperfx#796, in <c>JasperFxSingleStreamProjectionBase.determineEventTypes()</c>. That
    ///         used not to be true, and #557's fix was Polecat adding the marker to the loader's allow
    ///         list itself; #568 deleted that patch once the upstream rule covered exactly the same
    ///         scope. The assertion below is the seam: if the marker ever stops arriving, every test in
    ///         this class fails HERE, naming the cause, rather than further down where the symptom is a
    ///         wrongly-folded aggregate.
    ///     </para>
    ///     <para>
    ///         What the tests below still say, with the marker supplied rather than patched in, is that
    ///         Polecat's own filtering does not drop it again — separately on each of the two paths
    ///         #550 can take. That is store-local code and the upstream fix does not reach it.
    ///     </para>
    /// </remarks>
    private static SingleStreamProjection<QuestParty, Guid> SnapshotProjection()
    {
        var projection = new SingleStreamProjection<QuestParty, Guid>();
        projection.AssembleAndAssertValidity();

        projection.IncludedEventTypes.ShouldContain(typeof(Compacted<QuestParty>));

        return projection;
    }

    /// <summary>
    ///     The other half of the same guarantee, now that JasperFx 2.66.1 supplies it: a single-stream
    ///     projection's declared event types carry the compaction marker — and the archival marker —
    ///     without the store doing anything. jasperfx#796.
    /// </summary>
    [Fact]
    public void the_upstream_event_type_list_now_carries_the_marker_too()
    {
        var projection = new SingleStreamProjection<QuestParty, Guid>();
        projection.AssembleAndAssertValidity();

        projection.IncludedEventTypes.ShouldContain(typeof(Compacted<QuestParty>));
        projection.IncludedEventTypes.ShouldContain(typeof(Archived));

        // and exactly once each: the append used to be re-applied on every evaluation, past the base's
        // own Distinct(), and reached the generated SQL as a repeated IN member
        projection.IncludedEventTypes.Count(x => x == typeof(Compacted<QuestParty>)).ShouldBe(1);
        projection.IncludedEventTypes.Count(x => x == typeof(Archived)).ShouldBe(1);
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
