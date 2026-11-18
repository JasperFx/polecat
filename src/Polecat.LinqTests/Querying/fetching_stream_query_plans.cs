using JasperFx;
using JasperFx.Events;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Querying;

/// <summary>
///     #370 (parity with marten#5053): <see cref="FetchStreamStatePlan" /> and
///     <see cref="FetchStreamPlan" /> wrap the raw event-stream fetches as query plans. Both implement
///     <b>both</b> <see cref="IQueryPlan{T}" /> and <see cref="IBatchQueryPlan{T}" />, so each is
///     exercised standalone and through a batch here.
/// </summary>
[Collection("integration")]
public class fetching_stream_query_plans : IntegrationContext
{
    public fetching_stream_query_plans(DefaultStoreFixture fixture) : base(fixture)
    {
    }


    /* Eight facts retired here when Polecat enrolled StreamQueryPlanCompliance (#556): the
     * standalone/batched pair for both plans, the version cap, and the two missing-stream cases.
     * The shared suite carries every one of them as a [Theory] over `batched`, which is a strictly
     * better shape than the hand-split pairs this file had -- the two routes compose their SQL
     * separately, and a theory makes it impossible to add a fact to one route and forget the other.
     *
     * The two below stay because the suite does NOT carry them, deliberately: `fromVersion` is a
     * Polecat-only parameter on FetchStreamPlan (the shared FetchStreamAsync contract has only the
     * inclusive cap the suite pins), and the one-round-trip assertion is about Polecat's batching
     * behaviour rather than about the plans. `both_plans_resolve_a_string_keyed_stream` in the
     * class below is the string-identity twin the suite does cover, kept only because it also
     * exercises Polecat's own DefaultStoreFixture wiring for a string-keyed store.
     */

    [Fact]
    public async Task fetch_stream_plan_honors_from_version()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var fetcher = batch.QueryByPlan(new FetchStreamPlan(streamId, fromVersion: 3));
        await batch.Execute(TestContext.Current.CancellationToken);

        var events = await fetcher;
        events.Count.ShouldBe(1);
        events[0].Version.ShouldBe(3);
    }


    [Fact]
    public async Task both_plans_share_one_round_trip_with_document_loads()
    {
        var streamId = await StartQuestStreamAsync();

        var target = new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 7 };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(target);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var stateFetcher = batch.QueryByPlan(new FetchStreamStatePlan(streamId));
        var eventsFetcher = batch.QueryByPlan(new FetchStreamPlan(streamId));
        var docFetcher = batch.Load<Target>(target.Id);
        await batch.Execute(TestContext.Current.CancellationToken);

        (await stateFetcher)!.Version.ShouldBe(3);
        (await eventsFetcher).Count.ShouldBe(3);
        (await docFetcher)!.Number.ShouldBe(7);
    }

    private async Task<Guid> StartQuestStreamAsync()
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Quest 1"), new QuestStarted("Quest 2"), new QuestStarted("Quest 3"));
        await session.SaveChangesAsync();
        return streamId;
    }
}

/// <summary>
///     #370: the string-identity half. Stream identity is fixed at store construction, so the
///     <c>streamKey</c> constructor overloads need their own store rather than the shared Guid fixture.
/// </summary>
public class fetching_stream_query_plans_by_string_key : IAsyncLifetime
{
    private const string Schema = "fetch_stream_plans_str";
    private DocumentStore _store = null!;

    public ValueTask InitializeAsync()
    {
        _store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.StreamIdentity = StreamIdentity.AsString;
        });

        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _store.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task both_plans_resolve_a_string_keyed_stream()
    {
        var streamKey = "quest/" + Guid.NewGuid().ToString("N");

        await using (var session = _store.LightweightSession())
        {
            session.Events.StartStream(streamKey, new QuestStarted("A"), new QuestStarted("B"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = _store.QuerySession();

        var state = await query.QueryByPlanAsync(new FetchStreamStatePlan(streamKey), TestContext.Current.CancellationToken);
        state.ShouldNotBeNull();
        state.Key.ShouldBe(streamKey);
        state.Version.ShouldBe(2);

        var batch = query.CreateBatchQuery();
        var stateFetcher = batch.QueryByPlan(new FetchStreamStatePlan(streamKey));
        var eventsFetcher = batch.QueryByPlan(new FetchStreamPlan(streamKey));
        await batch.Execute(TestContext.Current.CancellationToken);

        (await stateFetcher)!.Key.ShouldBe(streamKey);
        var events = await eventsFetcher;
        events.Count.ShouldBe(2);
        events.ShouldAllBe(x => x.StreamKey == streamKey);
    }
}

