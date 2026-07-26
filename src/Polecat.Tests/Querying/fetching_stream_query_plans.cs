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

    [Fact]
    public async Task fetch_stream_state_plan_standalone()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var state = await query.QueryByPlanAsync(new FetchStreamStatePlan(streamId));

        state.ShouldNotBeNull();
        state.Id.ShouldBe(streamId);
        state.Version.ShouldBe(3);
        state.IsArchived.ShouldBeFalse();
    }

    [Fact]
    public async Task fetch_stream_state_plan_batched()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var fetcher = batch.QueryByPlan(new FetchStreamStatePlan(streamId));
        await batch.Execute();

        var state = await fetcher;
        state.ShouldNotBeNull();
        state.Id.ShouldBe(streamId);
        state.Version.ShouldBe(3);
    }

    [Fact]
    public async Task fetch_stream_state_plan_yields_null_for_a_missing_stream()
    {
        await using var query = theStore.QuerySession();

        (await query.QueryByPlanAsync(new FetchStreamStatePlan(Guid.NewGuid()))).ShouldBeNull();

        var batch = query.CreateBatchQuery();
        var fetcher = batch.QueryByPlan(new FetchStreamStatePlan(Guid.NewGuid()));
        await batch.Execute();
        (await fetcher).ShouldBeNull();
    }

    [Fact]
    public async Task fetch_stream_plan_standalone()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var events = await query.QueryByPlanAsync(new FetchStreamPlan(streamId));

        events.Count.ShouldBe(3);
        events.Select(x => x.Version).ShouldBe([1, 2, 3]);
        events[0].Data.ShouldBeOfType<QuestStarted>().Name.ShouldBe("Quest 1");
    }

    [Fact]
    public async Task fetch_stream_plan_batched()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var fetcher = batch.QueryByPlan(new FetchStreamPlan(streamId));
        await batch.Execute();

        var events = await fetcher;
        events.Count.ShouldBe(3);
        events.Select(x => x.Version).ShouldBe([1, 2, 3]);
        events.ShouldAllBe(x => x.StreamId == streamId);
    }

    [Fact]
    public async Task fetch_stream_plan_honors_the_version_cap()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();

        // Standalone and batched must apply the filter identically — the batched item composes its own
        // SQL, so the cap is the thing most likely to drift between the two paths.
        var standalone = await query.QueryByPlanAsync(new FetchStreamPlan(streamId, version: 2));
        standalone.Count.ShouldBe(2);

        var batch = query.CreateBatchQuery();
        var fetcher = batch.QueryByPlan(new FetchStreamPlan(streamId, version: 2));
        await batch.Execute();
        (await fetcher).Count.ShouldBe(2);
    }

    [Fact]
    public async Task fetch_stream_plan_honors_from_version()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var fetcher = batch.QueryByPlan(new FetchStreamPlan(streamId, fromVersion: 3));
        await batch.Execute();

        var events = await fetcher;
        events.Count.ShouldBe(1);
        events[0].Version.ShouldBe(3);
    }

    [Fact]
    public async Task fetch_stream_plan_yields_an_empty_list_for_a_missing_stream()
    {
        await using var query = theStore.QuerySession();

        (await query.QueryByPlanAsync(new FetchStreamPlan(Guid.NewGuid()))).ShouldBeEmpty();

        var batch = query.CreateBatchQuery();
        var fetcher = batch.QueryByPlan(new FetchStreamPlan(Guid.NewGuid()));
        await batch.Execute();
        (await fetcher).ShouldBeEmpty();
    }

    [Fact]
    public async Task both_plans_share_one_round_trip_with_document_loads()
    {
        var streamId = await StartQuestStreamAsync();

        var target = new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 7 };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(target);
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var stateFetcher = batch.QueryByPlan(new FetchStreamStatePlan(streamId));
        var eventsFetcher = batch.QueryByPlan(new FetchStreamPlan(streamId));
        var docFetcher = batch.Load<Target>(target.Id);
        await batch.Execute();

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

    public Task InitializeAsync()
    {
        _store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.StreamIdentity = StreamIdentity.AsString;
        });

        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        _store.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    public async Task both_plans_resolve_a_string_keyed_stream()
    {
        var streamKey = "quest/" + Guid.NewGuid().ToString("N");

        await using (var session = _store.LightweightSession())
        {
            session.Events.StartStream(streamKey, new QuestStarted("A"), new QuestStarted("B"));
            await session.SaveChangesAsync();
        }

        await using var query = _store.QuerySession();

        var state = await query.QueryByPlanAsync(new FetchStreamStatePlan(streamKey));
        state.ShouldNotBeNull();
        state.Key.ShouldBe(streamKey);
        state.Version.ShouldBe(2);

        var batch = query.CreateBatchQuery();
        var stateFetcher = batch.QueryByPlan(new FetchStreamStatePlan(streamKey));
        var eventsFetcher = batch.QueryByPlan(new FetchStreamPlan(streamKey));
        await batch.Execute();

        (await stateFetcher)!.Key.ShouldBe(streamKey);
        var events = await eventsFetcher;
        events.Count.ShouldBe(2);
        events.ShouldAllBe(x => x.StreamKey == streamKey);
    }
}
