using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Batching;

/// <summary>
///     #370: <c>IBatchedQuery.Events</c> — the batched counterparts of <c>FetchStreamStateAsync</c> and
///     <c>FetchStreamAsync</c>. These pin the surface directly rather than through the query plans that
///     sit on top of it, and pin that the batched read agrees with the standalone one row for row: the
///     batch item composes its own SQL, so drift between the two paths is the risk worth guarding.
/// </summary>
[Collection("integration")]
public class batch_event_fetching : IntegrationContext
{
    public batch_event_fetching(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task fetch_stream_state_in_a_batch()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var fetcher = batch.Events.FetchStreamState(streamId);
        await batch.Execute(TestContext.Current.CancellationToken);

        var state = await fetcher;
        state.ShouldNotBeNull();
        state.Id.ShouldBe(streamId);
        state.Version.ShouldBe(3);
    }

    /// <summary>
    ///     #370: <c>pc_streams.type</c> was always projected and never read, so
    ///     <c>StreamState.AggregateType</c> came back null on every stream — which would have made the
    ///     <c>StreamStateResponse.AggregateTypeName</c> wire field structurally dead. Both the standalone
    ///     and batched reads resolve it now.
    ///     <para>
    ///     #373 dropped the projection registration this test used to need: the stream writer registers the
    ///     alias as it stamps it, so a plain <c>StartStream&lt;T&gt;</c> tag is enough.
    ///     </para>
    /// </summary>
    [Fact]
    public async Task stream_state_reports_the_aggregate_type_it_was_tagged_with()
    {
        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<BatchTaggedAggregate>(streamId, new QuestStarted("Tagged"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();

        (await query.Events.FetchStreamStateAsync(streamId, TestContext.Current.CancellationToken))!.AggregateType.ShouldBe(typeof(BatchTaggedAggregate));

        var batch = query.CreateBatchQuery();
        var fetcher = batch.Events.FetchStreamState(streamId);
        await batch.Execute(TestContext.Current.CancellationToken);
        (await fetcher)!.AggregateType.ShouldBe(typeof(BatchTaggedAggregate));
    }

    /// <summary>
    ///     An untagged stream simply has no aggregate type — not an error, and not a reason to fail the
    ///     metadata read.
    /// </summary>
    [Fact]
    public async Task stream_state_reports_no_aggregate_type_for_an_untagged_stream()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var fetcher = batch.Events.FetchStreamState(streamId);
        await batch.Execute(TestContext.Current.CancellationToken);

        (await fetcher)!.AggregateType.ShouldBeNull();
    }

    [Fact]
    public async Task fetch_stream_state_is_null_for_a_missing_stream()
    {
        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var fetcher = batch.Events.FetchStreamState(Guid.NewGuid());
        await batch.Execute(TestContext.Current.CancellationToken);

        (await fetcher).ShouldBeNull();
    }

    [Fact]
    public async Task fetch_stream_in_a_batch_matches_the_standalone_fetch()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var expected = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        var batch = query.CreateBatchQuery();
        var fetcher = batch.Events.FetchStream(streamId);
        await batch.Execute(TestContext.Current.CancellationToken);

        var actual = await fetcher;
        actual.Count.ShouldBe(expected.Count);
        actual.Select(x => x.Id).ShouldBe(expected.Select(x => x.Id));
        actual.Select(x => x.Version).ShouldBe(expected.Select(x => x.Version));
        actual.Select(x => x.Sequence).ShouldBe(expected.Select(x => x.Sequence));
        actual.ShouldAllBe(x => x.StreamId == streamId);
        actual[0].Data.ShouldBeOfType<QuestStarted>().Name.ShouldBe("Quest 1");
    }

    [Fact]
    public async Task fetch_stream_applies_the_optional_filters()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var capped = batch.Events.FetchStream(streamId, version: 2);
        var from = batch.Events.FetchStream(streamId, fromVersion: 2);
        var window = batch.Events.FetchStream(streamId, version: 2, fromVersion: 2);
        await batch.Execute(TestContext.Current.CancellationToken);

        (await capped).Select(x => x.Version).ShouldBe([1, 2]);
        (await from).Select(x => x.Version).ShouldBe([2, 3]);
        (await window).Select(x => x.Version).ShouldBe([2]);
    }

    /// <summary>
    ///     The timestamp filter gets its own test because it is the one parameter whose binding differs in
    ///     kind from the rest — a DateTimeOffset going through the batch's ICommandBuilder rather than the
    ///     standalone fetch's AddWithValue.
    /// </summary>
    [Fact]
    public async Task fetch_stream_applies_the_timestamp_filter()
    {
        var streamId = await StartQuestStreamAsync();

        await using var query = theStore.QuerySession();
        var all = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        var cutoff = all[^1].Timestamp;

        var batch = query.CreateBatchQuery();
        var upToCutoff = batch.Events.FetchStream(streamId, timestamp: cutoff);
        var beforeEverything = batch.Events.FetchStream(streamId, timestamp: all[0].Timestamp.AddMinutes(-5));
        await batch.Execute(TestContext.Current.CancellationToken);

        (await upToCutoff).Count.ShouldBe(3);
        (await beforeEverything).ShouldBeEmpty();
    }

    [Fact]
    public async Task fetch_stream_is_empty_for_a_missing_stream()
    {
        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var fetcher = batch.Events.FetchStream(Guid.NewGuid());
        await batch.Execute(TestContext.Current.CancellationToken);

        (await fetcher).ShouldBeEmpty();
    }

    [Fact]
    public async Task several_event_fetches_and_a_document_load_resolve_in_one_batch()
    {
        var first = await StartQuestStreamAsync();
        var second = await StartQuestStreamAsync();

        var target = new Target { Id = Guid.NewGuid(), Color = "Green", Number = 42 };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(target);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        // Interleaved deliberately: every item reads the result set at its own ordinal, so an item that
        // consumed the wrong one would show up as a cross-wired answer here rather than as a clean error.
        var firstState = batch.Events.FetchStreamState(first);
        var doc = batch.Load<Target>(target.Id);
        var secondEvents = batch.Events.FetchStream(second);
        var secondState = batch.Events.FetchStreamState(second);
        var firstEvents = batch.Events.FetchStream(first);
        await batch.Execute(TestContext.Current.CancellationToken);

        (await firstState)!.Id.ShouldBe(first);
        (await secondState)!.Id.ShouldBe(second);
        (await firstEvents).ShouldAllBe(x => x.StreamId == first);
        (await secondEvents).ShouldAllBe(x => x.StreamId == second);
        (await doc)!.Number.ShouldBe(42);
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
///     Tagged onto a stream by <c>StartStream&lt;T&gt;</c> and registered as a projection, which is what
///     makes the persisted alias resolvable — Polecat's QuickAppend writer stores
///     <c>AggregateType.Name</c> directly and never goes through <c>StreamAction.PrepareEvents</c>, so the
///     registered projections are the source of truth for resolving it back.
/// </summary>
public class BatchTaggedAggregate
{
    public Guid Id { get; set; }

    public void Apply(QuestStarted e)
    {
    }
}
