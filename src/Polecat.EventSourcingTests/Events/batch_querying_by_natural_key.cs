using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #1131 / #1050 (marten#6150): the aggregate fetches in a batched query, by natural key and by stream id. Each
///     is one command, so any number of them, beside any other batched read, is one round trip.
/// </summary>
public class batch_querying_by_natural_key : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void configure()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<NkOrderSummary>(SnapshotLifecycle.Async);
            opts.Projections.Snapshot<NkSkuProduct>(SnapshotLifecycle.Inline);
        });
    }

    private async Task<Guid> startOrder(OrderNumber number, int items)
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<OrderAggregate>(streamId, new NkOrderCreated(number, "Alice"));
        for (var i = 0; i < items; i++)
        {
            session.Events.Append(streamId, new NkOrderItemAdded("Widget", 10m));
        }

        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private static OrderNumber aNumber(string prefix) => new($"{prefix}-{Guid.NewGuid():N}");

    [Fact]
    public async Task fetch_latest_by_natural_keys_in_one_round_trip()
    {
        configure();

        var order = aNumber("BATCH");
        var orderId = await startOrder(order, 2);

        var sku = new NkSku($"SKU-BATCH-{Guid.NewGuid():N}");
        await using (var setup = theStore.LightweightSession())
        {
            setup.Events.StartStream<NkSkuProduct>(Guid.NewGuid(), new NkSkuListed(sku, "Gadget"),
                new NkSkuRestocked(4));
            await setup.SaveChangesAsync(Ct);
        }

        await using var session = theStore.LightweightSession();

        // Not the fetches' own round trips
        await session.Events.FetchLatest<OrderAggregate, OrderNumber>(order, Ct);
        await session.Events.FetchLatest<NkSkuProduct, NkSku>(sku, Ct);

        var before = session.RequestCount;

        #region sample_polecat_batch_fetch_by_natural_key

        var batch = session.CreateBatchQuery();

        var summary = batch.Events.FetchLatest<NkOrderSummary, OrderNumber>(order);
        var owner = batch.Events.FetchLatest<OrderAggregate, OrderNumber>(order);
        var byStreamId = batch.Events.FetchLatest<OrderAggregate>(orderId);
        var product = batch.Events.FetchLatest<NkSkuProduct, NkSku>(sku);
        var missing = batch.Events.FetchLatest<NkSkuProduct, NkSku>(new NkSku("SKU-NONE"));

        // One round trip for all five
        await batch.Execute();

        #endregion

        session.RequestCount.ShouldBe(before + 1);

        (await summary)!.Items.ShouldBe(2);
        (await owner)!.TotalAmount.ShouldBe(20m);
        (await byStreamId)!.Id.ShouldBe(orderId);
        (await product)!.Stock.ShouldBe(4);
        (await missing).ShouldBeNull();
    }

    [Fact]
    public async Task fetch_for_writing_by_natural_key_and_by_stream_id_in_a_batch()
    {
        configure();

        var first = aNumber("BATCH-W1");
        var second = aNumber("BATCH-W2");
        var firstId = await startOrder(first, 1);
        var secondId = await startOrder(second, 2);
        var unknown = aNumber("BATCH-NEW");

        await using var session = theStore.LightweightSession();
        await session.Events.FetchLatest<OrderAggregate, OrderNumber>(first, Ct);

        var before = session.RequestCount;
        var batch = session.CreateBatchQuery();
        var byKey = batch.Events.FetchForWriting<OrderAggregate, OrderNumber>(first);
        var mapped = batch.Events.FetchForWriting<NkOrderSummary, OrderNumber>(second, 3);
        var byId = batch.Events.FetchForWriting<OrderAggregate>(secondId);
        var fresh = batch.Events.FetchForWriting<OrderAggregate, OrderNumber>(unknown);
        await batch.Execute(Ct);
        session.RequestCount.ShouldBe(before + 1);

        var firstStream = await byKey;
        firstStream.Id.ShouldBe(firstId);
        firstStream.Aggregate!.TotalAmount.ShouldBe(10m);

        (await mapped).Aggregate!.Items.ShouldBe(2);
        (await byId).Id.ShouldBe(secondId);

        var newStream = await fresh;
        newStream.Aggregate.ShouldBeNull();
        newStream.StartingVersion.ShouldBe(0);

        firstStream.AppendOne(new NkOrderItemAdded("Gadget", 5m));
        newStream.AppendOne(new NkOrderCreated(unknown, "Bob"));
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchLatest<OrderAggregate, OrderNumber>(first, Ct))!.TotalAmount.ShouldBe(15m);
        (await query.Events.FetchLatest<OrderAggregate, OrderNumber>(unknown, Ct))!.Id.ShouldBe(newStream.Id);
    }

    [Fact]
    public async Task a_stale_version_in_a_batch_fails_the_batch()
    {
        configure();

        var order = aNumber("BATCH-STALE");
        await startOrder(order, 2);

        await using var session = theStore.LightweightSession();

        var batch = session.CreateBatchQuery();
        var fetch = batch.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, 2);

        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await batch.Execute(Ct);
            await fetch;
        });

        session.PendingChanges.Streams.ShouldBeEmpty();
    }

    [Fact]
    public async Task an_exclusive_fetch_in_a_batch_holds_its_lock_until_the_session_commits()
    {
        configure();

        var first = aNumber("BATCH-X1");
        var second = aNumber("BATCH-X2");
        var firstId = await startOrder(first, 1);
        var secondId = await startOrder(second, 2);

        await using var session = theStore.LightweightSession();
        await session.Events.FetchLatest<OrderAggregate, OrderNumber>(first, Ct);

        var before = session.RequestCount;
        var batch = session.CreateBatchQuery();
        var owner = batch.Events.FetchLatest<OrderAggregate, OrderNumber>(second);
        var exclusive = batch.Events.FetchForExclusiveWriting<OrderAggregate, OrderNumber>(first);
        var writing = batch.Events.FetchForWriting<NkOrderSummary, OrderNumber>(first);
        var latest = batch.Events.FetchLatest<NkOrderSummary, OrderNumber>(second);
        await batch.Execute(Ct);

        // The batch's own round trip, after the one that opens the transaction
        (session.RequestCount - before).ShouldBeLessThanOrEqualTo(2);

        (await owner)!.Id.ShouldBe(secondId);
        var locked = await exclusive;
        locked.Id.ShouldBe(firstId);
        var stream = await writing;
        stream.Id.ShouldBe(firstId);
        stream.Aggregate!.Items.ShouldBe(1);
        (await latest)!.Items.ShouldBe(2);

        // The exclusive fetch's row lock lasts until SaveChangesAsync
        await using var other = theStore.LightweightSession();
        var blocked = other.Events.FetchForExclusiveWriting<OrderAggregate, OrderNumber>(first, Ct);
        await Task.Delay(500, Ct);
        blocked.IsCompleted.ShouldBeFalse();

        locked.AppendOne(new NkOrderItemAdded("Widget", 10m));
        await session.SaveChangesAsync(Ct);
        (await blocked).Aggregate!.TotalAmount.ShouldBe(20m);
    }

    [Fact]
    public async Task fetching_for_writing_in_a_batch_needs_a_document_session()
    {
        configure();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        Should.Throw<InvalidOperationException>(() =>
            batch.Events.FetchForWriting<OrderAggregate, OrderNumber>(aNumber("QUERY")));

        // A read works from a query session
        var order = aNumber("QUERY-READ");
        await startOrder(order, 1);

        var reads = query.CreateBatchQuery();
        var latest = reads.Events.FetchLatest<OrderAggregate, OrderNumber>(order);
        await reads.Execute(Ct);
        (await latest)!.TotalAmount.ShouldBe(10m);
    }
}
