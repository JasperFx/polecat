using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #1131 / #1050 (marten#6150): an aggregate fetched by a natural key whose lookup another aggregate owns.
///     The owner keeps the lookup current inline, so an Async read model is found by key as soon as its events
///     commit, before the daemon has caught up. MappedNaturalKeyCompliance holds the shared contract; these are
///     the Polecat-only shapes around it.
/// </summary>
public class mapped_natural_key_tests : OneOffConfigurationsContext
{
    private void configure(Action<StoreOptions>? also = null)
    {
        ConfigureStore(opts =>
        {
            #region sample_polecat_registering_mapped_natural_key_projections

            // The owner maintains the order number lookup inline...
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);

            // ...and the async view is fetched through it
            opts.Projections.Snapshot<NkOrderSummary>(SnapshotLifecycle.Async);

            #endregion

            also?.Invoke(opts);
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

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return streamId;
    }

    [Fact]
    public async Task a_mapped_key_fetches_an_async_aggregate_through_the_owners_lookup()
    {
        configure();

        var orderNumber = new OrderNumber($"MAPPED-{Guid.NewGuid():N}");
        var streamId = await startOrder(orderNumber, 2);

        await using var session = theStore.LightweightSession();

        #region sample_polecat_fetch_async_aggregate_by_mapped_natural_key

        // Current state, whether or not the daemon has caught up yet
        var latest = await session.Events.FetchLatest<NkOrderSummary, OrderNumber>(orderNumber);

        #endregion

        latest.ShouldNotBeNull();
        latest.Id.ShouldBe(streamId);
        latest.Items.ShouldBe(2);

        var stream = await session.Events.FetchForWriting<NkOrderSummary, OrderNumber>(orderNumber,
            TestContext.Current.CancellationToken);
        stream.Id.ShouldBe(streamId);
        stream.CurrentVersion.ShouldBe(3);
        stream.Aggregate!.Items.ShouldBe(2);
        stream.AppendOne(new NkOrderItemAdded("Doohickey", 5.50m));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchLatest<NkOrderSummary, OrderNumber>(orderNumber,
            TestContext.Current.CancellationToken))!.Items.ShouldBe(3);
    }

    [Fact]
    public async Task a_mapped_key_follows_the_owner_when_the_key_changes()
    {
        configure();

        var original = new OrderNumber($"MAPPED-OLD-{Guid.NewGuid():N}");
        var renamed = new OrderNumber($"MAPPED-NEW-{Guid.NewGuid():N}");
        await startOrder(original, 0);

        await using (var session = theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<NkOrderSummary, OrderNumber>(original,
                TestContext.Current.CancellationToken);
            stream.AppendOne(new NkOrderNumberChanged(renamed));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchLatest<NkOrderSummary, OrderNumber>(original,
            TestContext.Current.CancellationToken)).ShouldBeNull();
        (await query.Events.FetchLatest<NkOrderSummary, OrderNumber>(renamed,
            TestContext.Current.CancellationToken))!.Number.ShouldBe(renamed);
    }

    [Fact]
    public async Task a_mapped_key_reads_the_stored_snapshot_once_the_daemon_has_caught_up()
    {
        configure();

        var orderNumber = new OrderNumber($"MAPPED-DAEMON-{Guid.NewGuid():N}");
        var streamId = await startOrder(orderNumber, 3);

        using var daemon = await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await theStore.Database.WaitForNonStaleProjectionDataAsync(30.Seconds());
        await daemon.StopAllAsync();

        await using var query = theStore.LightweightSession();
        (await query.LoadAsync<NkOrderSummary>(streamId, TestContext.Current.CancellationToken))!.Items.ShouldBe(3);
        (await query.Events.FetchLatest<NkOrderSummary, OrderNumber>(orderNumber,
            TestContext.Current.CancellationToken))!.Items.ShouldBe(3);
    }

    [Fact]
    public async Task an_aggregate_with_only_a_mapped_key_is_never_registered_on_the_fly()
    {
        // #1131: fetching by key used to register an Inline projection for any aggregate with a [NaturalKey]
        // attribute, and [MappedNaturalKey<T>] is one. Only an owned key registers one now.
        ConfigureStore(opts => opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline));

        await using var session = theStore.LightweightSession();
        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await session.Events.FetchLatest<NkOrderSummary, OrderNumber>(new OrderNumber("MAPPED-NONE"),
                TestContext.Current.CancellationToken));
        ex.Message.ShouldContain("MappedNaturalKey");

        theStore.Options.Projections.TryFindAggregate(typeof(NkOrderSummary), out _).ShouldBeFalse();
    }

    [Fact]
    public void a_mapped_key_whose_source_is_not_registered_fails_when_the_store_is_built()
    {
        var ex = Should.Throw<InvalidProjectionException>(() =>
            ConfigureStore(opts => opts.Projections.Snapshot<NkOrderSummary>(SnapshotLifecycle.Async)));

        ex.Message.ShouldContain("NkOrderSummary.Number");
        ex.Message.ShouldContain("no aggregate projection for that type is registered");
    }

    [Fact]
    public async Task a_mapped_key_is_isolated_by_tenant()
    {
        configure(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);

        var orderNumber = new OrderNumber($"MAPPED-TENANT-{Guid.NewGuid():N}");
        var streamId = Guid.NewGuid();
        await using (var red = theStore.LightweightSession(new SessionOptions { TenantId = "red" }))
        {
            red.Events.StartStream<OrderAggregate>(streamId, new NkOrderCreated(orderNumber, "Red"),
                new NkOrderItemAdded("Widget", 1m));
            await red.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var redQuery = theStore.LightweightSession(new SessionOptions { TenantId = "red" });
        (await redQuery.Events.FetchLatest<NkOrderSummary, OrderNumber>(orderNumber,
            TestContext.Current.CancellationToken))!.Id.ShouldBe(streamId);

        await using var blueQuery = theStore.LightweightSession(new SessionOptions { TenantId = "blue" });
        (await blueQuery.Events.FetchLatest<NkOrderSummary, OrderNumber>(orderNumber,
            TestContext.Current.CancellationToken)).ShouldBeNull();
    }
}
