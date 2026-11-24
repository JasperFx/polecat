using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #1131 / #1050 (marten#6150): fetching by natural key with an expected version, the many-stream form, the
///     one command a fetch takes, and the session's own aggregates.
/// </summary>
public class natural_key_fetch_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void configure(bool live = false, bool cache = false)
    {
        ConfigureStore(opts =>
        {
            if (live)
            {
                opts.Projections.Add(new SingleStreamProjection<OrderAggregate, Guid>(), ProjectionLifecycle.Live);
            }
            else
            {
                opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
            }

            opts.Projections.Snapshot<NkOrderSummary>(SnapshotLifecycle.Async);
            if (cache) opts.Events.CacheAggregatesForWriting<OrderAggregate>();
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
    public async Task fetch_for_writing_by_natural_key_at_the_expected_version()
    {
        configure();

        var order = aNumber("EV");
        var orderId = await startOrder(order, 2);

        await using var session = theStore.LightweightSession();

        #region sample_polecat_fetch_for_writing_by_natural_key_with_expected_version

        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, 3);

        #endregion

        stream.Id.ShouldBe(orderId);
        stream.CurrentVersion.ShouldBe(3);

        stream.AppendOne(new NkOrderItemAdded("Gadget", 5m));
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchLatest<OrderAggregate, OrderNumber>(order, Ct))!.TotalAmount.ShouldBe(25m);
    }

    [Fact]
    public async Task an_unknown_natural_key_is_a_new_stream_at_version_zero()
    {
        configure();

        await using var session = theStore.LightweightSession();

        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(aNumber("EV-NONE"), 0, Ct);
        stream.CurrentVersion.ShouldBe(0);
        stream.Aggregate.ShouldBeNull();

        await Should.ThrowAsync<ConcurrencyException>(() =>
            session.Events.FetchForWriting<OrderAggregate, OrderNumber>(aNumber("EV-NONE"), 1, Ct));
    }

    public static TheoryData<bool, bool> EveryShape() => new()
    {
        { false, false }, { false, true }, { true, false }, { true, true }
    };

    [Theory]
    [MemberData(nameof(EveryShape))]
    public async Task a_stale_version_leaves_the_session_as_it_was(bool live, bool cache)
    {
        configure(live, cache);

        var order = aNumber("EV-STALE");
        var orderId = await startOrder(order, 2);

        await using var session = theStore.LightweightSession();

        // Checked before the fetch enlists anything, as a fetch by stream id is
        await Should.ThrowAsync<ConcurrencyException>(() =>
            session.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, 2, Ct));

        session.PendingChanges.Streams.ShouldBeEmpty();

        // So the session carries on as though the stale fetch never happened
        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, 3, Ct);
        stream.AppendOne(new NkOrderItemAdded("Gadget", 5m));
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchStreamStateAsync(orderId, Ct))!.Version.ShouldBe(4);
    }

    [Theory]
    [MemberData(nameof(EveryShape))]
    public async Task a_fetch_by_natural_key_writes_back_through_the_write_cache(bool live, bool cache)
    {
        configure(live, cache);

        var order = aNumber("CACHE");
        await startOrder(order, 1);

        // Three writers one after the other, each folding the last one's commit
        for (var i = 0; i < 3; i++)
        {
            await using var session = theStore.LightweightSession();
            var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, Ct);
            stream.Aggregate!.TotalAmount.ShouldBe(10m + i * 5m);
            stream.AppendOne(new NkOrderItemAdded("Gadget", 5m));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchLatest<OrderAggregate, OrderNumber>(order, Ct))!.TotalAmount.ShouldBe(25m);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task every_fetch_by_natural_key_is_one_round_trip(bool live)
    {
        configure(live);

        var order = aNumber("ONE-TRIP");
        await startOrder(order, 2);

        // Warm up the schema checks, which are not the fetch's own
        await using (var warm = theStore.LightweightSession())
        {
            await warm.Events.FetchLatest<OrderAggregate, OrderNumber>(order, Ct);
            await warm.Events.FetchLatest<NkOrderSummary, OrderNumber>(order, Ct);
        }

        await using var session = theStore.LightweightSession();

        var before = session.RequestCount;
        (await session.Events.FetchLatest<OrderAggregate, OrderNumber>(order, Ct))!.TotalAmount.ShouldBe(20m);
        session.RequestCount.ShouldBe(before + 1);

        before = session.RequestCount;
        (await session.Events.FetchLatest<NkOrderSummary, OrderNumber>(order, Ct))!.Items.ShouldBe(2);
        session.RequestCount.ShouldBe(before + 1);

        before = session.RequestCount;
        (await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, Ct)).CurrentVersion.ShouldBe(3);
        session.RequestCount.ShouldBe(before + 1);

        before = session.RequestCount;
        (await session.Events.FetchForWriting<NkOrderSummary, OrderNumber>(order, 3, Ct)).Aggregate!.Items.ShouldBe(2);
        session.RequestCount.ShouldBe(before + 1);
    }

    [Fact]
    public async Task an_exclusive_fetch_by_natural_key_locks_the_stream_until_the_session_commits()
    {
        configure();

        var order = aNumber("EXCLUSIVE");
        await startOrder(order, 1);

        await using var first = theStore.LightweightSession();
        var locked = await first.Events.FetchForExclusiveWriting<OrderAggregate, OrderNumber>(order, Ct);

        await using var second = theStore.LightweightSession();
        var waiting = second.Events.FetchForExclusiveWriting<OrderAggregate, OrderNumber>(order, Ct);
        await Task.Delay(500, Ct);
        waiting.IsCompleted.ShouldBeFalse();

        locked.AppendOne(new NkOrderItemAdded("Gadget", 5m));
        await first.SaveChangesAsync(Ct);

        (await waiting).Aggregate!.TotalAmount.ShouldBe(15m);
    }

    [Fact]
    public async Task fetch_many_for_writing_by_natural_key()
    {
        configure();

        var first = aNumber("MANY-1");
        var second = aNumber("MANY-2");
        var unknown = aNumber("MANY-NONE");
        var firstId = await startOrder(first, 1);
        var secondId = await startOrder(second, 2);

        await using var session = theStore.LightweightSession();

        #region sample_polecat_fetch_many_for_writing_by_natural_key

        var streams = await session.Events.FetchManyForWriting<OrderAggregate, OrderNumber>([first, second, unknown]);

        #endregion

        streams[0].Id.ShouldBe(firstId);
        streams[0].Aggregate!.TotalAmount.ShouldBe(10m);
        streams[1].Id.ShouldBe(secondId);
        streams[1].Aggregate!.TotalAmount.ShouldBe(20m);

        // An unknown key gets a new stream in its place
        streams[2].Aggregate.ShouldBeNull();
        streams[2].StartingVersion.ShouldBe(0);

        streams[0].AppendOne(new NkOrderItemAdded("Gadget", 1m));
        streams[1].AppendOne(new NkOrderItemAdded("Gadget", 1m));
        streams[2].AppendOne(new NkOrderCreated(unknown, "Bob"));
        await session.SaveChangesAsync(Ct);

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchLatest<OrderAggregate, OrderNumber>(unknown, Ct))!.Id.ShouldBe(streams[2].Id);
    }

    [Fact]
    public async Task fetch_many_for_writing_refuses_a_null_or_repeated_key()
    {
        configure();
        await using var session = theStore.LightweightSession();

        var key = aNumber("MANY-REPEAT");
        await Should.ThrowAsync<ArgumentException>(() =>
            session.Events.FetchManyForWriting<OrderAggregate, OrderNumber>([key, key], Ct));
        await Should.ThrowAsync<ArgumentNullException>(() =>
            session.Events.FetchManyForWriting<OrderAggregate, OrderNumber>([key, null!], Ct));
        await Should.ThrowAsync<ArgumentException>(() =>
            session.Events.FetchManyForWriting<NkSkuProduct, NkSku?>([new NkSku("A")], Ct));
    }

    [Fact]
    public async Task fetch_latest_by_natural_key_answers_from_the_aggregate_identity_map()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
            opts.Projections.UseIdentityMapForAggregates = true;
        });

        var order = aNumber("IDENTITY-MAP");
        await startOrder(order, 1);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, Ct);

        var before = session.RequestCount;
        var latest = await session.Events.FetchLatest<OrderAggregate, OrderNumber>(order, Ct);
        latest.ShouldBeSameAs(stream.Aggregate);
        session.RequestCount.ShouldBe(before);
    }

    [Fact]
    public async Task a_generic_fetch_by_the_stream_identity_type_fetches_by_stream_id()
    {
        configure();

        var order = aNumber("BY-ID");
        var orderId = await startOrder(order, 1);

        await using var session = theStore.LightweightSession();
        (await session.Events.FetchLatest<OrderAggregate, Guid>(orderId, Ct))!.OrderNum.ShouldBe(order);
        (await session.Events.FetchForWriting<OrderAggregate, Guid>(orderId, 2, Ct)).Id.ShouldBe(orderId);
    }
}

/// <summary>
///     #1131 (marten#6150): a struct key declared <c>TKey?</c> on the aggregate is fetched by <c>TKey</c>, and a null
///     key never matches a stream.
/// </summary>
public class nullable_natural_key_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task fetching_by_the_nullable_key_type_is_refused()
    {
        ConfigureStore(opts => opts.Projections.Snapshot<NkSkuProduct>(SnapshotLifecycle.Inline));

        // The caller knows whether it has a key, so it handles null itself and fetches by NkSku
        NkSku? asNullable = new NkSku("SKU-2");

        await using var session = theStore.LightweightSession();
        var ex = await Should.ThrowAsync<ArgumentException>(async () =>
            await session.Events.FetchLatest<NkSkuProduct, NkSku?>(asNullable, Ct));
        ex.Message.ShouldContain("Fetch by");

        await Should.ThrowAsync<ArgumentException>(() => session.Events.FetchForWriting<NkSkuProduct, NkSku?>(asNullable, Ct));
        await Should.ThrowAsync<ArgumentException>(() => session.Events.FetchForWriting<NkSkuProduct, NkSku?>(asNullable, 1, Ct));
        await Should.ThrowAsync<ArgumentException>(() =>
            session.Events.FetchForExclusiveWriting<NkSkuProduct, NkSku?>(asNullable, Ct));

        var batch = session.CreateBatchQuery();
        Should.Throw<ArgumentException>(() => batch.Events.FetchForWriting<NkSkuProduct, NkSku?>(asNullable));
    }

    [Fact]
    public async Task a_null_key_is_refused_for_writing_and_finds_nothing_for_reading()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Snapshot<NkSkuProduct>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
        });

        await using var session = theStore.LightweightSession();

        // As an empty Guid or a null string stream id is: no stream to load, and no key for a new one to claim.
        // default(NkSku) is a key whose value is null, which is what a missing value binds to.
        await Should.ThrowAsync<ArgumentOutOfRangeException>(() =>
            session.Events.FetchForWriting<NkSkuProduct, NkSku>(default, Ct));
        await Should.ThrowAsync<ArgumentOutOfRangeException>(() =>
            session.Events.FetchForWriting<NkSkuProduct, NkSku>(default, 1, Ct));
        await Should.ThrowAsync<ArgumentOutOfRangeException>(() =>
            session.Events.FetchForExclusiveWriting<NkSkuProduct, NkSku>(default, Ct));
        await Should.ThrowAsync<ArgumentOutOfRangeException>(() =>
            session.Events.FetchForWriting<OrderAggregate, OrderNumber>(null!, Ct));

        var batch = session.CreateBatchQuery();
        Should.Throw<ArgumentOutOfRangeException>(() => batch.Events.FetchForWriting<NkSkuProduct, NkSku>(default));

        // A read finds nothing, without a query
        var before = session.RequestCount;
        (await session.Events.FetchLatest<NkSkuProduct, NkSku>(default, Ct)).ShouldBeNull();
        (await session.Events.FetchLatest<OrderAggregate, OrderNumber>(null!, Ct)).ShouldBeNull();
        session.RequestCount.ShouldBe(before);

        var reads = session.CreateBatchQuery();
        var latest = reads.Events.FetchLatest<NkSkuProduct, NkSku>(default);
        await reads.Execute(Ct);
        (await latest).ShouldBeNull();
    }

    [Fact]
    public async Task a_source_returning_the_nullable_key_renames_the_stream()
    {
        ConfigureStore(opts => opts.Projections.Snapshot<NkSkuProduct>(SnapshotLifecycle.Inline));

        var original = new NkSku($"SKU-OLD-{Guid.NewGuid():N}");
        var renamed = new NkSku($"SKU-NEW-{Guid.NewGuid():N}");
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<NkSkuProduct>(Guid.NewGuid(), new NkSkuListed(original, "Doohickey"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<NkSkuProduct, NkSku>(original, Ct);
            stream.AppendOne(new NkSkuChanged(renamed));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchLatest<NkSkuProduct, NkSku>(original, Ct)).ShouldBeNull();
        (await query.Events.FetchLatest<NkSkuProduct, NkSku>(renamed, Ct))!.Sku.ShouldBe(renamed);
    }

    [Fact]
    public async Task setting_the_key_back_to_null_retires_it_and_another_stream_can_claim_it()
    {
        ConfigureStore(opts => opts.Projections.Snapshot<NkSkuProduct>(SnapshotLifecycle.Inline));

        var sku = new NkSku($"SKU-FREED-{Guid.NewGuid():N}");
        var first = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<NkSkuProduct>(first, new NkSkuListed(sku, "First"));
            await session.SaveChangesAsync(Ct);
            session.Events.Append(first, new NkSkuChanged(null));
            await session.SaveChangesAsync(Ct);
        }

        await using (var query = theStore.LightweightSession())
        {
            (await query.Events.FetchLatest<NkSkuProduct, NkSku>(sku, Ct)).ShouldBeNull();
        }

        var second = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<NkSkuProduct>(second, new NkSkuListed(sku, "Second"));
            await session.SaveChangesAsync(Ct);
        }

        await using var reader = theStore.LightweightSession();
        (await reader.Events.FetchLatest<NkSkuProduct, NkSku>(sku, Ct))!.Id.ShouldBe(second);
    }

    [Fact]
    public async Task a_nullable_struct_key_is_auto_registered_on_first_fetch()
    {
        // No projection registered: the first FetchForWriting by NkSku registers an Inline snapshot
        ConfigureStore(_ => { });

        var sku = new NkSku($"SKU-AUTO-{Guid.NewGuid():N}");
        await using (var session = theStore.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<NkSkuProduct, NkSku>(sku, Ct);
            stream.Aggregate.ShouldBeNull();
            stream.AppendOne(new NkSkuListed(sku, "Thingamajig"));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchLatest<NkSkuProduct, NkSku>(sku, Ct))!.Name.ShouldBe("Thingamajig");
    }

    [Fact]
    public async Task a_non_nullable_mapped_key_reads_a_nullable_owned_key()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Snapshot<NkSkuProduct>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<NkSkuStock>(SnapshotLifecycle.Async);
        });

        var sku = new NkSku($"SKU-MAPPED-{Guid.NewGuid():N}");
        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<NkSkuProduct>(streamId, new NkSkuListed(sku, "Gizmo"),
                new NkSkuRestocked(3));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.LightweightSession();
        var stock = await query.Events.FetchLatest<NkSkuStock, NkSku>(sku, Ct);
        stock.ShouldNotBeNull();
        stock.Id.ShouldBe(streamId);
        stock.Stock.ShouldBe(3);
    }
}

/// <summary>
///     #1131 (marten#6150): a natural key of the store's own stream identity type would be fetched as a stream id,
///     so the store refuses it when it is built. A bare <c>string</c> key on a Guid store is a natural key.
/// </summary>
public class natural_key_stream_identity_clash_tests : OneOffConfigurationsContext
{
    [Fact]
    public void a_string_key_on_a_string_identity_store_is_refused()
    {
        var ex = Should.Throw<InvalidProjectionException>(() => ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<NkClashLabel>(SnapshotLifecycle.Inline);
        }));

        ex.Message.ShouldContain("NkClashLabel.Label has type string, the same type as this store's stream identity");
        ex.Message.ShouldContain("Wrap it in a strong-typed identifier");
    }

    [Fact]
    public void a_string_key_on_a_guid_identity_store_is_fine()
    {
        ConfigureStore(opts => opts.Projections.Snapshot<NkClashLabelByGuid>(SnapshotLifecycle.Inline));
        theStore.ShouldNotBeNull();
    }

    [Fact]
    public async Task a_wrapped_string_key_on_a_string_identity_store_is_fetched_by_key()
    {
        ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<NkClashWrappedLabel>(SnapshotLifecycle.Inline);
        });

        var streamKey = "stream-" + Guid.NewGuid().ToString("N");
        var label = new NkClashLabelValue("L-" + Guid.NewGuid().ToString("N"));
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<NkClashWrappedLabel>(streamKey, new NkClashWrappedLabelCreated(label));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.LightweightSession();
        (await query.Events.FetchLatest<NkClashWrappedLabel, NkClashLabelValue>(label,
            TestContext.Current.CancellationToken))!.Id.ShouldBe(streamKey);
        (await query.Events.FetchForWriting<NkClashWrappedLabel, NkClashLabelValue>(label,
            TestContext.Current.CancellationToken)).Key.ShouldBe(streamKey);

        // And a string is the stream key
        (await query.Events.FetchLatest<NkClashWrappedLabel, string>(streamKey,
            TestContext.Current.CancellationToken))!.Label.ShouldBe(label);
    }
}

public record NkClashLabelCreated(string Label);

public partial class NkClashLabel
{
    public string Id { get; set; } = string.Empty;

    [NaturalKey]
    public string Label { get; set; } = string.Empty;

    [NaturalKeySource]
    public void Apply(NkClashLabelCreated e) => Label = e.Label;
}

public partial class NkClashLabelByGuid
{
    public Guid Id { get; set; }

    [NaturalKey]
    public string Label { get; set; } = string.Empty;

    [NaturalKeySource]
    public void Apply(NkClashLabelCreated e) => Label = e.Label;
}

public readonly record struct NkClashLabelValue(string Value);

public record NkClashWrappedLabelCreated(NkClashLabelValue Label);

public partial class NkClashWrappedLabel
{
    public string Id { get; set; } = string.Empty;

    [NaturalKey]
    public NkClashLabelValue Label { get; set; }

    [NaturalKeySource]
    public void Apply(NkClashWrappedLabelCreated e) => Label = e.Label;
}
