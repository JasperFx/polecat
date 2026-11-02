using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.TestUtils;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     Folds like <see cref="OrderAggregate" /> and adds a side-table row on every event of a stream
///     it has already seen, which is the shape that shows a fold enlisting its writes: nothing about
///     the key collides, so the row just appears.
/// </summary>
public class SideWritingHistoryProjection
    : EfCoreMultiStreamProjection<CustomerOrderHistory, string, TestDbContext>
{
    public SideWritingHistoryProjection()
    {
        Identity<CustomerOrderPlaced>(e => e.CustomerName);
    }

    protected override CustomerOrderHistory? ApplyEvent(CustomerOrderHistory? snapshot, string identity,
        IEvent @event, TestDbContext dbContext)
    {
        snapshot ??= new CustomerOrderHistory { Id = identity };

        if (@event.Data is CustomerOrderPlaced placed)
        {
            dbContext.OrderSummaries.Add(new OrderSummary
            {
                Id = placed.OrderId,
                CustomerName = placed.CustomerName,
                TotalAmount = placed.Amount,
                Status = "Folded"
            });

            snapshot.TotalOrders++;
        }

        return snapshot;
    }
}

/// <summary>
///     #869: an aggregate an EF Core projection writes lives in a <c>DbContext</c>, and two of the
///     event store's read paths did not know that.
/// </summary>
public class Bug_869_reading_an_ef_aggregate_through_the_event_store
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DocumentStore CreateStore(string schema, Action<StoreOptions> configure)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DatabaseSchemaName = schema;
            configure(opts);
        });
    }

    private static void AddOrders(StoreOptions opts)
    {
        opts.Projections.Add<OrderAggregate, Order, TestDbContext>(
            opts, new OrderAggregate(), ProjectionLifecycle.Inline);
    }

    private static async Task<DocumentStore> OrdersStoreAsync(string schema)
    {
        var store = CreateStore(schema, AddOrders);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);
        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString,
            "ef_orders", "ef_order_summaries");
        return store;
    }

    private static async Task<Guid> PlaceOrderAsync(IDocumentStore store, string customer)
    {
        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id, new OrderPlaced(id, customer, 10m, 2));
        await session.SaveChangesAsync(Ct);
        return id;
    }

    private static Task<int> CountAsync(string table, Guid id, string predicate = "1 = 1")
    {
        return EfCoreTestHelper.QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM {table} WHERE id = @id AND {predicate}", ("@id", id));
    }

    /// <summary>
    ///     What <c>FetchLatest</c> answered: null, for a stream that has an aggregate and a row, with
    ///     the same thing coming back whether the caller was reading or probing for existence.
    /// </summary>
    [Fact]
    public async Task fetch_latest_finds_an_inline_ef_aggregate()
    {
        using var store = await OrdersStoreAsync("ef869_latest");
        var id = await PlaceOrderAsync(store, "latest");

        await using (var session = store.LightweightSession())
        {
            session.Events.Append(id, new OrderShipped(id));
            await session.SaveChangesAsync(Ct);
        }

        await using var read = store.LightweightSession();
        var order = await read.Events.FetchLatest<Order>(id, Ct);

        order.ShouldNotBeNull();
        order.Id.ShouldBe(id);
        order.CustomerName.ShouldBe("latest");
        order.IsShipped.ShouldBeTrue();
    }

    [Fact]
    public async Task fetch_latest_finds_an_inline_ef_aggregate_from_a_query_session()
    {
        using var store = await OrdersStoreAsync("ef869_query");
        var id = await PlaceOrderAsync(store, "query");

        await using var query = store.QuerySession();
        var order = await query.Events.FetchLatest<Order>(id, Ct);

        order.ShouldNotBeNull();
        order.CustomerName.ShouldBe("query");
    }

    [Fact]
    public async Task fetch_latest_is_null_for_a_stream_that_does_not_exist()
    {
        using var store = await OrdersStoreAsync("ef869_missing");
        await PlaceOrderAsync(store, "someone else");

        await using var query = store.QuerySession();
        (await query.Events.FetchLatest<Order>(Guid.NewGuid(), Ct)).ShouldBeNull();
    }

    /// <summary>
    ///     #463's probe, kept: a stream the aggregate does not own is not an aggregate.
    /// </summary>
    [Fact]
    public async Task fetch_latest_is_null_for_a_stream_holding_nothing_the_aggregate_owns()
    {
        using var store = await OrdersStoreAsync("ef869_foreign");

        var id = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(id, new CustomerOrderCompleted(id, "foreign"));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = store.QuerySession();
        (await query.Events.FetchLatest<Order>(id, Ct)).ShouldBeNull();
    }

    /// <summary>
    ///     The fold reads the caller's tenant's stream, so one tenant cannot be handed another's
    ///     aggregate, which the table behind this entity could not promise on its own: it is keyed on
    ///     the id alone.
    /// </summary>
    [Fact]
    public async Task fetch_latest_answers_for_the_callers_tenant()
    {
        using var store = CreateStore("ef869_tenanted", opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Projections.Add<TenantedOrderAggregate, TenantedOrder, TenantedTestDbContext>(
                opts, new TenantedOrderAggregate(), ProjectionLifecycle.Inline);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TenantedTestDbContext>(ConnectionSource.ConnectionString);
        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString, "ef_tenanted_orders");

        var id = Guid.NewGuid();
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "tenant-a" }))
        {
            session.Events.StartStream(id, new OrderPlaced(id, "tenant a's", 10m, 2));
            await session.SaveChangesAsync(Ct);
        }

        await using var mine = store.QuerySession(new SessionOptions { TenantId = "tenant-a" });
        (await mine.Events.FetchLatest<TenantedOrder>(id, Ct)).ShouldNotBeNull().CustomerName.ShouldBe("tenant a's");

        await using var theirs = store.QuerySession(new SessionOptions { TenantId = "tenant-b" });
        (await theirs.Events.FetchLatest<TenantedOrder>(id, Ct)).ShouldBeNull();
    }

    /// <summary>
    ///     Reading the empty table meant ensuring it first, so asking for an EF aggregate created a
    ///     Polecat table for a document Polecat does not store: #846's rule, from the read side.
    /// </summary>
    [Fact]
    public async Task fetch_latest_invents_no_polecat_table_for_an_ef_document()
    {
        using var store = await OrdersStoreAsync("ef869_table");
        var id = await PlaceOrderAsync(store, "table");

        await using var query = store.QuerySession();
        await query.Events.FetchLatest<Order>(id, Ct);

        var tables = await EfCoreTestHelper.QueryScalarAsync<int>(
            "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id " +
            "WHERE s.name = @schema AND t.name = 'pc_doc_order'", ("@schema", "ef869_table"));

        tables.ShouldBe(0);
    }

    /// <summary>
    ///     <c>ProjectLatest</c> starts from <c>FetchLatest</c>, so it lost everything the stream held
    ///     before this session.
    /// </summary>
    [Fact]
    public async Task project_latest_folds_pending_events_onto_what_is_stored()
    {
        using var store = await OrdersStoreAsync("ef869_project");
        var id = await PlaceOrderAsync(store, "project");

        await using var session = store.LightweightSession();
        session.Events.Append(id, new OrderShipped(id));

        var order = await session.Events.ProjectLatest<Order>(id, Ct);

        order.ShouldNotBeNull();
        order.CustomerName.ShouldBe("project");
        order.IsShipped.ShouldBeTrue();

        await session.SaveChangesAsync(Ct);
        (await CountAsync("ef_orders", id, "is_shipped = 1")).ShouldBe(1);
        (await CountAsync("ef_order_summaries", id)).ShouldBe(1);
    }

    /// <summary>
    ///     The fold of <see cref="OrderPlaced" /> adds an <see cref="OrderSummary" />. That is the
    ///     inline projection's write, made once already, and it used to be flushed again at save as a
    ///     primary key violation, so no EF aggregate with a side effect could be written to this way.
    /// </summary>
    [Fact]
    public async Task fetch_for_writing_can_save()
    {
        using var store = await OrdersStoreAsync("ef869_write");
        var id = await PlaceOrderAsync(store, "write");

        await using var session = store.LightweightSession();
        var stream = await session.Events.FetchForWriting<Order>(id, Ct);

        stream.Aggregate.ShouldNotBeNull().CustomerName.ShouldBe("write");

        stream.AppendOne(new OrderShipped(id));
        await session.SaveChangesAsync(Ct);

        (await CountAsync("ef_orders", id, "is_shipped = 1")).ShouldBe(1);
        (await CountAsync("ef_order_summaries", id)).ShouldBe(1);
    }

    /// <summary>
    ///     The same fold with the session's transaction open, where its context is attached to the
    ///     connection the save goes on to use. Letting go of the context must leave that alone.
    /// </summary>
    [Fact]
    public async Task fetch_for_exclusive_writing_can_save()
    {
        using var store = await OrdersStoreAsync("ef869_exclusive");
        var id = await PlaceOrderAsync(store, "exclusive");

        await using var session = store.LightweightSession();
        var stream = await session.Events.FetchForExclusiveWriting<Order>(id, Ct);

        stream.Aggregate.ShouldNotBeNull();

        stream.AppendOne(new OrderShipped(id));
        await session.SaveChangesAsync(Ct);

        (await CountAsync("ef_orders", id, "is_shipped = 1")).ShouldBe(1);
        (await CountAsync("ef_order_summaries", id)).ShouldBe(1);
    }

    /// <summary>
    ///     A fold that leads to no append at all, which should be a read and nothing more.
    /// </summary>
    [Fact]
    public async Task fetching_for_writing_and_appending_nothing_writes_nothing()
    {
        using var store = await OrdersStoreAsync("ef869_nothing");
        var id = await PlaceOrderAsync(store, "nothing");

        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString, "ef_order_summaries");

        await using var session = store.LightweightSession();
        await session.Events.FetchForWriting<Order>(id, Ct);
        await session.SaveChangesAsync(Ct);

        (await CountAsync("ef_order_summaries", id)).ShouldBe(0);
    }

    [Fact]
    public async Task a_live_fold_on_a_session_enlists_nothing_at_save()
    {
        using var store = await OrdersStoreAsync("ef869_fold");
        var id = await PlaceOrderAsync(store, "fold");

        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString, "ef_order_summaries");

        await using var session = store.LightweightSession();
        (await session.Events.AggregateStreamAsync<Order>(id, token: Ct)).ShouldNotBeNull();

        // A second stream, so the save has real work and reaches its participants.
        await PlaceOrderInAsync(session);
        await session.SaveChangesAsync(Ct);

        (await CountAsync("ef_order_summaries", id)).ShouldBe(0);
    }

    private static Task PlaceOrderInAsync(IDocumentSession session)
    {
        var other = Guid.NewGuid();
        session.Events.StartStream(other, new OrderPlaced(other, "other", 1m, 1));
        return Task.CompletedTask;
    }

    /// <summary>
    ///     The multi-stream base class had the same branch. The event store has no route to it, because
    ///     <c>AggregatorFor</c> does not resolve a multi-stream projection as the aggregator of its
    ///     document, so this goes straight to the method.
    /// </summary>
    [Fact]
    public async Task a_multi_stream_fold_on_a_session_enlists_nothing_at_save()
    {
        var projection = new SideWritingHistoryProjection();

        using var store = CreateStore("ef869_multi", opts =>
        {
            opts.Projections.Add<SideWritingHistoryProjection, CustomerOrderHistory, string, TestDbContext>(
                opts, projection, ProjectionLifecycle.Inline);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);
        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString,
            "ef_order_summaries", "ef_customer_order_histories");

        var stream = Guid.NewGuid();
        var order = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(stream, new CustomerOrderPlaced(order, "multi", 5m));
            await session.SaveChangesAsync(Ct);
        }

        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString, "ef_order_summaries");

        await using var fold = store.LightweightSession();
        var events = await fold.Events.FetchStreamAsync(stream, token: Ct);

        var (history, _) = await projection.DetermineActionAsync(fold, null, "multi",
            new NulloIdentitySetter<CustomerOrderHistory, string>(), events, Ct);

        history.ShouldNotBeNull().TotalOrders.ShouldBe(1);

        fold.Events.Append(stream, new CustomerOrderCompleted(order, "multi"));
        await fold.SaveChangesAsync(Ct);

        (await CountAsync("ef_order_summaries", order)).ShouldBe(0);
    }

    /// <summary>
    ///     With nothing enlisted, nothing else would dispose the context this fold built, so it has to
    ///     go when the fold does rather than when the session does.
    /// </summary>
    [Fact]
    public async Task a_fold_on_a_session_disposes_its_context_straight_away()
    {
        using var store = CreateStore("ef869_disposal", opts =>
        {
            opts.Projections.Add<TrackedOrderAggregate, Order, DisposalTrackingDbContext>(
                opts, new TrackedOrderAggregate(), ProjectionLifecycle.Inline);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);

        var id = await PlaceOrderAsync(store, "disposal");

        DisposalTrackingDbContext.Reset();

        await using var session = store.LightweightSession();
        await session.Events.FetchForWriting<Order>(id, Ct);

        DisposalTrackingDbContext.Created.ShouldBe(1);
        DisposalTrackingDbContext.Disposed.ShouldBe(1);
    }
}
