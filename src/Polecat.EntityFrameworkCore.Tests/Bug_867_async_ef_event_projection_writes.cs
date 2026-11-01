using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.EntityFrameworkCore;
using Polly;
using Polly.Retry;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     <see cref="OrderDetailProjection" /> over <see cref="DisposalTrackingDbContext" />, stamping the
///     row with the tenant the event belongs to, so a batch spanning tenants can be told apart row by
///     row and every context it built can be counted.
/// </summary>
public class TrackedOrderDetailProjection<TDbContext> : EfCoreEventProjection<TDbContext>
    where TDbContext : TestDbContext
{
    public TrackedOrderDetailProjection()
    {
        Name = "TrackedOrderDetail";
        IncludeType<OrderPlaced>();
    }

    protected override Task ProjectAsync(IEvent @event, TDbContext dbContext, IDocumentOperations operations,
        CancellationToken token)
    {
        if (@event.Data is OrderPlaced placed)
        {
            dbContext.OrderDetails.Add(new OrderDetail
            {
                Id = placed.OrderId,
                CustomerName = placed.CustomerName,
                TotalAmount = placed.Amount,
                ItemCount = placed.Items,
                Status = @event.TenantId
            });

            operations.Store(new OrderLog
            {
                Id = placed.OrderId,
                CustomerName = placed.CustomerName,
                EventType = nameof(OrderPlaced)
            });
        }

        return Task.CompletedTask;
    }
}

/// <summary>
///     <see cref="OrderDetailProjection" />, declaring the EF table it writes so a rebuild empties it.
/// </summary>
public class TornDownOrderDetailProjection : OrderDetailProjection
{
    public TornDownOrderDetailProjection()
    {
        Options.DeleteDataInTableOnTeardown("dbo.ef_order_details");
    }
}

/// <summary>
///     Tracks an entity, as a real projection part way through a batch would have, and then throws.
/// </summary>
public class ThrowingOrderDetailProjection : EfCoreEventProjection<DisposalTrackingDbContext>
{
    public ThrowingOrderDetailProjection()
    {
        IncludeType<OrderPlaced>();
    }

    protected override Task ProjectAsync(IEvent @event, DisposalTrackingDbContext dbContext,
        IDocumentOperations operations, CancellationToken token)
    {
        var placed = (OrderPlaced)@event.Data;
        dbContext.OrderDetails.Add(new OrderDetail { Id = placed.OrderId, CustomerName = placed.CustomerName });

        throw new DivideByZeroException("The projection failed");
    }
}

/// <summary>
///     Tracks its row and then throws, once, so a second save on the same session has a clean run to
///     make after the first left a half-finished context behind.
/// </summary>
public class ThrowsOnceOrderDetailProjection : EfCoreEventProjection<DisposalTrackingDbContext>
{
    private int _attempts;

    public ThrowsOnceOrderDetailProjection()
    {
        IncludeType<OrderPlaced>();
    }

    protected override Task ProjectAsync(IEvent @event, DisposalTrackingDbContext dbContext,
        IDocumentOperations operations, CancellationToken token)
    {
        var placed = (OrderPlaced)@event.Data;
        dbContext.OrderDetails.Add(new OrderDetail { Id = placed.OrderId, CustomerName = placed.CustomerName });

        return Interlocked.Increment(ref _attempts) == 1
            ? throw new DivideByZeroException("The projection failed")
            : Task.CompletedTask;
    }
}

/// <summary>
///     Fails the first flush after its statements have run, which is the failure #846 is about: the
///     rows have been written, the transaction will roll back, and the block is about to be replayed
///     into this same context.
/// </summary>
public class FailsOnceAfterFlushDbContext : DisposalTrackingDbContext
{
    private static int _attempts;

    public FailsOnceAfterFlushDbContext(DbContextOptions<FailsOnceAfterFlushDbContext> options) : base(options)
    {
    }

    public static int Attempts => Volatile.Read(ref _attempts);

    public static void ResetAttempts() => Interlocked.Exchange(ref _attempts, 0);

    public override async Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess,
        CancellationToken cancellationToken = default)
    {
        var written = await base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);

        return Interlocked.Increment(ref _attempts) == 1
            ? throw new TransientFailureAfterTheFlush()
            : written;
    }
}

/// <summary>
///     #867. The daemon disposes an event projection's session as soon as <c>ApplyAsync</c> returns,
///     and that session owned the transaction participant carrying the <c>DbContext</c>, so the
///     context was gone before the batch committed: nothing was flushed, nothing failed, and the
///     progression moved past the events, so the writes were lost rather than delayed.
/// </summary>
/// <remarks>
///     Inline coverage could not see this and the aggregate projections do not reach it: the runner
///     behind those holds its session until the batch is finished with.
/// </remarks>
public class Bug_867_async_ef_event_projection_writes
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static ShardName Shard(string projectionName) => new(projectionName, "All", 1);

    private static async Task<DocumentStore> CreateStoreAsync(string schema, Action<StoreOptions> configure)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DatabaseSchemaName = schema;

            configure(opts);
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);

        // The schema outlives the run, and these facts read the progression as a number.
        await store.Advanced.CleanAllEventDataAsync(Ct);
        await store.Advanced.CleanAsync<OrderLog>(Ct);

        return store;
    }

    private static Task<long> HighestSequenceAsync(string schema)
    {
        return EfCoreTestHelper.QueryScalarAsync<long>($"SELECT ISNULL(MAX(seq_id), 0) FROM [{schema}].pc_events");
    }

    private static async Task WaitForTheShardToStopAsync(ISubscriptionAgent agent, DocumentStore store,
        ShardName shard)
    {
        for (var attempt = 0; attempt < 120; attempt++)
        {
            // The progression moving is what the unfixed code does instead of stopping, and there is
            // no point waiting out the clock to find that out.
            if (agent.Status != AgentStatus.Running
                || await store.Database.ProjectionProgressFor(shard, Ct) > 0) return;

            await Task.Delay(250, Ct);
        }

        throw new TimeoutException($"The shard is still {agent.Status}, failure: {agent.Failure?.Detail}");
    }

    [Fact]
    public async Task an_async_event_projection_writes_its_row()
    {
        const string schema = "ef867_single";
        using var store = await CreateStoreAsync(schema, opts =>
            opts.Projections.Add<OrderDetailProjection, TestDbContext>(
                opts, new OrderDetailProjection(), ProjectionLifecycle.Async));

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "async", 42m, 3));
            await session.SaveChangesAsync(Ct);
        }

        await store.WaitForProjectionAsync();

        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, status FROM ef_order_details WHERE id = @id", ("@id", orderId));

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("async");
        row["status"].ShouldBe("Placed");

        // The document the projection wrote beside it, through the same transaction.
        await using var query = store.QuerySession();
        (await query.LoadAsync<OrderLog>(orderId, Ct)).ShouldNotBeNull();

        (await store.Database.ProjectionProgressFor(Shard(nameof(OrderDetailProjection)), Ct))
            .ShouldBe(await HighestSequenceAsync(schema));
    }

    /// <summary>
    ///     A later batch reads the row an earlier one wrote and changes it, which takes the context
    ///     through a read on its placeholder connection before it joins the batch's transaction.
    /// </summary>
    [Fact]
    public async Task a_later_batch_updates_the_row_an_earlier_batch_wrote()
    {
        using var store = await CreateStoreAsync("ef867_update", opts =>
            opts.Projections.Add<OrderDetailProjection, TestDbContext>(
                opts, new OrderDetailProjection(), ProjectionLifecycle.Async));

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "async", 42m, 3));
            await session.SaveChangesAsync(Ct);
        }

        await store.WaitForProjectionAsync();

        await using (var session = store.LightweightSession())
        {
            session.Events.Append(orderId, new OrderShipped(orderId));
            await session.SaveChangesAsync(Ct);
        }

        await store.WaitForProjectionAsync();

        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT status, is_shipped FROM ef_order_details WHERE id = @id", ("@id", orderId));

        row.ShouldNotBeNull();
        row["status"].ShouldBe("Shipped");
        row["is_shipped"].ShouldBe(true);
    }

    /// <summary>
    ///     One daemon batch, two tenants: <c>ProjectionExecution</c> opens and disposes a session per
    ///     tenant group, so every session is gone before the commit and every context has to reach it
    ///     anyway.
    /// </summary>
    [Fact]
    public async Task a_conjoined_batch_writes_every_tenants_row_and_disposes_every_context()
    {
        using var store = await CreateStoreAsync("ef867_conjoined", opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Projections.Add<TrackedOrderDetailProjection<DisposalTrackingDbContext>, DisposalTrackingDbContext>(
                opts, new TrackedOrderDetailProjection<DisposalTrackingDbContext>(), ProjectionLifecycle.Async);
        });

        var orderForRed = Guid.NewGuid();
        var orderForBlue = Guid.NewGuid();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(orderForRed, new OrderPlaced(orderForRed, "red customer", 1m, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession("blue"))
        {
            session.Events.StartStream(orderForBlue, new OrderPlaced(orderForBlue, "blue customer", 2m, 2));
            await session.SaveChangesAsync(Ct);
        }

        DisposalTrackingDbContext.Reset();

        await store.WaitForProjectionAsync();

        const string sql = "SELECT status FROM ef_order_details WHERE id = @id";

        (await EfCoreTestHelper.QueryRowAsync(sql, ("@id", orderForRed))).ShouldNotBeNull()["status"].ShouldBe("red");
        (await EfCoreTestHelper.QueryRowAsync(sql, ("@id", orderForBlue))).ShouldNotBeNull()["status"].ShouldBe("blue");

        DisposalTrackingDbContext.Created.ShouldBe(2);
        DisposalTrackingDbContext.Disposed.ShouldBe(2);
    }

    /// <summary>
    ///     The half of the issue that makes this data loss rather than a missing feature. The EF write
    ///     fails on a primary key, so the batch must commit nothing: not its progression and not the
    ///     document beside the row. And because the events are still ahead of the progression, clearing
    ///     the conflict and restarting the shard writes them.
    /// </summary>
    [Fact]
    public async Task a_failed_ef_write_holds_the_progression_back_and_loses_nothing()
    {
        const string schema = "ef867_failure";
        using var store = await CreateStoreAsync(schema, opts =>
            opts.Projections.Add<TrackedOrderDetailProjection<DisposalTrackingDbContext>, DisposalTrackingDbContext>(
                opts, new TrackedOrderDetailProjection<DisposalTrackingDbContext>(), ProjectionLifecycle.Async));

        var shard = Shard("TrackedOrderDetail");
        var orderId = Guid.NewGuid();

        await EfCoreTestHelper.QueryScalarAsync<int>(
            """
            INSERT INTO ef_order_details (id, customer_name, total_amount, item_count, is_shipped, status)
            VALUES (@id, 'in the way', 1, 1, 0, 'Conflict')
            """, ("@id", orderId));

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "projected", 42m, 3));
            await session.SaveChangesAsync(Ct);
        }

        DisposalTrackingDbContext.Reset();

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        var agent = daemon.CurrentAgents().ShouldHaveSingleItem();
        await WaitForTheShardToStopAsync(agent, store, shard);

        (await store.Database.ProjectionProgressFor(shard, Ct)).ShouldBe(0);

        await using (var query = store.QuerySession())
        {
            (await query.LoadAsync<OrderLog>(orderId, Ct)).ShouldBeNull();
        }

        agent.Status.ShouldBe(AgentStatus.Paused);

        // #650: the batch that failed before it could commit still gave its context back.
        DisposalTrackingDbContext.Created.ShouldBeGreaterThan(0);
        DisposalTrackingDbContext.Disposed.ShouldBe(DisposalTrackingDbContext.Created);

        await EfCoreTestHelper.QueryScalarAsync<int>("DELETE FROM ef_order_details WHERE id = @id", ("@id", orderId));

        await daemon.StartAgentAsync(agent.Name.Identity, Ct);
        await daemon.WaitForNonStaleData(30.Seconds());
        await daemon.StopAllAsync();

        (await EfCoreTestHelper.QueryRowAsync("SELECT customer_name FROM ef_order_details WHERE id = @id",
            ("@id", orderId))).ShouldNotBeNull()["customer_name"].ShouldBe("projected");

        (await store.Database.ProjectionProgressFor(shard, Ct)).ShouldBe(await HighestSequenceAsync(schema));

        DisposalTrackingDbContext.Disposed.ShouldBe(DisposalTrackingDbContext.Created);
    }

    /// <summary>
    ///     #846's guarantee on this route. The first attempt's flush is rolled back and the pipeline
    ///     replays the block into the same context, which only has the row to write again because the
    ///     flush did not accept its changes, and the context survives the retry to be disposed once.
    /// </summary>
    [Fact]
    public async Task a_retried_batch_commits_the_row_and_disposes_the_context_once()
    {
        const string schema = "ef867_retry";
        using var store = await CreateStoreAsync(schema, opts =>
        {
            opts.ConfigurePolly(pipeline => pipeline.AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<TransientFailureAfterTheFlush>(),
                MaxRetryAttempts = 2,
                Delay = TimeSpan.Zero
            }));

            opts.Projections.Add<TrackedOrderDetailProjection<FailsOnceAfterFlushDbContext>, FailsOnceAfterFlushDbContext>(
                opts, new TrackedOrderDetailProjection<FailsOnceAfterFlushDbContext>(), ProjectionLifecycle.Async);
        });

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "retried", 42m, 3));
            await session.SaveChangesAsync(Ct);
        }

        DisposalTrackingDbContext.Reset();
        FailsOnceAfterFlushDbContext.ResetAttempts();

        await store.WaitForProjectionAsync();

        // Not vacuous: the flush failed once and a second one ran.
        FailsOnceAfterFlushDbContext.Attempts.ShouldBe(2);

        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_order_details WHERE id = @id",
            ("@id", orderId))).ShouldBe(1);

        (await store.Database.ProjectionProgressFor(Shard("TrackedOrderDetail"), Ct))
            .ShouldBe(await HighestSequenceAsync(schema));

        DisposalTrackingDbContext.Created.ShouldBe(1);
        DisposalTrackingDbContext.Disposed.ShouldBe(1);
    }

    /// <summary>
    ///     A rebuild goes through the same batch in a different mode, and on the unfixed code reported
    ///     success having written nothing.
    /// </summary>
    [Fact]
    public async Task rebuilding_an_async_event_projection_writes_its_rows_back()
    {
        using var store = await CreateStoreAsync("ef867_rebuild", opts =>
            opts.Projections.Add<OrderDetailProjection, TestDbContext>(
                opts, new OrderDetailProjection(), ProjectionLifecycle.Async));

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "rebuilt", 42m, 3));
            await session.SaveChangesAsync(Ct);
        }

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.RebuildProjectionAsync(nameof(OrderDetailProjection), 30.Seconds(), Ct);

        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_order_details WHERE id = @id",
            ("@id", orderId))).ShouldBe(1);
    }

    /// <summary>
    ///     A rebuild replays onto whatever is in the table, and a projection that inserts needs the
    ///     table emptied first. The rows are EF's and not a document type Polecat knows about, so
    ///     that has to be declared; without it, this fails on the primary key rather than quietly
    ///     keeping the old rows, which is what it did while nothing was written at all.
    /// </summary>
    [Fact]
    public async Task rebuilding_over_existing_rows_works_with_a_declared_teardown()
    {
        using var store = await CreateStoreAsync("ef867_rebuild_teardown", opts =>
            opts.Projections.Add<TornDownOrderDetailProjection, TestDbContext>(
                opts, new TornDownOrderDetailProjection(), ProjectionLifecycle.Async));

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "rebuilt", 42m, 3));
            await session.SaveChangesAsync(Ct);
        }

        await store.WaitForProjectionAsync();

        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_order_details WHERE id = @id",
            ("@id", orderId))).ShouldBe(1);

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.RebuildProjectionAsync(nameof(TornDownOrderDetailProjection), 30.Seconds(), Ct);

        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_order_details WHERE id = @id",
            ("@id", orderId))).ShouldBe(1);
    }

    /// <summary>
    ///     #870: the participant used to be registered after the loop, so a projection that threw left
    ///     a context with nothing holding it.
    /// </summary>
    [Fact]
    public async Task an_inline_projection_that_throws_disposes_its_context()
    {
        using var store = await CreateStoreAsync("ef867_throw_inline", opts =>
            opts.Projections.Add<ThrowingOrderDetailProjection, DisposalTrackingDbContext>(
                opts, new ThrowingOrderDetailProjection(), ProjectionLifecycle.Inline));

        DisposalTrackingDbContext.Reset();

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "never saved", 1m, 1));
            await Should.ThrowAsync<DivideByZeroException>(() => session.SaveChangesAsync(Ct));
        }

        DisposalTrackingDbContext.Created.ShouldBe(1);
        DisposalTrackingDbContext.Disposed.ShouldBe(1);
    }

    /// <summary>
    ///     Why the failed context is disposed on the spot rather than registered up front so that
    ///     something owns it: the session survives the failure, so a registered context would still be
    ///     there holding its half-finished changes when the second save flushed them beside its own.
    /// </summary>
    [Fact]
    public async Task a_second_save_does_not_inherit_what_a_failed_projection_tracked()
    {
        using var store = await CreateStoreAsync("ef867_throw_once", opts =>
            opts.Projections.Add<ThrowsOnceOrderDetailProjection, DisposalTrackingDbContext>(
                opts, new ThrowsOnceOrderDetailProjection(), ProjectionLifecycle.Inline));

        DisposalTrackingDbContext.Reset();

        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "second time lucky", 1m, 1));

            await Should.ThrowAsync<DivideByZeroException>(() => session.SaveChangesAsync(Ct));
            await session.SaveChangesAsync(Ct);
        }

        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_order_details WHERE id = @id",
            ("@id", orderId))).ShouldBe(1);

        DisposalTrackingDbContext.Created.ShouldBe(2);
        DisposalTrackingDbContext.Disposed.ShouldBe(2);
    }

    [Fact]
    public async Task an_async_projection_that_throws_disposes_its_context_and_commits_nothing()
    {
        using var store = await CreateStoreAsync("ef867_throw_async", opts =>
            opts.Projections.Add<ThrowingOrderDetailProjection, DisposalTrackingDbContext>(
                opts, new ThrowingOrderDetailProjection(), ProjectionLifecycle.Async));

        var shard = Shard(nameof(ThrowingOrderDetailProjection));
        var orderId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(orderId, new OrderPlaced(orderId, "never projected", 1m, 1));
            await session.SaveChangesAsync(Ct);
        }

        DisposalTrackingDbContext.Reset();

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        var agent = daemon.CurrentAgents().ShouldHaveSingleItem();
        await WaitForTheShardToStopAsync(agent, store, shard);

        (await store.Database.ProjectionProgressFor(shard, Ct)).ShouldBe(0);

        DisposalTrackingDbContext.Created.ShouldBeGreaterThan(0);
        DisposalTrackingDbContext.Disposed.ShouldBe(DisposalTrackingDbContext.Created);

        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_order_details WHERE id = @id",
            ("@id", orderId))).ShouldBe(0);
    }
}
