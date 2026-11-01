using System.Data.Common;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Polecat.Exceptions;
using Polly;
using Polly.Retry;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     Reads <c>pc_events</c> through the DbContext for the event it is projecting. That row is
///     uncommitted and exclusively locked by the session's own transaction, so only a read inside
///     that transaction can see it: from any other connection this blocks on the session's lock,
///     which is what makes it a test of the transaction and not merely of the connection.
/// </summary>
public class ReadsItsOwnEventProjection : EfCoreEventProjection<TestDbContext>
{
    private readonly string _schema;

    public ReadsItsOwnEventProjection(string schema)
    {
        _schema = schema;
        IncludeType<OrderPlaced>();
    }

    public int EventsVisibleToTheRead { get; private set; } = -1;

    protected override async Task ProjectAsync(IEvent @event, TestDbContext dbContext,
        IDocumentOperations operations, CancellationToken token)
    {
        dbContext.Database.SetCommandTimeout(2);

        EventsVisibleToTheRead = await dbContext.Database
            .SqlQueryRaw<int>($"SELECT COUNT(*) AS [Value] FROM [{_schema}].pc_events WHERE stream_id = @p0",
                new SqlParameter("@p0", @event.StreamId))
            .SingleAsync(token);
    }
}

/// <summary>
///     Ends the session's transaction from the server side, through the DbContext's own connection,
///     then folds as usual. The fold's write reaches <c>EfCoreProjectionStorage.Store</c>, whose read
///     is the one that has to notice.
/// </summary>
public class RollsBackTheSessionAggregate : EfCoreSingleStreamProjection<Order, TestDbContext>
{
    public RollsBackTheSessionAggregate()
    {
        IncludeType<OrderPlaced>();
    }

    protected override Order? ApplyEvent(Order? snapshot, Guid identity, IEvent @event,
        TestDbContext dbContext, IQuerySession session)
    {
        dbContext.Database.ExecuteSqlRaw("IF @@TRANCOUNT > 0 ROLLBACK");

        var placed = (OrderPlaced)@event.Data;
        return new Order
        {
            Id = placed.OrderId, CustomerName = placed.CustomerName, TotalAmount = placed.Amount,
            ItemCount = placed.Items
        };
    }
}

/// <summary>
///     The same server-side end to the transaction, with an entity pending and no storage read
///     between it and the flush.
/// </summary>
public class RollsBackTheSessionEventProjection : EfCoreEventProjection<TestDbContext>
{
    public RollsBackTheSessionEventProjection()
    {
        IncludeType<OrderPlaced>();
    }

    protected override Task ProjectAsync(IEvent @event, TestDbContext dbContext,
        IDocumentOperations operations, CancellationToken token)
    {
        var placed = (OrderPlaced)@event.Data;

        dbContext.OrderDetails.Add(new OrderDetail
        {
            Id = placed.OrderId, CustomerName = placed.CustomerName, TotalAmount = placed.Amount,
            ItemCount = placed.Items, Status = "Placed"
        });

        return dbContext.Database.ExecuteSqlRawAsync("IF @@TRANCOUNT > 0 ROLLBACK", token);
    }
}

public class TransientEfCommandFailure : Exception;

/// <summary>
///     Fails the first command whose text matches, before it reaches the server, which models a
///     transient failure a pipeline retry can absorb. It can only be absorbed if the command really
///     does run inside the pipeline.
/// </summary>
public class FailsOnceInterceptor : DbCommandInterceptor
{
    private static string? _commandFragment;
    private static int _failures;

    public static int Failures => Volatile.Read(ref _failures);

    public static void FailNext(string commandFragment)
    {
        _failures = 0;
        _commandFragment = commandFragment;
    }

    private static void Inspect(DbCommand command)
    {
        var fragment = _commandFragment;
        if (fragment == null || !command.CommandText.Contains(fragment, StringComparison.OrdinalIgnoreCase)) return;

        _commandFragment = null;
        Interlocked.Increment(ref _failures);
        throw new TransientEfCommandFailure();
    }

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command,
        CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        Inspect(command);
        return result;
    }

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command,
        CommandEventData eventData, InterceptionResult<DbDataReader> result,
        CancellationToken cancellationToken = default)
    {
        Inspect(command);
        return ValueTask.FromResult(result);
    }

    public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command,
        CommandEventData eventData, InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        Inspect(command);
        return ValueTask.FromResult(result);
    }
}

public class InterceptedOrdersDbContext : DbContext
{
    public InterceptedOrdersDbContext(DbContextOptions<InterceptedOrdersDbContext> options) : base(options)
    {
    }

    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.AddInterceptors(new FailsOnceInterceptor());
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("ef_orders");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.CustomerName).HasColumnName("customer_name").HasMaxLength(200);
            entity.Property(e => e.TotalAmount).HasColumnName("total_amount").HasColumnType("decimal(18,2)");
            entity.Property(e => e.ItemCount).HasColumnName("item_count");
            entity.Property(e => e.IsShipped).HasColumnName("is_shipped");
            entity.Property(e => e.IsCancelled).HasColumnName("is_cancelled");
        });
    }
}

public class InterceptedOrderAggregate : EfCoreSingleStreamProjection<Order, InterceptedOrdersDbContext>
{
    public InterceptedOrderAggregate()
    {
        IncludeType<OrderPlaced>();
        IncludeType<OrderShipped>();
    }

    protected override Order? ApplyEvent(Order? snapshot, Guid identity, IEvent @event,
        InterceptedOrdersDbContext dbContext, IQuerySession session)
    {
        switch (@event.Data)
        {
            case OrderPlaced placed:
                return new Order
                {
                    Id = placed.OrderId, CustomerName = placed.CustomerName, TotalAmount = placed.Amount,
                    ItemCount = placed.Items
                };

            case OrderShipped:
                snapshot!.IsShipped = true;
                return snapshot;
        }

        return snapshot;
    }
}

/// <summary>
///     #846, the Polecat twin of marten#5601, and worse here. A save's transaction opened first, then
///     EF opened its placeholder from the same pool for every read, so each save held one connection
///     while waiting for a second and concurrency at the pool size deadlocked until the timeout:
///     <c>Max Pool Size=1</c> could not complete a single save, and 13 of 16 failed at 4. Marten's fix
///     (let EF open and close the placeholder around each read) does not transfer, because SQL Server
///     needs these reads inside the session's transaction, not merely on a connection of their own.
/// </summary>
public class Bug_846_an_inline_ef_save_needs_one_connection
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string ConnectionString(int maxPoolSize, bool mars)
    {
        return new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            MaxPoolSize = maxPoolSize,
            ConnectTimeout = 3,
            MultipleActiveResultSets = mars,

            // A pool is keyed on the connection string, so this keeps each store's pool to itself
            // rather than inheriting a sibling test's idle connections.
            ApplicationName = $"ef846_{Guid.NewGuid():N}"
        }.ConnectionString;
    }

    private static DocumentStore CreateStore(string connectionString, string schema,
        Action<StoreOptions> configure)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = connectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DatabaseSchemaName = schema;
            configure(opts);
        });
    }

    /// <summary>
    ///     Provisions on a pool of its own, so what is measured afterwards is a save and not the
    ///     migration in front of it.
    /// </summary>
    private static async Task ProvisionAsync(string schema, Action<StoreOptions> configure)
    {
        using var store = CreateStore(ConnectionSource.ConnectionString, schema, configure);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);
        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString,
            "ef_orders", "ef_order_summaries", "ef_order_details");

        // Warm the event store's own first-use work, which is not what this issue is about and
        // opens connections of its own by design.
        await using var session = store.LightweightSession();
        var id = Guid.NewGuid();
        session.Events.StartStream(id, new OrderPlaced(id, "seed", 1m, 1));
        await session.SaveChangesAsync(Ct);
    }

    private static void AddOrders(StoreOptions opts)
    {
        opts.Projections.Add<OrderAggregate, Order, TestDbContext>(
            opts, new OrderAggregate(), ProjectionLifecycle.Inline);
    }

    private static Task<int> OrderCountAsync(Guid id, string predicate = "1 = 1")
    {
        return EfCoreTestHelper.QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM ef_orders WHERE id = @id AND {predicate}", ("@id", id));
    }

    /// <summary>
    ///     The stream's start reaches <c>Store</c>, whose read is synchronous, and the append reaches
    ///     <c>LoadManyAsync</c>, so one pooled connection has to carry both kinds.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_inline_save_completes_on_a_pool_of_one(bool mars)
    {
        var schema = $"ef846_one_{(mars ? "mars" : "plain")}";
        await ProvisionAsync(schema, AddOrders);

        using var store = CreateStore(ConnectionString(1, mars), schema, AddOrders);

        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(id, new OrderPlaced(id, "pool of one", 10m, 2));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession())
        {
            session.Events.Append(id, new OrderShipped(id));
            await session.SaveChangesAsync(Ct);
        }

        (await OrderCountAsync(id, "is_shipped = 1")).ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task concurrent_saves_at_the_pool_size_do_not_stall(bool mars)
    {
        var schema = $"ef846_many_{(mars ? "mars" : "plain")}";
        await ProvisionAsync(schema, AddOrders);

        using var store = CreateStore(ConnectionString(4, mars), schema, AddOrders);

        var failures = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(async () =>
        {
            try
            {
                await using var session = store.LightweightSession();
                var id = Guid.NewGuid();
                session.Events.StartStream(id, new OrderPlaced(id, "concurrent", 1m, 1));
                await session.SaveChangesAsync(Ct);
                return null;
            }
            catch (Exception e)
            {
                return $"{e.GetType().Name}: {e.Message}";
            }
        }, Ct)));

        failures.Where(x => x != null).ToArray().ShouldBeEmpty();
    }

    /// <summary>
    ///     The other half of a pool of one. The ensurer ran for every type an inline projection
    ///     publishes, from inside the save's transaction and on a connection of its own, and an EF
    ///     projection's entity is one of those types. Polecat does not store that document, so the
    ///     table it was creating was never going to be used. Asserted on its own because the pool
    ///     tests above cannot tell this from the EF reads.
    /// </summary>
    [Fact]
    public async Task a_save_invents_no_polecat_table_for_an_ef_document()
    {
        const string schema = "ef846_no_phantom";

        using var store = CreateStore(ConnectionSource.ConnectionString, schema, AddOrders);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);

        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id, new OrderPlaced(id, "no phantom", 1m, 1));
        await session.SaveChangesAsync(Ct);

        (await OrderCountAsync(id)).ShouldBe(1);

        (await EfCoreTestHelper.QueryScalarAsync<int>(
            "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id " +
            "WHERE s.name = @schema AND t.name = 'pc_doc_order'", ("@schema", schema))).ShouldBe(0);
    }

    /// <summary>
    ///     The claim Marten's fix does not reach. Not "EF read on a connection" but "EF read inside
    ///     the transaction": this row exists only there.
    /// </summary>
    [Fact]
    public async Task an_ef_read_runs_inside_the_session_transaction()
    {
        const string schema = "ef846_inside";
        var projection = new ReadsItsOwnEventProjection(schema);

        Action<StoreOptions> configure = opts =>
            opts.Projections.Add<ReadsItsOwnEventProjection, TestDbContext>(
                opts, projection, ProjectionLifecycle.Inline);

        await ProvisionAsync(schema, configure);
        using var store = CreateStore(ConnectionString(1, mars: false), schema, configure);

        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id, new OrderPlaced(id, "uncommitted", 1m, 1));
        await session.SaveChangesAsync(Ct);

        projection.EventsVisibleToTheRead.ShouldBe(1);
    }

    /// <summary>
    ///     Several contexts on one connection is what attaching them to the session makes of a store
    ///     with more than one EF projection, and a transaction can only be used by a context that
    ///     was told about it.
    /// </summary>
    [Fact]
    public async Task two_ef_projections_share_the_session_connection()
    {
        const string schema = "ef846_two";

        void Configure(StoreOptions opts)
        {
            AddOrders(opts);
            opts.Projections.Add<OrderDetailFollowUpProjection, TestDbContext>(
                opts, new OrderDetailFollowUpProjection(), ProjectionLifecycle.Inline);
        }

        await ProvisionAsync(schema, Configure);
        using var store = CreateStore(ConnectionString(1, mars: false), schema, Configure);

        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(id, new OrderPlaced(id, "two projections", 1m, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession())
        {
            session.Events.Append(id, new OrderShipped(id));
            await session.SaveChangesAsync(Ct);
        }

        (await OrderCountAsync(id, "is_shipped = 1")).ShouldBe(1);
        (await EfCoreTestHelper.QueryScalarAsync<int>(
            "SELECT COUNT(*) FROM ef_order_details WHERE id = @id AND status = 'Shipped'", ("@id", id))).ShouldBe(1);
    }

    /// <summary>
    ///     A session outlives its transaction. The context from the first save is still registered
    ///     when the second one commits, and it still believes in a transaction that has ended.
    /// </summary>
    [Fact]
    public async Task a_session_can_save_more_than_once()
    {
        const string schema = "ef846_again";
        await ProvisionAsync(schema, AddOrders);
        using var store = CreateStore(ConnectionString(1, mars: false), schema, AddOrders);

        var first = Guid.NewGuid();
        var second = Guid.NewGuid();

        await using var session = store.LightweightSession();

        session.Events.StartStream(first, new OrderPlaced(first, "first", 1m, 1));
        await session.SaveChangesAsync(Ct);

        session.Events.Append(first, new OrderShipped(first));
        await session.SaveChangesAsync(Ct);

        session.Events.StartStream(second, new OrderPlaced(second, "second", 1m, 1));
        await session.SaveChangesAsync(Ct);

        (await OrderCountAsync(first, "is_shipped = 1")).ShouldBe(1);
        (await OrderCountAsync(second)).ShouldBe(1);
    }

    /// <summary>
    ///     #832 applied to a connection Polecat is now sharing. <c>SqlCommand</c> quietly drops a
    ///     completed transaction, so a read that carried on here would run in autocommit, and so
    ///     would the flush behind it, with the events it belongs to already rolled back.
    /// </summary>
    [Fact]
    public async Task an_ef_read_refuses_to_run_once_the_server_has_ended_the_transaction()
    {
        const string schema = "ef846_ended";

        static void Configure(StoreOptions opts)
        {
            opts.Projections.Add<RollsBackTheSessionAggregate, Order, TestDbContext>(
                opts, new RollsBackTheSessionAggregate(), ProjectionLifecycle.Inline);
        }

        using var store = CreateStore(ConnectionSource.ConnectionString, schema, Configure);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);

        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id, new OrderPlaced(id, "rolled back", 1m, 1));

        var refused = await Should.ThrowAsync<SessionTransactionUnusableException>(
            () => session.SaveChangesAsync(Ct));

        // At the read. The flush has a check of its own further on, which is pinned below, and the
        // save is refused either way, so without this the read's check could go unnoticed.
        refused.StackTrace.ShouldContain("EfCoreProjectionStorage");

        (await OrderCountAsync(id)).ShouldBe(0);
    }

    /// <summary>
    ///     What the read's refusal protects, on a projection that never makes one. The rows go in
    ///     the unit of work's name, and with the transaction gone they would go in by themselves.
    /// </summary>
    [Fact]
    public async Task an_ef_flush_refuses_to_run_once_the_server_has_ended_the_transaction()
    {
        const string schema = "ef846_ended_flush";

        static void Configure(StoreOptions opts)
        {
            opts.Projections.Add<RollsBackTheSessionEventProjection, TestDbContext>(
                opts, new RollsBackTheSessionEventProjection(), ProjectionLifecycle.Inline);
        }

        using var store = CreateStore(ConnectionSource.ConnectionString, schema, Configure);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);

        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id, new OrderPlaced(id, "rolled back", 1m, 1));

        await Should.ThrowAsync<SessionTransactionUnusableException>(() => session.SaveChangesAsync(Ct));

        (await EfCoreTestHelper.QueryScalarAsync<int>("SELECT COUNT(*) FROM ef_order_details WHERE id = @id",
            ("@id", id))).ShouldBe(0);
    }

    private static int _pipelineRetries;

    private static void AddInterceptedOrders(StoreOptions opts, ProjectionLifecycle lifecycle)
    {
        _pipelineRetries = 0;

        // Counted rather than inferred from the row being there. The daemon has a retry of its own,
        // one level up from the pipeline, so on that path the row turns up whether or not the
        // pipeline ever saw the command.
        opts.ConfigurePolly(pipeline => pipeline.AddRetry(new RetryStrategyOptions
        {
            // HandleInner because a failed flush reaches here wrapped in a DbUpdateException.
            ShouldHandle = new PredicateBuilder().Handle<TransientEfCommandFailure>()
                .HandleInner<TransientEfCommandFailure>(),
            MaxRetryAttempts = 2,
            Delay = TimeSpan.Zero,
            OnRetry = _ =>
            {
                Interlocked.Increment(ref _pipelineRetries);
                return ValueTask.CompletedTask;
            }
        }));

        opts.Projections.Add<InterceptedOrderAggregate, Order, InterceptedOrdersDbContext>(
            opts, new InterceptedOrderAggregate(), lifecycle);
    }

    /// <summary>
    ///     CLAUDE.md: all database command execution is covered by the resilience pipeline. Moving
    ///     these reads onto the session's connection does not move them through the session's
    ///     pipeline by itself, so each of the three inline commands is pinned separately: the
    ///     synchronous read <c>Store</c> makes, the asynchronous one <c>LoadManyAsync</c> makes, and
    ///     the flush.
    /// </summary>
    [Theory]
    [InlineData("SELECT", false)]
    [InlineData("SELECT", true)]
    [InlineData("INSERT", false)]
    public async Task every_inline_ef_command_runs_inside_the_resilience_pipeline(string command, bool throughAnAppend)
    {
        var schema = $"ef846_pipeline_{command.ToLowerInvariant()}_{(throughAnAppend ? "append" : "start")}";

        void Configure(StoreOptions opts) => AddInterceptedOrders(opts, ProjectionLifecycle.Inline);

        await ProvisionAsync(schema, Configure);
        using var store = CreateStore(ConnectionSource.ConnectionString, schema, Configure);

        var id = Guid.NewGuid();

        if (throughAnAppend)
        {
            await using var seed = store.LightweightSession();
            seed.Events.StartStream(id, new OrderPlaced(id, "seeded", 1m, 1));
            await seed.SaveChangesAsync(Ct);
        }

        FailsOnceInterceptor.FailNext(command);

        await using var session = store.LightweightSession();
        if (throughAnAppend)
        {
            session.Events.Append(id, new OrderShipped(id));
        }
        else
        {
            session.Events.StartStream(id, new OrderPlaced(id, "fails once", 1m, 1));
        }

        await session.SaveChangesAsync(Ct);

        // Not vacuous: the command really was failed, and the retry is why the save survived it.
        FailsOnceInterceptor.Failures.ShouldBe(1);
        _pipelineRetries.ShouldBe(1);
        (await OrderCountAsync(id)).ShouldBe(1);
    }

    [Fact]
    public async Task a_daemon_ef_read_runs_inside_the_resilience_pipeline()
    {
        const string schema = "ef846_pipeline_async";

        void Configure(StoreOptions opts) => AddInterceptedOrders(opts, ProjectionLifecycle.Async);

        await ProvisionAsync(schema, Configure);
        using var store = CreateStore(ConnectionSource.ConnectionString, schema, Configure);

        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(id, new OrderPlaced(id, "fails once", 1m, 1));
            await session.SaveChangesAsync(Ct);
        }

        FailsOnceInterceptor.FailNext("SELECT");

        await store.WaitForProjectionAsync();

        FailsOnceInterceptor.Failures.ShouldBe(1);
        _pipelineRetries.ShouldBe(1);
        (await OrderCountAsync(id)).ShouldBe(1);
    }
}
