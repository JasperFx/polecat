using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.TestUtils;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     Fails the first save that reaches it, ahead of whatever is behind it in the list, which is the
///     failure in question: nothing has been flushed and the transaction rolls back.
/// </summary>
public class FailsOnceParticipant : ITransactionParticipant
{
    private int _attempts;

    public int Attempts => _attempts;

    public Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        if (Interlocked.Increment(ref _attempts) == 1)
        {
            throw new InvalidOperationException("The first attempt at this save fails.");
        }

        return Task.CompletedTask;
    }
}

/// <summary>
///     <see cref="OrderDetailProjection" />, saying what it writes through <c>operations</c>.
/// </summary>
public class DeclaredOrderDetailProjection : OrderDetailProjection
{
    public DeclaredOrderDetailProjection()
    {
        RegisterPublishedType(typeof(OrderLog));
    }
}

/// <summary>
///     #870: the first-use ensure a save still has to make from inside its transaction, for what an
///     event projection writes through <c>operations</c>, on a pool with nothing to spare for it.
/// </summary>
public class Bug_870_a_projected_table_on_a_pool_of_one
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string NewSchema() => "ef870p_" + Guid.NewGuid().ToString("N")[..10];

    private static string PoolOfOne()
    {
        return new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
        {
            MaxPoolSize = 1,
            ConnectTimeout = 3,
            MultipleActiveResultSets = false,

            // A pool is keyed on the connection string, so this keeps each store's pool to itself.
            ApplicationName = $"ef870_{Guid.NewGuid():N}"
        }.ConnectionString;
    }

    private static DocumentStore CreateStore(string connectionString, string schema, Action<StoreOptions> configure)
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

    private static void AddDetails(StoreOptions opts)
    {
        opts.Projections.Add<OrderDetailProjection, TestDbContext>(
            opts, new OrderDetailProjection(), ProjectionLifecycle.Inline);
    }

    private static void AddDeclaredDetails(StoreOptions opts)
    {
        opts.Projections.Add<DeclaredOrderDetailProjection, TestDbContext>(
            opts, new DeclaredOrderDetailProjection(), ProjectionLifecycle.Inline);
    }

    private static async Task<Guid> SaveAsync(IDocumentStore store)
    {
        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id, new OrderPlaced(id, "pool of one", 1m, 1));
        await session.SaveChangesAsync(Ct);
        return id;
    }

    [Fact]
    public async Task a_table_that_exists_needs_no_second_connection()
    {
        var schema = NewSchema();
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);

        using (var earlier = CreateStore(ConnectionSource.ConnectionString, schema, AddDetails))
        {
            await SaveAsync(earlier);
        }

        using var store = CreateStore(PoolOfOne(), schema, AddDetails);

        var id = await SaveAsync(store);

        await using var query = store.QuerySession();
        (await query.LoadAsync<OrderLog>(id, Ct)).ShouldNotBeNull();
    }

    [Fact]
    public async Task a_declared_type_needs_no_second_connection_even_when_its_table_is_missing()
    {
        var schema = NewSchema();
        await EfCoreTestHelper.EnsureEfCoreTablesAsync<TestDbContext>(ConnectionSource.ConnectionString);

        // The event store is there and the document table is not.
        using (var earlier = CreateStore(ConnectionSource.ConnectionString, schema, _ => { }))
        {
            await using var seed = earlier.LightweightSession();
            seed.Events.StartStream(Guid.NewGuid(), new OrderCancelled(Guid.NewGuid()));
            await seed.SaveChangesAsync(Ct);
        }

        using var store = CreateStore(PoolOfOne(), schema, AddDeclaredDetails);

        var id = await SaveAsync(store);

        await using var query = store.QuerySession();
        (await query.LoadAsync<OrderLog>(id, Ct)).ShouldNotBeNull();
    }
}

/// <summary>
///     #870: a save that fails on a session can be tried again on that session. The attempt that failed
///     left its participant behind, still holding the changes it never flushed, and the retry's
///     projection enlisted another one holding the same changes.
/// </summary>
public class Bug_870_a_retried_save_writes_once
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

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
        await EfCoreTestHelper.CleanEfCoreTablesAsync(ConnectionSource.ConnectionString,
            "ef_orders", "ef_order_summaries", "ef_order_details");
        return store;
    }

    private static void AddOrders(StoreOptions opts)
    {
        opts.Projections.Add<OrderAggregate, Order, TestDbContext>(
            opts, new OrderAggregate(), ProjectionLifecycle.Inline);
    }

    private static Task<int> CountAsync(string table, Guid id, string predicate = "1 = 1")
    {
        return EfCoreTestHelper.QueryScalarAsync<int>(
            $"SELECT COUNT(*) FROM {table} WHERE id = @id AND {predicate}", ("@id", id));
    }

    [Fact]
    public async Task a_single_stream_projection_starting_a_stream()
    {
        using var store = await CreateStoreAsync("ef870_start", AddOrders);

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession();
        var failing = new FailsOnceParticipant();
        session.AddTransactionParticipant(failing);

        session.Events.StartStream(id, new OrderPlaced(id, "retry", 10m, 2));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        failing.Attempts.ShouldBe(2);
        (await CountAsync("ef_orders", id)).ShouldBe(1);
        (await CountAsync("ef_order_summaries", id)).ShouldBe(1);
    }

    [Fact]
    public async Task a_single_stream_projection_appending()
    {
        using var store = await CreateStoreAsync("ef870_append", AddOrders);

        var id = Guid.NewGuid();
        await using (var start = store.LightweightSession())
        {
            start.Events.StartStream(id, new OrderPlaced(id, "retry", 10m, 2));
            await start.SaveChangesAsync(Ct);
        }

        await using var session = store.LightweightSession();
        session.AddTransactionParticipant(new FailsOnceParticipant());

        session.Events.Append(id, new OrderShipped(id));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        var events = await session.Events.FetchStreamAsync(id, token: Ct);
        events.Select(x => x.Version).ShouldBe([1L, 2L]);

        (await CountAsync("ef_orders", id, "is_shipped = 1")).ShouldBe(1);
    }

    [Fact]
    public async Task an_event_projection_starting_a_stream()
    {
        using var store = await CreateStoreAsync("ef870_event", opts =>
            opts.Projections.Add<OrderDetailProjection, TestDbContext>(
                opts, new OrderDetailProjection(), ProjectionLifecycle.Inline));

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.AddTransactionParticipant(new FailsOnceParticipant());

        session.Events.StartStream(id, new OrderPlaced(id, "retry", 10m, 2));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        await session.SaveChangesAsync(Ct);

        (await CountAsync("ef_order_details", id)).ShouldBe(1);
    }

    /// <summary>
    ///     The context of the failed attempt has nothing left to do, and leaving it for the end of the
    ///     session would hold it for as long as the retries take.
    /// </summary>
    [Fact]
    public async Task the_failed_attempts_context_goes_with_it()
    {
        using var store = await CreateStoreAsync("ef870_disposal", opts =>
            opts.Projections.Add<TrackedOrderAggregate, Order, DisposalTrackingDbContext>(
                opts, new TrackedOrderAggregate(), ProjectionLifecycle.Inline));

        DisposalTrackingDbContext.Reset();

        var id = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.AddTransactionParticipant(new FailsOnceParticipant());

        session.Events.StartStream(id, new OrderPlaced(id, "retry", 10m, 2));

        await Should.ThrowAsync<InvalidOperationException>(() => session.SaveChangesAsync(Ct));

        DisposalTrackingDbContext.Created.ShouldBe(1);
        DisposalTrackingDbContext.Disposed.ShouldBe(1);
    }
}
