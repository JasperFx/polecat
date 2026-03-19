using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;

namespace Polecat.EntityFrameworkCore.Tests;

public abstract class ef_core_single_stream_projection_tests_base : IAsyncLifetime
{
    protected DocumentStore Store = null!;
    protected abstract ProjectionLifecycle Lifecycle { get; }

    public async Task InitializeAsync()
    {
        Store = await EfCoreTestHelper.CreateStoreWithSingleStreamProjection(Lifecycle);
    }

    public async Task DisposeAsync()
    {
        Store?.Dispose();
    }

    protected virtual Task WaitForProjectionAsync() => Task.CompletedTask;

    [Fact]
    public async Task single_stream_projection_writes_aggregate_on_create()
    {
        var orderId = Guid.NewGuid();
        await using var session = Store.LightweightSession();
        session.Events.StartStream(orderId,
            new OrderPlaced(orderId, "Carol", 200.00m, 5));
        await session.SaveChangesAsync();

        await WaitForProjectionAsync();

        // Verify EF Core entity was written
        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, total_amount, item_count FROM ef_orders WHERE id = @id",
            ("@id", orderId));

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("Carol");
        ((decimal)row["total_amount"]!).ShouldBe(200.00m);
        ((int)row["item_count"]!).ShouldBe(5);
    }

    [Fact]
    public async Task single_stream_projection_evolves_aggregate_with_subsequent_events()
    {
        var orderId = Guid.NewGuid();
        await using var session = Store.LightweightSession();
        session.Events.StartStream(orderId,
            new OrderPlaced(orderId, "Dave", 150.00m, 3));
        await session.SaveChangesAsync();

        await WaitForProjectionAsync();

        await using var session2 = Store.LightweightSession();
        session2.Events.Append(orderId, new OrderShipped(orderId));
        await session2.SaveChangesAsync();

        await WaitForProjectionAsync();

        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, is_shipped FROM ef_orders WHERE id = @id",
            ("@id", orderId));

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("Dave");
        ((bool)row["is_shipped"]!).ShouldBeTrue();
    }

    [Fact]
    public async Task single_stream_projection_handles_multiple_events_in_one_append()
    {
        var orderId = Guid.NewGuid();
        await using var session = Store.LightweightSession();
        session.Events.StartStream(orderId,
            new OrderPlaced(orderId, "Eve", 300.00m, 7),
            new OrderShipped(orderId));
        await session.SaveChangesAsync();

        await WaitForProjectionAsync();

        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, total_amount, is_shipped FROM ef_orders WHERE id = @id",
            ("@id", orderId));

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("Eve");
        ((decimal)row["total_amount"]!).ShouldBe(300.00m);
        ((bool)row["is_shipped"]!).ShouldBeTrue();
    }

    [Fact]
    public async Task single_stream_projection_writes_ef_core_side_effects()
    {
        var orderId = Guid.NewGuid();
        await using var session = Store.LightweightSession();
        session.Events.StartStream(orderId,
            new OrderPlaced(orderId, "Frank", 99.99m, 2));
        await session.SaveChangesAsync();

        await WaitForProjectionAsync();

        // Verify the OrderSummary side-effect was also written
        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, status FROM ef_order_summaries WHERE id = @id",
            ("@id", orderId));

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("Frank");
        row["status"].ShouldBe("Placed");
    }
}

public class ef_core_single_stream_inline_tests : ef_core_single_stream_projection_tests_base
{
    protected override ProjectionLifecycle Lifecycle => ProjectionLifecycle.Inline;
}

public class ef_core_single_stream_async_tests : ef_core_single_stream_projection_tests_base
{
    protected override ProjectionLifecycle Lifecycle => ProjectionLifecycle.Async;
    private IProjectionDaemon? _daemon;

    protected override async Task WaitForProjectionAsync()
    {
        if (_daemon == null)
        {
            SqlConnection.ClearAllPools();
            _daemon = (IProjectionDaemon)await Store.BuildProjectionDaemonAsync();
            await _daemon.StartAllAsync();
        }

        // Retry on transient SQL Server connection errors from daemon internals
        for (var attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                await _daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
                return;
            }
            catch (AggregateException) when (attempt < 2)
            {
                SqlConnection.ClearAllPools();
                await Task.Delay(200);
            }
        }
    }
}

public class ef_core_single_stream_live_tests : IAsyncLifetime
{
    private DocumentStore _store = null!;

    public async Task InitializeAsync()
    {
        _store = await EfCoreTestHelper.CreateStoreWithSingleStreamProjection(ProjectionLifecycle.Inline);
    }

    public Task DisposeAsync()
    {
        _store?.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    public async Task live_aggregation_builds_aggregate_on_the_fly()
    {
        var orderId = Guid.NewGuid();
        await using var session = _store.LightweightSession();
        session.Events.StartStream(orderId,
            new OrderPlaced(orderId, "Grace", 500.00m, 10),
            new OrderShipped(orderId));
        await session.SaveChangesAsync();

        await using var query = _store.QuerySession();
        var order = await query.Events.AggregateStreamAsync<Order>(orderId);

        order.ShouldNotBeNull();
        order.CustomerName.ShouldBe("Grace");
        order.TotalAmount.ShouldBe(500.00m);
        order.IsShipped.ShouldBeTrue();
    }

    [Fact]
    public async Task live_aggregation_returns_null_for_unknown_stream()
    {
        await using var query = _store.QuerySession();
        var order = await query.Events.AggregateStreamAsync<Order>(Guid.NewGuid());
        order.ShouldBeNull();
    }
}
