using JasperFx.Events.Projections;

namespace Polecat.EntityFrameworkCore.Tests;

public abstract class ef_core_multi_stream_projection_tests_base : IAsyncLifetime
{
    protected DocumentStore Store = null!;
    protected abstract ProjectionLifecycle Lifecycle { get; }

    public async Task InitializeAsync()
    {
        Store = await EfCoreTestHelper.CreateStoreWithMultiStreamProjection(Lifecycle);
    }

    public async Task DisposeAsync()
    {
        Store?.Dispose();
    }

    protected virtual Task WaitForProjectionAsync() => Task.CompletedTask;

    [RequiresNativeJsonFact(true)]
    public async Task multi_stream_projection_aggregates_across_streams()
    {
        var orderId1 = Guid.NewGuid();
        var orderId2 = Guid.NewGuid();

        await using var session = Store.LightweightSession();
        session.Events.StartStream(orderId1,
            new CustomerOrderPlaced(orderId1, "Alice", 100.00m));
        session.Events.StartStream(orderId2,
            new CustomerOrderPlaced(orderId2, "Alice", 200.00m));
        await session.SaveChangesAsync();

        await WaitForProjectionAsync();

        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT total_orders, total_spent FROM ef_customer_order_histories WHERE id = @id",
            ("@id", "Alice"));

        row.ShouldNotBeNull();
        ((int)row["total_orders"]!).ShouldBe(2);
        ((decimal)row["total_spent"]!).ShouldBe(300.00m);
    }

    [RequiresNativeJsonFact(true)]
    public async Task multi_stream_projection_creates_separate_aggregates_per_identity()
    {
        var orderId1 = Guid.NewGuid();
        var orderId2 = Guid.NewGuid();

        await using var session = Store.LightweightSession();
        session.Events.StartStream(orderId1,
            new CustomerOrderPlaced(orderId1, "Bob", 50.00m));
        session.Events.StartStream(orderId2,
            new CustomerOrderPlaced(orderId2, "Charlie", 75.00m));
        await session.SaveChangesAsync();

        await WaitForProjectionAsync();

        var bobRow = await EfCoreTestHelper.QueryRowAsync(
            "SELECT total_orders, total_spent FROM ef_customer_order_histories WHERE id = @id",
            ("@id", "Bob"));
        var charlieRow = await EfCoreTestHelper.QueryRowAsync(
            "SELECT total_orders, total_spent FROM ef_customer_order_histories WHERE id = @id",
            ("@id", "Charlie"));

        bobRow.ShouldNotBeNull();
        ((int)bobRow["total_orders"]!).ShouldBe(1);
        ((decimal)bobRow["total_spent"]!).ShouldBe(50.00m);

        charlieRow.ShouldNotBeNull();
        ((int)charlieRow["total_orders"]!).ShouldBe(1);
        ((decimal)charlieRow["total_spent"]!).ShouldBe(75.00m);
    }

    [RequiresNativeJsonFact(true)]
    public async Task multi_stream_projection_handles_subsequent_appends()
    {
        var orderId1 = Guid.NewGuid();
        var orderId2 = Guid.NewGuid();

        await using var session = Store.LightweightSession();
        session.Events.StartStream(orderId1,
            new CustomerOrderPlaced(orderId1, "Diana", 100.00m));
        await session.SaveChangesAsync();

        await WaitForProjectionAsync();

        await using var session2 = Store.LightweightSession();
        session2.Events.StartStream(orderId2,
            new CustomerOrderPlaced(orderId2, "Diana", 200.00m));
        await session2.SaveChangesAsync();

        await WaitForProjectionAsync();

        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT total_orders, total_spent FROM ef_customer_order_histories WHERE id = @id",
            ("@id", "Diana"));

        row.ShouldNotBeNull();
        ((int)row["total_orders"]!).ShouldBe(2);
        ((decimal)row["total_spent"]!).ShouldBe(300.00m);
    }
}

public class ef_core_multi_stream_inline_tests : ef_core_multi_stream_projection_tests_base
{
    protected override ProjectionLifecycle Lifecycle => ProjectionLifecycle.Inline;
}

public class ef_core_multi_stream_async_tests : ef_core_multi_stream_projection_tests_base
{
    protected override ProjectionLifecycle Lifecycle => ProjectionLifecycle.Async;

    protected override async Task WaitForProjectionAsync()
    {
        await Store.WaitForProjectionAsync();
    }
}

public class ef_core_multi_stream_live_tests : IAsyncLifetime
{
    private DocumentStore _store = null!;

    public async Task InitializeAsync()
    {
        _store = await EfCoreTestHelper.CreateStoreWithMultiStreamProjection(ProjectionLifecycle.Inline);
    }

    public Task DisposeAsync()
    {
        _store?.Dispose();
        return Task.CompletedTask;
    }

    [RequiresNativeJsonFact(true)]
    public async Task can_store_events_without_error()
    {
        var orderId = Guid.NewGuid();
        await using var session = _store.LightweightSession();
        session.Events.StartStream(orderId,
            new CustomerOrderPlaced(orderId, "TestCustomer", 100.00m));
        await session.SaveChangesAsync();

        // Verify stream was created
        var stream = await _store.QuerySession().Events.FetchStreamAsync(orderId);
        stream.ShouldNotBeNull();
        stream.Count.ShouldBe(1);
    }
}
