using JasperFx.Events.Projections;

namespace Polecat.EntityFrameworkCore.Tests;

public class ef_core_event_projection_tests : IAsyncLifetime
{
    private DocumentStore _store = null!;

    public async Task InitializeAsync()
    {
        _store = await EfCoreTestHelper.CreateStoreWithEventProjection(ProjectionLifecycle.Inline);
    }

    public Task DisposeAsync()
    {
        _store?.Dispose();
        return Task.CompletedTask;
    }

    [RequiresNativeJsonFact(true)]
    public async Task can_project_event_to_ef_core_and_polecat()
    {
        var orderId = Guid.NewGuid();
        await using var session = _store.LightweightSession();
        session.Events.StartStream(orderId,
            new OrderPlaced(orderId, "Alice", 100.00m, 3));
        await session.SaveChangesAsync();

        // Verify EF Core entity
        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, status FROM ef_order_details WHERE id = @id",
            ("@id", orderId));

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("Alice");
        row["status"].ShouldBe("Placed");

        // Verify Polecat document
        await using var query = _store.QuerySession();
        var log = await query.LoadAsync<OrderLog>(orderId);
        log.ShouldNotBeNull();
        log.CustomerName.ShouldBe("Alice");
        log.EventType.ShouldBe("OrderPlaced");
    }

    [RequiresNativeJsonFact(true)]
    public async Task can_project_multiple_events()
    {
        var orderId = Guid.NewGuid();
        await using var session = _store.LightweightSession();
        session.Events.StartStream(orderId,
            new OrderPlaced(orderId, "Bob", 250.00m, 5),
            new OrderShipped(orderId));
        await session.SaveChangesAsync();

        // Verify EF Core entity reflects both events
        var row = await EfCoreTestHelper.QueryRowAsync(
            "SELECT customer_name, is_shipped, status FROM ef_order_details WHERE id = @id",
            ("@id", orderId));

        row.ShouldNotBeNull();
        row["customer_name"].ShouldBe("Bob");
        ((bool)row["is_shipped"]!).ShouldBeTrue();
        row["status"].ShouldBe("Shipped");
    }
}
