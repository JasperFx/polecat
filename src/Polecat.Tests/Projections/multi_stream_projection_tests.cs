using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

public class CustomerSummary
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int OrderCount { get; set; }
    public decimal TotalSpent { get; set; }
    public decimal TotalPaid { get; set; }
}

public class CustomerSummaryProjection : MultiStreamProjection<CustomerSummary, Guid>
{
    public CustomerSummaryProjection()
    {
        Identity<CustomerCreated>(e => e.CustomerId);
        Identity<OrderPlaced>(e => e.CustomerId);
        Identity<PaymentReceived>(e => e.CustomerId);
    }

    public static CustomerSummary Create(CustomerCreated e)
    {
        return new CustomerSummary { Name = e.Name };
    }

    public void Apply(OrderPlaced e, CustomerSummary summary)
    {
        summary.OrderCount++;
        summary.TotalSpent += e.Amount;
    }

    public void Apply(PaymentReceived e, CustomerSummary summary)
    {
        summary.TotalPaid += e.Amount;
    }
}

[Collection("integration")]
public class multi_stream_projection_tests : IntegrationContext
{
    public multi_stream_projection_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreWithInlineMultiStream()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Add<CustomerSummaryProjection>(ProjectionLifecycle.Inline);
        });
        return theStore;
    }

    private async Task<DocumentStore> CreateStoreWithAsyncMultiStream()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Add<CustomerSummaryProjection>(ProjectionLifecycle.Async);
        });
        return theStore;
    }

    [Fact]
    public async Task multi_stream_creates_aggregate_from_identity()
    {
        var store = await CreateStoreWithInlineMultiStream();
        var customerId = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(),
            new CustomerCreated(customerId, "Alice"));
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var summary = await query.LoadAsync<CustomerSummary>(customerId);

        summary.ShouldNotBeNull();
        summary.Id.ShouldBe(customerId);
        summary.Name.ShouldBe("Alice");
    }

    [Fact]
    public async Task multi_stream_updates_from_multiple_event_streams()
    {
        var store = await CreateStoreWithInlineMultiStream();
        var customerId = Guid.NewGuid();

        // Create customer in one stream
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(Guid.NewGuid(),
            new CustomerCreated(customerId, "Bob"));
        await session1.SaveChangesAsync();

        // Place order in another stream
        await using var session2 = store.LightweightSession();
        session2.Events.StartStream(Guid.NewGuid(),
            new OrderPlaced(customerId, 99.99m));
        await session2.SaveChangesAsync();

        // Make payment in yet another stream
        await using var session3 = store.LightweightSession();
        session3.Events.StartStream(Guid.NewGuid(),
            new PaymentReceived(customerId, 50.00m));
        await session3.SaveChangesAsync();

        await using var query = store.QuerySession();
        var summary = await query.LoadAsync<CustomerSummary>(customerId);

        summary.ShouldNotBeNull();
        summary.Name.ShouldBe("Bob");
        summary.OrderCount.ShouldBe(1);
        summary.TotalSpent.ShouldBe(99.99m);
        summary.TotalPaid.ShouldBe(50.00m);
    }

    [Fact]
    public async Task multi_stream_handles_multiple_aggregates()
    {
        var store = await CreateStoreWithInlineMultiStream();
        var customer1 = Guid.NewGuid();
        var customer2 = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(),
            new CustomerCreated(customer1, "Alice"),
            new OrderPlaced(customer1, 100m));
        session.Events.StartStream(Guid.NewGuid(),
            new CustomerCreated(customer2, "Bob"),
            new OrderPlaced(customer2, 200m),
            new OrderPlaced(customer2, 150m));
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var s1 = await query.LoadAsync<CustomerSummary>(customer1);
        var s2 = await query.LoadAsync<CustomerSummary>(customer2);

        s1.ShouldNotBeNull();
        s1.Name.ShouldBe("Alice");
        s1.OrderCount.ShouldBe(1);
        s1.TotalSpent.ShouldBe(100m);

        s2.ShouldNotBeNull();
        s2.Name.ShouldBe("Bob");
        s2.OrderCount.ShouldBe(2);
        s2.TotalSpent.ShouldBe(350m);
    }

    [Fact]
    public async Task multi_stream_works_with_async_daemon()
    {
        var store = await CreateStoreWithAsyncMultiStream();
        var customerId = Guid.NewGuid();

        // Insert events
        await using var session = store.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(),
            new CustomerCreated(customerId, "Async Alice"));
        session.Events.StartStream(Guid.NewGuid(),
            new OrderPlaced(customerId, 75.00m));
        await session.SaveChangesAsync();

        // Run daemon
        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        // Verify
        await using var query = store.QuerySession();
        var summary = await query.LoadAsync<CustomerSummary>(customerId);

        summary.ShouldNotBeNull();
        summary.Name.ShouldBe("Async Alice");
        summary.OrderCount.ShouldBe(1);
        summary.TotalSpent.ShouldBe(75.00m);
    }
}
