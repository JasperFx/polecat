using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

#region sample_polecat_natural_key_aggregate_types

public record OrderNumber(string Value);

public class OrderAggregate
{
    public Guid Id { get; set; }

    [NaturalKey]
    public OrderNumber OrderNum { get; set; } = null!;

    public decimal TotalAmount { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public bool IsComplete { get; set; }

    [NaturalKeySource]
    public void Apply(NkOrderCreated e)
    {
        OrderNum = e.OrderNumber;
        CustomerName = e.CustomerName;
    }

    public void Apply(NkOrderItemAdded e)
    {
        TotalAmount += e.Price;
    }

    [NaturalKeySource]
    public void Apply(NkOrderNumberChanged e)
    {
        OrderNum = e.NewOrderNumber;
    }

    public void Apply(NkOrderCompleted e)
    {
        IsComplete = true;
    }
}

public class InvoiceAggregate
{
    public Guid Id { get; set; }

    [NaturalKey]
    public string InvoiceCode { get; set; } = string.Empty;

    public decimal Amount { get; set; }

    [NaturalKeySource]
    public void Apply(NkInvoiceCreated e)
    {
        InvoiceCode = e.Code;
        Amount = e.Amount;
    }
}

public record NkOrderCreated(OrderNumber OrderNumber, string CustomerName);
public record NkOrderItemAdded(string ItemName, decimal Price);
public record NkOrderNumberChanged(OrderNumber NewOrderNumber);
public record NkOrderCompleted;
public record NkInvoiceCreated(string Code, decimal Amount);

#endregion

#region Guid stream identity + Inline lifecycle

public class natural_key_inline_guid_tests : OneOffConfigurationsContext
{
    private async Task ConfigureAndApply()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task fetch_for_writing_new_stream_by_natural_key()
    {
        await ConfigureAndApply();

        var orderNumber = new OrderNumber("ORD-DOES-NOT-EXIST");

        await using var session = theStore.LightweightSession();
        await Should.ThrowAsync<InvalidOperationException>(
            session.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber));
    }

    [Fact]
    public async Task fetch_for_writing_existing_stream_by_natural_key()
    {
        await ConfigureAndApply();

        var streamId = Guid.NewGuid();
        var orderNumber = new OrderNumber("ORD-001");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new NkOrderCreated(orderNumber, "Alice"),
            new NkOrderItemAdded("Widget", 9.99m));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        #region sample_polecat_fetch_for_writing_by_natural_key
        // FetchForWriting by the business identifier instead of stream id
        var stream = await session2.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.OrderNum.ShouldBe(orderNumber);

        // Append new events through the stream
        stream.AppendOne(new NkOrderItemAdded("Gadget", 19.99m));
        await session2.SaveChangesAsync();
        #endregion
    }

    [Fact]
    public async Task fetch_for_writing_and_append_events_by_natural_key()
    {
        await ConfigureAndApply();

        var streamId = Guid.NewGuid();
        var orderNumber = new OrderNumber("ORD-002");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new NkOrderCreated(orderNumber, "Bob"));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);
        stream.AppendOne(new NkOrderItemAdded("Gadget", 19.99m));
        stream.AppendOne(new NkOrderCompleted());
        await session2.SaveChangesAsync();

        await using var session3 = theStore.LightweightSession();
        var verify = await session3.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);

        verify.Aggregate.ShouldNotBeNull();
        verify.Aggregate!.TotalAmount.ShouldBe(19.99m);
        verify.Aggregate.IsComplete.ShouldBeTrue();
        verify.StartingVersion.ShouldBe(3);
    }

    [Fact]
    public async Task fetch_latest_by_natural_key()
    {
        await ConfigureAndApply();

        var streamId = Guid.NewGuid();
        var orderNumber = new OrderNumber("ORD-003");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new NkOrderCreated(orderNumber, "Charlie"),
            new NkOrderItemAdded("Doohickey", 5.50m));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        #region sample_polecat_fetch_latest_by_natural_key
        // Read-only access by natural key
        var aggregate = await session2.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber);
        #endregion

        aggregate.ShouldNotBeNull();
        aggregate!.OrderNum.ShouldBe(orderNumber);
        aggregate.CustomerName.ShouldBe("Charlie");
        aggregate.TotalAmount.ShouldBe(5.50m);
    }

    [Fact]
    public async Task fetch_latest_returns_null_for_nonexistent_natural_key()
    {
        await ConfigureAndApply();

        var orderNumber = new OrderNumber("ORD-NO-SUCH-KEY");

        await using var session = theStore.LightweightSession();
        var aggregate = await session.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber);

        aggregate.ShouldBeNull();
    }

    [Fact]
    public async Task fetch_for_exclusive_writing_by_natural_key()
    {
        await ConfigureAndApply();

        var streamId = Guid.NewGuid();
        var orderNumber = new OrderNumber("ORD-004");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new NkOrderCreated(orderNumber, "Dana"),
            new NkOrderItemAdded("Thingamajig", 12.00m));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForExclusiveWriting<OrderAggregate, OrderNumber>(orderNumber);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.OrderNum.ShouldBe(orderNumber);
        stream.Aggregate.CustomerName.ShouldBe("Dana");
        stream.StartingVersion.ShouldBe(2);

        stream.AppendOne(new NkOrderCompleted());
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
    }

    [Fact]
    public async Task natural_key_is_mutable_fetch_after_change()
    {
        await ConfigureAndApply();

        var streamId = Guid.NewGuid();
        var originalKey = new OrderNumber("ORD-OLD");
        var newKey = new OrderNumber("ORD-NEW");

        // Create stream with original key
        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new NkOrderCreated(originalKey, "Eve"));
        await session1.SaveChangesAsync();

        // Change the natural key
        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<OrderAggregate, OrderNumber>(originalKey);
        stream.AppendOne(new NkOrderNumberChanged(newKey));
        await session2.SaveChangesAsync();

        // Fetch by new key should work
        {
            await using var session3 = theStore.LightweightSession();
            var byNewKey = await session3.Events.FetchForWriting<OrderAggregate, OrderNumber>(newKey);

            byNewKey.Aggregate.ShouldNotBeNull();
            byNewKey.Aggregate!.OrderNum.ShouldBe(newKey);
            byNewKey.Aggregate.CustomerName.ShouldBe("Eve");
            byNewKey.Id.ShouldBe(streamId);
        }

        // Fetch by old key still works because the old mapping row persists
        {
            await using var session4 = theStore.LightweightSession();
            var byOldKey = await session4.Events.FetchForWriting<OrderAggregate, OrderNumber>(originalKey);
            byOldKey.Aggregate.ShouldNotBeNull();
            byOldKey.Id.ShouldBe(streamId);
        }
    }

    [Fact]
    public async Task natural_key_with_primitive_string_type()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Add<SingleStreamProjection<InvoiceAggregate, Guid>>(ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = Guid.NewGuid();
        var invoiceCode = "INV-2024-001";

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new NkInvoiceCreated(invoiceCode, 250.00m));
        await session1.SaveChangesAsync();

        // FetchForWriting by primitive string key
        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<InvoiceAggregate, string>(invoiceCode);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.InvoiceCode.ShouldBe(invoiceCode);
        stream.Aggregate.Amount.ShouldBe(250.00m);
        stream.Id.ShouldBe(streamId);

        // FetchLatest by primitive string key
        await using var session3 = theStore.LightweightSession();
        var latest = await session3.Events.FetchLatest<InvoiceAggregate, string>(invoiceCode);

        latest.ShouldNotBeNull();
        latest!.InvoiceCode.ShouldBe(invoiceCode);
        latest.Amount.ShouldBe(250.00m);
    }
}

#endregion

#region Live lifecycle (aggregate built from events each time)

public class natural_key_live_tests : OneOffConfigurationsContext
{
    private async Task ConfigureAndApply()
    {
        ConfigureStore(opts =>
        {
            // Register as Inline so the natural key projection is created,
            // but FetchForWriting always replays from events anyway
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task live_fetch_for_writing_by_natural_key()
    {
        await ConfigureAndApply();

        var streamId = Guid.NewGuid();
        var orderNumber = new OrderNumber("ORD-LIVE-001");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new NkOrderCreated(orderNumber, "Frank"),
            new NkOrderItemAdded("Live Widget", 15.00m),
            new NkOrderItemAdded("Live Gadget", 25.00m));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.OrderNum.ShouldBe(orderNumber);
        stream.Aggregate.CustomerName.ShouldBe("Frank");
        stream.Aggregate.TotalAmount.ShouldBe(40.00m);
        stream.StartingVersion.ShouldBe(3);
    }

    [Fact]
    public async Task live_fetch_latest_by_natural_key()
    {
        await ConfigureAndApply();

        var streamId = Guid.NewGuid();
        var orderNumber = new OrderNumber("ORD-LIVE-002");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new NkOrderCreated(orderNumber, "Grace"),
            new NkOrderItemAdded("Item A", 10.00m));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var aggregate = await session2.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber);

        aggregate.ShouldNotBeNull();
        aggregate!.OrderNum.ShouldBe(orderNumber);
        aggregate.CustomerName.ShouldBe("Grace");
        aggregate.TotalAmount.ShouldBe(10.00m);
    }
}

#endregion

#region String stream identity

public class natural_key_string_identity_tests : OneOffConfigurationsContext
{
    private async Task ConfigureAndApply()
    {
        ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task string_identity_fetch_for_writing_by_natural_key()
    {
        await ConfigureAndApply();

        var streamKey = "order-stream-str-001";
        var orderNumber = new OrderNumber("ORD-STR-001");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamKey,
            new NkOrderCreated(orderNumber, "Hank"),
            new NkOrderItemAdded("String Widget", 7.50m));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.OrderNum.ShouldBe(orderNumber);
        stream.Aggregate.CustomerName.ShouldBe("Hank");
        stream.Aggregate.TotalAmount.ShouldBe(7.50m);
        stream.StartingVersion.ShouldBe(2);

        // Append and save via natural key
        stream.AppendOne(new NkOrderCompleted());
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamKey);
        events.Count.ShouldBe(3);
    }

    [Fact]
    public async Task string_identity_fetch_latest_by_natural_key()
    {
        await ConfigureAndApply();

        var streamKey = "order-stream-str-002";
        var orderNumber = new OrderNumber("ORD-STR-002");

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamKey,
            new NkOrderCreated(orderNumber, "Irene"),
            new NkOrderItemAdded("String Gadget", 30.00m));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var aggregate = await session2.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber);

        aggregate.ShouldNotBeNull();
        aggregate!.OrderNum.ShouldBe(orderNumber);
        aggregate.CustomerName.ShouldBe("Irene");
        aggregate.TotalAmount.ShouldBe(30.00m);
    }
}

#endregion
