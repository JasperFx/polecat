using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

#region sample_polecat_natural_key_aggregate_types

public record OrderNumber(string Value);

public partial class OrderAggregate
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

public partial class InvoiceAggregate
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
            session.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber, TestContext.Current.CancellationToken));
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
        await session1.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        #region sample_polecat_fetch_for_writing_by_natural_key
        // FetchForWriting by the business identifier instead of stream id
        var stream = await session2.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber, TestContext.Current.CancellationToken);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.OrderNum.ShouldBe(orderNumber);

        // Append new events through the stream
        stream.AppendOne(new NkOrderItemAdded("Gadget", 19.99m));
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        #endregion
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
        await session1.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        #region sample_polecat_fetch_latest_by_natural_key
        // Read-only access by natural key
        var aggregate = await session2.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber, TestContext.Current.CancellationToken);
        #endregion

        aggregate.ShouldNotBeNull();
        aggregate!.OrderNum.ShouldBe(orderNumber);
        aggregate.CustomerName.ShouldBe("Charlie");
        aggregate.TotalAmount.ShouldBe(5.50m);
    }

    [Fact]
    public async Task natural_key_with_primitive_string_type()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Add<SingleStreamProjection<InvoiceAggregate, Guid>>(ProjectionLifecycle.Inline);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();
        var invoiceCode = "INV-2024-001";

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId,
            new NkInvoiceCreated(invoiceCode, 250.00m));
        await session1.SaveChangesAsync(TestContext.Current.CancellationToken);

        // FetchForWriting by primitive string key
        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<InvoiceAggregate, string>(invoiceCode, TestContext.Current.CancellationToken);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.InvoiceCode.ShouldBe(invoiceCode);
        stream.Aggregate.Amount.ShouldBe(250.00m);
        stream.Id.ShouldBe(streamId);

        // FetchLatest by primitive string key
        await using var session3 = theStore.LightweightSession();
        var latest = await session3.Events.FetchLatest<InvoiceAggregate, string>(invoiceCode, TestContext.Current.CancellationToken);

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
        await session1.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber, TestContext.Current.CancellationToken);

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
        await session1.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        var aggregate = await session2.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber, TestContext.Current.CancellationToken);

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

}

#endregion

/* Six facts were retired from this file when Polecat enrolled NaturalKeyCompliance (#556): the
 * append-after-fetch round trip, the FetchLatest miss, exclusive writing, key mutation, and the two
 * string-identity fetches. The shared suite carries all six under the same names, plus the archive,
 * clean, conjoined-tenancy and rebuild facts this file never had.
 *
 * What stays, and why none of it is duplication for its own sake:
 *
 *   fetch_for_writing_existing_stream_by_natural_key
 *   fetch_latest_by_natural_key   -- both ARE in the suite, and both stay because they carry the
 *                                    docs snippets mdsnippets pulls into the natural-key docs. A
 *                                    compiling, executing sample earns its one duplicate copy.
 *
 *   fetch_for_writing_new_stream_by_natural_key
 *                                 -- the MISS case, which the suite deliberately excludes because
 *                                    the products disagree: Marten returns a null aggregate at
 *                                    version 0, Polecat throws (NaturalKeyFetchPlanner). Polecat's
 *                                    answer is a product decision and needs a product test.
 *
 *   natural_key_with_primitive_string_type
 *                                 -- overlaps the suite's primitive-key fact, kept because it also
 *                                    asserts the lookup ROW, which no shared contract can reach.
 *
 *   live_fetch_for_writing_by_natural_key
 *   live_fetch_latest_by_natural_key
 *                                 -- the Live lifecycle pair. The suite covers Inline and Async
 *                                    only; Live is the third registration Polecat supports and
 *                                    nothing shared pins it.
 */
