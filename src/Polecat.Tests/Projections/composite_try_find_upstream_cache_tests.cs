using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Grouping;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

#region sample_polecat_composite_upstream_cache_events

public record CompositeOrderPlaced(Guid CustomerId, decimal Total);
public record CompositeOrderShipped(string Carrier);

#endregion

#region sample_polecat_composite_upstream_cache_documents

public class CompositeOrder
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public decimal Total { get; set; }
    public bool IsShipped { get; set; }
}

public class OrderShippingNotification
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public decimal OrderTotal { get; set; }
    public string Carrier { get; set; } = string.Empty;
}

#endregion

public class CompositeOrderProjection : SingleStreamProjection<CompositeOrder, Guid>
{
    public CompositeOrderProjection()
    {
        Options.CacheLimitPerTenant = 1000;
    }

    public override CompositeOrder? Evolve(CompositeOrder? snapshot, Guid id, IEvent e)
    {
        switch (e.Data)
        {
            case CompositeOrderPlaced placed:
                snapshot = new CompositeOrder
                {
                    Id = id,
                    CustomerId = placed.CustomerId,
                    Total = placed.Total
                };
                break;

            case CompositeOrderShipped:
                if (snapshot != null) snapshot.IsShipped = true;
                break;
        }

        return snapshot;
    }
}

#region sample_polecat_try_find_upstream_cache

public class OrderShippingNotificationProjection : MultiStreamProjection<OrderShippingNotification, Guid>
{
    public OrderShippingNotificationProjection()
    {
        Identity<IEvent<CompositeOrderShipped>>(e => e.StreamId);
    }

    public override Task EnrichEventsAsync(SliceGroup<OrderShippingNotification, Guid> group,
        IQuerySession querySession, CancellationToken cancellation)
    {
        // Ask the upstream CompositeOrderProjection (running earlier in the same composite stage)
        // for its in-memory aggregate cache. A SQL query for CompositeOrder in this same batch
        // would return nothing — those writes are still queued on the shared IProjectionBatch
        // and have not been committed to SQL Server yet.
        if (!group.TryFindUpstreamCache<Guid, CompositeOrder>(out var upstreamOrders))
        {
            return Task.CompletedTask;
        }

        foreach (var slice in group.Slices)
        {
            if (upstreamOrders.TryFind(slice.Id, out var order))
            {
                // Stamp a synthetic References<CompositeOrder> event onto the slice so
                // the Evolve method can read the upstream entity's data.
                slice.Reference(order);
            }
        }

        return Task.CompletedTask;
    }

    public override OrderShippingNotification? Evolve(OrderShippingNotification? snapshot, Guid id, IEvent e)
    {
        switch (e.Data)
        {
            case CompositeOrderShipped shipped:
                snapshot ??= new OrderShippingNotification { Id = id };
                snapshot.Carrier = shipped.Carrier;
                break;

            case References<CompositeOrder> orderRef:
                snapshot ??= new OrderShippingNotification { Id = id };
                snapshot.CustomerId = orderRef.Entity.CustomerId;
                snapshot.OrderTotal = orderRef.Entity.Total;
                break;
        }

        return snapshot;
    }
}

#endregion

[Collection("integration")]
public class composite_try_find_upstream_cache_tests : IntegrationContext
{
    public composite_try_find_upstream_cache_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task downstream_stage_can_read_upstream_in_flight_order_via_upstream_cache()
    {
        // Regression coverage from JasperFx/marten#4329 / JasperFx/jasperfx#205,
        // ported into Polecat. Confirms that downstream composite stages can read
        // upstream-stage aggregates via the in-memory cache, even when the
        // upstream writes are still queued on the shared projection batch.
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "composite_upstream_cache";

            opts.Projections.CompositeProjectionFor("OrderComposite", composite =>
            {
                composite.Add<CompositeOrderProjection>();                  // stage 1
                composite.Add<OrderShippingNotificationProjection>(2);      // stage 2
            });
        });

        var orderId = Guid.NewGuid();
        var customerId = Guid.NewGuid();

        // Both events arrive in a single batch so the CompositeOrder document is being
        // produced and the OrderShippingNotification is being computed in the same
        // composite execution.
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<CompositeOrder>(orderId,
            new CompositeOrderPlaced(customerId, 99.95m),
            new CompositeOrderShipped("UPS"));
        await session.SaveChangesAsync();

        await theStore.WaitForProjectionAsync();

        await using var query = theStore.QuerySession();
        var notification = await query.LoadAsync<OrderShippingNotification>(orderId);

        notification.ShouldNotBeNull();
        notification!.CustomerId.ShouldBe(customerId);
        notification.OrderTotal.ShouldBe(99.95m);
        notification.Carrier.ShouldBe("UPS");
    }

    [Fact]
    public async Task tiny_upstream_cache_limit_does_not_starve_downstream_stage()
    {
        // Regression coverage from JasperFx/jasperfx#206. Before that fix,
        // CompactIfNecessary at the end of an upstream batch could evict the
        // entries a downstream stage was about to read via TryFindUpstreamCache,
        // producing zero-data downstream notifications even though every upstream
        // write succeeded. With JasperFx.Events 1.35.0 in place, the upstream
        // cache is held until *all* composite stages have run.
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "composite_tiny_cache";

            opts.Projections.CompositeProjectionFor("TinyCacheOrderComposite", composite =>
            {
                composite.Add(new CompositeOrderProjection
                {
                    Options = { CacheLimitPerTenant = 1 }
                });
                composite.Add<OrderShippingNotificationProjection>(2);
            });
        });

        const int orderCount = 8;
        var orderIds = Enumerable.Range(0, orderCount).Select(_ => Guid.NewGuid()).ToArray();
        var customerId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            foreach (var id in orderIds)
            {
                session.Events.StartStream<CompositeOrder>(id,
                    new CompositeOrderPlaced(customerId, 10m),
                    new CompositeOrderShipped("DHL"));
            }
            await session.SaveChangesAsync();
        }

        await theStore.WaitForProjectionAsync();

        // The point of the regression: every downstream notification should be
        // enriched from the upstream cache, even with CacheLimitPerTenant=1.
        // Before the 1.35.0 fix, OrderTotal/CustomerId were left at default(T)
        // for the slices whose upstream entries had already been evicted.
        await using var query = theStore.QuerySession();
        foreach (var id in orderIds)
        {
            var notification = await query.LoadAsync<OrderShippingNotification>(id);
            notification.ShouldNotBeNull($"Missing notification for order {id}");
            notification!.Carrier.ShouldBe("DHL");
            notification.OrderTotal.ShouldBe(10m);
            notification.CustomerId.ShouldBe(customerId);
        }
    }
}
