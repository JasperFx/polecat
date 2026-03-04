using JasperFx.Events;

namespace Polecat.EntityFrameworkCore.Tests;

/// <summary>
///     Single-stream projection: builds Order aggregate + writes OrderSummary side effect.
/// </summary>
public class OrderAggregate : EfCoreSingleStreamProjection<Order, TestDbContext>
{
    public OrderAggregate()
    {
        IncludeType<OrderPlaced>();
        IncludeType<OrderShipped>();
        IncludeType<OrderCancelled>();
    }

    protected override Order? ApplyEvent(Order? snapshot, Guid identity, IEvent @event,
        TestDbContext dbContext, IQuerySession session)
    {
        switch (@event.Data)
        {
            case OrderPlaced placed:
                // Write side effect to EF Core
                dbContext.OrderSummaries.Add(new OrderSummary
                {
                    Id = placed.OrderId,
                    CustomerName = placed.CustomerName,
                    TotalAmount = placed.Amount,
                    ItemCount = placed.Items,
                    Status = "Placed"
                });
                return new Order
                {
                    Id = placed.OrderId,
                    CustomerName = placed.CustomerName,
                    TotalAmount = placed.Amount,
                    ItemCount = placed.Items
                };

            case OrderShipped:
                if (snapshot != null) snapshot.IsShipped = true;
                return snapshot;

            case OrderCancelled:
                if (snapshot != null) snapshot.IsCancelled = true;
                return snapshot;
        }

        return snapshot;
    }
}

/// <summary>
///     Multi-stream projection: aggregates across streams by customer name.
/// </summary>
public class CustomerOrderHistoryProjection
    : EfCoreMultiStreamProjection<CustomerOrderHistory, string, TestDbContext>
{
    public CustomerOrderHistoryProjection()
    {
        Identity<CustomerOrderPlaced>(e => e.CustomerName);
        Identity<CustomerOrderCompleted>(e => e.CustomerName);
    }

    protected override CustomerOrderHistory? ApplyEvent(CustomerOrderHistory? snapshot,
        string identity, IEvent @event, TestDbContext dbContext)
    {
        snapshot ??= new CustomerOrderHistory { Id = identity };

        switch (@event.Data)
        {
            case CustomerOrderPlaced placed:
                snapshot.TotalOrders++;
                snapshot.TotalSpent += placed.Amount;
                break;
        }

        return snapshot;
    }
}

/// <summary>
///     Event projection: dual-writes to both EF Core (OrderDetail) and Polecat (OrderLog).
/// </summary>
public class OrderDetailProjection : EfCoreEventProjection<TestDbContext>
{
    public OrderDetailProjection()
    {
        IncludeType<OrderPlaced>();
        IncludeType<OrderShipped>();
    }

    protected override Task ProjectAsync(IEvent @event, TestDbContext dbContext,
        IDocumentOperations operations, CancellationToken token)
    {
        switch (@event.Data)
        {
            case OrderPlaced placed:
                // Write to EF Core
                dbContext.OrderDetails.Add(new OrderDetail
                {
                    Id = placed.OrderId,
                    CustomerName = placed.CustomerName,
                    TotalAmount = placed.Amount,
                    ItemCount = placed.Items,
                    Status = "Placed"
                });
                // Also write to Polecat
                operations.Store(new OrderLog
                {
                    Id = placed.OrderId,
                    CustomerName = placed.CustomerName,
                    EventType = "OrderPlaced"
                });
                break;

            case OrderShipped shipped:
                var detail = dbContext.OrderDetails.Find(shipped.OrderId);
                if (detail != null)
                {
                    detail.IsShipped = true;
                    detail.Status = "Shipped";
                }

                break;
        }

        return Task.CompletedTask;
    }
}

/// <summary>
///     Tenanted single-stream projection.
/// </summary>
public class TenantedOrderAggregate : EfCoreSingleStreamProjection<TenantedOrder, TenantedTestDbContext>
{
    public TenantedOrderAggregate()
    {
        IncludeType<OrderPlaced>();
        IncludeType<OrderShipped>();
        IncludeType<OrderCancelled>();
    }

    protected override TenantedOrder? ApplyEvent(TenantedOrder? snapshot, Guid identity, IEvent @event,
        TenantedTestDbContext dbContext, IQuerySession session)
    {
        switch (@event.Data)
        {
            case OrderPlaced placed:
                return new TenantedOrder
                {
                    Id = placed.OrderId,
                    CustomerName = placed.CustomerName,
                    TotalAmount = placed.Amount,
                    ItemCount = placed.Items,
                    TenantId = @event.TenantId
                };

            case OrderShipped:
                if (snapshot != null) snapshot.IsShipped = true;
                return snapshot;

            case OrderCancelled:
                if (snapshot != null) snapshot.IsCancelled = true;
                return snapshot;
        }

        return snapshot;
    }
}

/// <summary>
///     Non-tenanted projection for validation tests.
/// </summary>
public class NonTenantedOrderAggregate : EfCoreSingleStreamProjection<NonTenantedOrder, TestDbContext>
{
    public NonTenantedOrderAggregate()
    {
        IncludeType<OrderPlaced>();
    }

    protected override NonTenantedOrder? ApplyEvent(NonTenantedOrder? snapshot, Guid identity, IEvent @event,
        TestDbContext dbContext, IQuerySession session)
    {
        if (@event.Data is OrderPlaced placed)
        {
            return new NonTenantedOrder
            {
                Id = placed.OrderId,
                CustomerName = placed.CustomerName
            };
        }

        return snapshot;
    }
}

/// <summary>
///     Non-tenanted multi-stream projection for validation tests.
/// </summary>
public class NonTenantedMultiStreamProjection
    : EfCoreMultiStreamProjection<NonTenantedOrder, Guid, TestDbContext>
{
    public NonTenantedMultiStreamProjection()
    {
        Identity<OrderPlaced>(e => e.OrderId);
    }

    protected override NonTenantedOrder? ApplyEvent(NonTenantedOrder? snapshot,
        Guid identity, IEvent @event, TestDbContext dbContext)
    {
        if (@event.Data is OrderPlaced placed)
        {
            return new NonTenantedOrder
            {
                Id = placed.OrderId,
                CustomerName = placed.CustomerName
            };
        }

        return snapshot;
    }
}
