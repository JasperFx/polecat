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
///     #489: multi-stream projection whose grouping fans ONE event out into many slices, so the
///     async daemon hands a whole rangeful of slices to a single EfCoreProjectionStorage. That is
///     the shape marten#5266 reported, and the reason that storage declares IsThreadSafe => false.
/// </summary>
public class PlayerTallyProjection
    : EfCoreMultiStreamProjection<PlayerTally, string, TestDbContext>
{
    /// <summary>
    ///     #489 probe. Records how the async daemon dispatched each slice application: through
    ///     AggregationRunner's 10-wide <c>Block</c>, or inline. Only the storage's IsThreadSafe answer
    ///     decides that, so it is the one observable that actually distinguishes fixed from unfixed.
    /// </summary>
    public static bool ProbeEnabled;

    public static int ProbedCalls;

    public static bool SawBlockDispatch;

    public PlayerTallyProjection()
    {
        Identities<TeamScored>(e => e.PlayerNames);
    }

    public static void ResetProbe()
    {
        ProbedCalls = 0;
        SawBlockDispatch = false;
        ProbeEnabled = true;
    }

    protected override PlayerTally? ApplyEvent(PlayerTally? snapshot,
        string identity, IEvent @event, TestDbContext dbContext)
    {
        snapshot ??= new PlayerTally { Id = identity };

        // Sampled — a StackTrace per call over a whole rebuild would dominate the test's runtime, and
        // the dispatch route is a property of the storage, not of any individual slice.
        if (ProbeEnabled && Interlocked.Increment(ref ProbedCalls) <= 50)
        {
            if (new System.Diagnostics.StackTrace(false).ToString().Contains("JasperFx.Blocks.Block"))
            {
                SawBlockDispatch = true;
            }
        }

        if (@event.Data is TeamScored scored)
        {
            // Mutating a snapshot the DbContext is already tracking (on a rebuild it was loaded,
            // not constructed) is precisely what a concurrent Entry() would run DetectChanges over.
            snapshot.Points += scored.Points;
            snapshot.Appearances++;
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
