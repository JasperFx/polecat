# Multi Stream Projections

Multi stream projections aggregate events from multiple streams into a single document.

## Defining a Multi Stream Projection

```cs
public class CustomerDashboard
{
    public Guid Id { get; set; }
    public int TotalOrders { get; set; }
    public decimal TotalSpent { get; set; }
    public DateTimeOffset? LastOrderDate { get; set; }
}

public class CustomerDashboardProjection : MultiStreamProjection<CustomerDashboard, Guid>
{
    public CustomerDashboardProjection()
    {
        // Route events to the correct aggregate by extracting the customer ID
        Identity<OrderCreated>(e => e.CustomerId);
        Identity<OrderShipped>(e => e.CustomerId);
        Identity<OrderCancelled>(e => e.CustomerId);
    }

    public static CustomerDashboard Create(OrderCreated e) =>
        new()
        {
            TotalOrders = 1,
            TotalSpent = e.Amount,
            LastOrderDate = DateTimeOffset.UtcNow
        };

    public void Apply(OrderCreated e, CustomerDashboard current)
    {
        current.TotalOrders++;
        current.TotalSpent += e.Amount;
        current.LastOrderDate = DateTimeOffset.UtcNow;
    }

    public void Apply(OrderCancelled e, CustomerDashboard current)
    {
        current.TotalOrders--;
        current.TotalSpent -= e.RefundAmount;
    }
}
```

## Registration

```cs
opts.Projections.Add<CustomerDashboardProjection>(ProjectionLifecycle.Async);
```

Multi stream projections can run inline or async, but async is typically recommended since they process events across streams.

## Event Routing

The `Identity<TEvent>()` method tells Polecat which aggregate document an event belongs to:

```cs
// Route by a property on the event
Identity<OrderCreated>(e => e.CustomerId);

// For string IDs
Identity<OrderCreated>(e => e.CustomerKey);
```

## Time-Based Segmentation

Multi-stream projections can segment a single event stream by time period. This is useful for monthly reports, daily summaries, billing periods, or any scenario where you need per-period aggregations of a single stream's events.

The key technique is using a composite identity that combines the stream ID with a time bucket (e.g., `"{streamId}:{yyyy-MM}"`), derived from the event's timestamp metadata via `IEvent<T>`.

**Events:**

<!-- snippet: sample_polecat_monthly_account_activity_events -->
<!-- endSnippet -->

**Read model document:**

<!-- snippet: sample_polecat_monthly_account_activity_document -->
<!-- endSnippet -->

**Projection with time-based routing:**

<!-- snippet: sample_polecat_monthly_account_activity_projection -->
<!-- endSnippet -->

Each account stream's events are routed to monthly documents automatically. Querying is straightforward:

```cs
// Get all monthly summaries for an account
var monthlies = await session.Query<MonthlyAccountActivity>()
    .Where(x => x.AccountId == accountId)
    .OrderBy(x => x.Month)
    .ToListAsync();

// Get a specific month
var jan = await session.LoadAsync<MonthlyAccountActivity>($"{accountId}:2026-01");
```

## ID Types

Multi stream projections support any ID type for the aggregate:

```cs
// Guid IDs
public class MyProjection : MultiStreamProjection<MyDoc, Guid> { }

// String IDs
public class MyProjection : MultiStreamProjection<MyDoc, string> { }

// Int IDs
public class MyProjection : MultiStreamProjection<MyDoc, int> { }
```
