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
<a id='snippet-sample_polecat_monthly_account_activity_events'></a>
```cs
public record AccountOpened(string AccountName);
public record AccountDebited(decimal Amount, string Description);
public record AccountCredited(decimal Amount, string Description);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Projections/time_based_multi_stream_projection_tests.cs#L9-L15' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_monthly_account_activity_events' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

**Read model document:**

<!-- snippet: sample_polecat_monthly_account_activity_document -->
<a id='snippet-sample_polecat_monthly_account_activity_document'></a>
```cs
public partial class MonthlyAccountActivity
{
    public string Id { get; set; } = "";
    public Guid AccountId { get; set; }
    public string Month { get; set; } = "";
    public int TransactionCount { get; set; }
    public decimal TotalDebits { get; set; }
    public decimal TotalCredits { get; set; }
    public decimal NetChange => TotalCredits - TotalDebits;
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Projections/time_based_multi_stream_projection_tests.cs#L17-L30' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_monthly_account_activity_document' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

**Projection with time-based routing:**

<!-- snippet: sample_polecat_monthly_account_activity_projection -->
<a id='snippet-sample_polecat_monthly_account_activity_projection'></a>
```cs
public partial class MonthlyAccountActivityProjection : MultiStreamProjection<MonthlyAccountActivity, string>
{
    public MonthlyAccountActivityProjection()
    {
        // Route each event to a document keyed by "{streamId}:{yyyy-MM}"
        // This segments a single account stream into monthly summaries
        Identity<IEvent<AccountDebited>>(e =>
            $"{e.StreamId}:{e.Timestamp:yyyy-MM}");
        Identity<IEvent<AccountCredited>>(e =>
            $"{e.StreamId}:{e.Timestamp:yyyy-MM}");
    }

    public MonthlyAccountActivity Create(IEvent<AccountDebited> e) => new()
    {
        AccountId = e.StreamId,
        Month = e.Timestamp.ToString("yyyy-MM"),
        TransactionCount = 1,
        TotalDebits = e.Data.Amount
    };

    public MonthlyAccountActivity Create(IEvent<AccountCredited> e) => new()
    {
        AccountId = e.StreamId,
        Month = e.Timestamp.ToString("yyyy-MM"),
        TransactionCount = 1,
        TotalCredits = e.Data.Amount
    };

    public void Apply(IEvent<AccountDebited> e, MonthlyAccountActivity view)
    {
        view.TransactionCount++;
        view.TotalDebits += e.Data.Amount;
    }

    public void Apply(IEvent<AccountCredited> e, MonthlyAccountActivity view)
    {
        view.TransactionCount++;
        view.TotalCredits += e.Data.Amount;
    }
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Projections/time_based_multi_stream_projection_tests.cs#L32-L75' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_monthly_account_activity_projection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Register the projection as inline (for immediate consistency) or async (for eventual consistency):

```cs
// Inline — projected immediately during SaveChangesAsync
opts.Projections.Add<MonthlyAccountActivityProjection>(ProjectionLifecycle.Inline);

// Async — projected by the async daemon in the background
opts.Projections.Add<MonthlyAccountActivityProjection>(ProjectionLifecycle.Async);
```

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

::: tip
When using async projections, make sure to start the async daemon — see [Asynchronous Projections](/events/projections/async-daemon) for details.
:::

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
