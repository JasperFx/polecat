# Single Stream Projections

Single stream projections build one aggregate document per event stream. This is the most common projection type.

## Defining a Projection

Use conventional `Apply` methods:

```cs
public class OrderSummary
{
    public Guid Id { get; set; }
    public string Status { get; set; } = "";
    public decimal TotalAmount { get; set; }
    public int ItemCount { get; set; }
    public DateTimeOffset? ShippedDate { get; set; }

    public static OrderSummary Create(OrderCreated e) =>
        new() { Status = "Created", TotalAmount = e.Amount };

    public void Apply(OrderItemAdded e)
    {
        TotalAmount += e.Price;
        ItemCount++;
    }

    public void Apply(OrderShipped e)
    {
        Status = "Shipped";
        ShippedDate = e.ShippedAt;
    }

    public bool ShouldDelete(OrderCancelled e) => true;
}
```

## Registration

### Inline

```cs
opts.Projections.Add<SingleStreamProjection<OrderSummary, Guid>>(ProjectionLifecycle.Inline);
```

The projection runs in the same transaction as event appending. The aggregate is stored in `pc_doc_ordersummary`.

### Async

```cs
opts.Projections.Add<SingleStreamProjection<OrderSummary, Guid>>(ProjectionLifecycle.Async);
```

The async daemon processes events in the background.

## Using IEvent Metadata

Access event metadata in your Apply methods:

```cs
public void Apply(IEvent<OrderCreated> @event)
{
    Status = "Created";
    TotalAmount = @event.Data.Amount;
    CreatedAt = @event.Timestamp;
    CreatedBy = @event.Headers?["user"]?.ToString();
}
```

## Live Aggregation

Use a single stream projection for on-demand replay without persisting:

```cs
var order = await session.Events.AggregateStreamAsync<OrderSummary>(streamId);
```

This replays all events in the stream through the `Create` and `Apply` methods.

## Custom SingleStreamProjection Class

For more control, extend `SingleStreamProjection<T>`:

```cs
public class OrderProjection : SingleStreamProjection<OrderSummary>
{
    public OrderSummary Create(OrderCreated e) =>
        new() { Status = "Created", TotalAmount = e.Amount };

    public void Apply(OrderItemAdded e, OrderSummary current)
    {
        current.TotalAmount += e.Price;
        current.ItemCount++;
    }
}

// Register
opts.Projections.Add<OrderProjection>(ProjectionLifecycle.Inline);
```
