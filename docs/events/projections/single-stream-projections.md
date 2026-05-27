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

## Identifying the Event Parameter

Notice that the examples above name the event parameter `e` in some methods and `@event` in others — both work. Polecat identifies the event argument of a conventional `Create` / `Apply` / `ShouldDelete` method (and an [event projection](/events/projections/event-projections)'s `Project` / `Transform` methods) **by type, not by name**, using the same rule for every projection type:

1. A parameter typed `IEvent<T>` is always the event, and `T` is the event type — use this when you need event metadata such as `Timestamp` or `Headers`.
2. Otherwise the single **concrete** parameter that is not an interface (`IQuerySession`, `IDocumentOperations`), not `IEvent`, not `CancellationToken`, and not the aggregate type is the event.

So `Apply(OrderShipped e)` and `Apply(OrderShipped shipped)` are equivalent — the parameter name is incidental. A conventional event parameter **name** is only consulted to disambiguate an unusual signature in which more than one parameter could be the event; the recognized names are `@event`, `event`, `e`, and `ev`.

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
