# Projections Overview

Projections are the mechanism for building read models from events. Polecat supports several projection types and lifecycle strategies.

## Projection Lifecycle

Every projection runs with one of three lifecycle strategies:

### Inline

Projections run in the **same transaction** as the event append. This provides strong consistency -- the read model is always up to date:

```cs
opts.Projections.Add<SingleStreamProjection<OrderSummary, Guid>>(ProjectionLifecycle.Inline);
```

### Async

Projections run in the **background** via the async daemon. The read model is eventually consistent:

```cs
opts.Projections.Add<SingleStreamProjection<OrderSummary, Guid>>(ProjectionLifecycle.Async);
```

### Live

Projections are built **on demand** by replaying events each time. No read model is persisted:

```cs
var order = await session.Events.AggregateStreamAsync<OrderSummary>(streamId);
```

## Projection Types

| Type | Description | Use Case |
| :--- | :--- | :--- |
| [Single Stream](/events/projections/single-stream-projections) | One aggregate per stream | Order, Invoice, Account |
| [Multi Stream](/events/projections/multi-stream-projections) | Aggregate across multiple streams | Dashboard, Report |
| [Event Projection](/events/projections/event-projections) | Per-event document creation | Audit log, Search index |
| [Flat Table](/events/projections/flat) | Direct SQL table writes | Reporting, Analytics |
| [Composite](/events/projections/composite) | Multi-stage orchestration | Complex pipelines |

## Conventional Projection Methods

Polecat discovers projection methods by convention:

### Create

Creates the initial aggregate from a stream-starting event:

```cs
public static OrderSummary Create(OrderCreated e) =>
    new() { Status = "Created", Amount = e.Amount };
```

### Apply

Applies an event to an existing aggregate:

```cs
public void Apply(OrderShipped e)
{
    Status = "Shipped";
    ShippedDate = e.ShippedAt;
}
```

### ShouldDelete

Signals that the aggregate should be deleted:

```cs
public bool ShouldDelete(OrderCancelled e) => true;
```

## Registration

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // Single stream projection (inline)
    opts.Projections.Add<SingleStreamProjection<OrderSummary, Guid>>(ProjectionLifecycle.Inline);

    // Multi stream projection as async
    opts.Projections.Add<DashboardProjection>(ProjectionLifecycle.Async);

    // Event projection
    opts.Projections.Add<AuditLogProjection>(ProjectionLifecycle.Inline);
});
```
