# Inline Projections

Inline projections run in the **same transaction** as the event append, providing strong consistency between events and their read models.

## How It Works

When `SaveChangesAsync()` is called:

1. Events are inserted into `pc_events`
2. Stream versions are updated in `pc_streams`
3. Inline projections process the new events
4. Projection documents are upserted into their tables
5. All operations commit in a single transaction

If any step fails, the entire transaction rolls back -- events and projections are always consistent.

## Registration

Register a projection for inline processing:

```cs
// Single stream projection
opts.Projections.Add<SingleStreamProjection<OrderSummary, Guid>>(ProjectionLifecycle.Inline);

// Event projection
opts.Projections.Add<AuditLogProjection>(ProjectionLifecycle.Inline);

// Multi stream projection
opts.Projections.Add<CustomerDashboardProjection>(ProjectionLifecycle.Inline);
```

## When to Use Inline Projections

Inline projections are ideal when:

- **Read models must always be consistent** with events
- **Queries immediately follow writes** in the same request
- **The projection is simple** and fast (doesn't slow down writes)

## Trade-offs

| Advantage | Disadvantage |
| :--- | :--- |
| Strong consistency | Adds latency to writes |
| No staleness window | Transaction grows larger |
| Simpler architecture | No async daemon needed, but more write contention |

## Inline vs Async

If your projections are complex or slow, consider [async projections](/events/projections/async-daemon) instead. The async daemon processes events in the background, decoupling write performance from projection processing.

::: tip
Start with inline projections for simplicity. Move to async if write performance becomes a concern.
:::
