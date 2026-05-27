# Event Projections

Event projections process individual events to create, modify, or delete documents. Unlike aggregate projections, they don't maintain per-stream state.

## Defining an Event Projection

```cs
public class AuditLogProjection : EventProjection
{
    public void Project(IEvent<OrderCreated> @event, IDocumentOperations ops)
    {
        ops.Store(new AuditEntry
        {
            Id = Guid.NewGuid(),
            Action = "OrderCreated",
            StreamId = @event.StreamId,
            Timestamp = @event.Timestamp,
            Details = $"Order created for {@@event.Data.Amount:C}"
        });
    }

    public void Project(IEvent<OrderCancelled> @event, IDocumentOperations ops)
    {
        ops.Store(new AuditEntry
        {
            Id = Guid.NewGuid(),
            Action = "OrderCancelled",
            StreamId = @event.StreamId,
            Timestamp = @event.Timestamp,
            Details = "Order was cancelled"
        });
    }
}
```

The examples name the event parameter `@event`, but that is not required: Polecat identifies the event argument **by type**. In `Project(IEvent<OrderCreated> @event, IDocumentOperations ops)`, the `IEvent<OrderCreated>` parameter is the event regardless of its name (`IDocumentOperations` is an interface and is never treated as the event). You only need a conventional event parameter **name** — `@event`, `event`, `e`, or `ev` — to disambiguate a signature in which more than one parameter could be the event. This is the same rule used by aggregate projections; see [Identifying the Event Parameter](/events/projections/single-stream-projections#identifying-the-event-parameter).

## Registration

```cs
opts.Projections.Add<AuditLogProjection>(ProjectionLifecycle.Inline);
// or
opts.Projections.Add<AuditLogProjection>(ProjectionLifecycle.Async);
```

## Use Cases

Event projections are ideal for:

- **Audit logs** -- Create a record for each significant event
- **Search indexes** -- Maintain denormalized documents for search
- **Notifications** -- Create notification records per event
- **Cross-cutting concerns** -- Track metrics, analytics, or compliance data

## Accessing Session Operations

The `IDocumentOperations` parameter gives you full access to document operations:

```cs
public void Project(IEvent<UserDeactivated> @event, IDocumentOperations ops)
{
    // Store new documents
    ops.Store(new DeactivationRecord { ... });

    // Delete documents
    ops.Delete<ActiveUser>(@event.Data.UserId);

    // Patch existing documents
    ops.Patch<UserStats>(@event.Data.UserId)
        .Set(x => x.IsActive, false);
}
```

## Event Enrichment

`EventProjection` supports an `EnrichEventsAsync` hook that runs **before** individual events
are processed. This allows you to batch-load reference data from the database and enrich events
with it, avoiding N+1 query problems.

::: warning
Event enrichment is designed for **read model / query model** projections processed by the async
daemon or inline during `SaveChangesAsync`. It is **not** called during `FetchForWriting()` or
`FetchLatest()`. Avoid depending on enriched data in write model aggregates used with those APIs.
:::

Override `EnrichEventsAsync` in your `EventProjection` subclass:

```cs
public class TaskSummaryProjection : EventProjection
{
    public TaskSummaryProjection()
    {
        Project<TaskAssigned>((e, ops) =>
        {
            ops.Store(new TaskSummary
            {
                Id = e.TaskId,
                AssignedUserName = e.UserName // Set by enrichment
            });
        });
    }

    public override async Task EnrichEventsAsync(
        IQuerySession querySession,
        IReadOnlyList<IEvent> events,
        CancellationToken cancellation)
    {
        var assigned = events.OfType<IEvent<TaskAssigned>>().ToArray();
        if (assigned.Length == 0) return;

        var userIds = assigned.Select(e => e.Data.UserId).Distinct().ToArray();

        foreach (var userId in userIds)
        {
            var user = await querySession.LoadAsync<User>(userId, cancellation);
            if (user != null)
            {
                foreach (var e in assigned.Where(a => a.Data.UserId == userId))
                {
                    e.Data.UserName = user.Name;
                }
            }
        }
    }
}
```

The method is called once per tenant batch before any `Project<T>` handlers run. Modifications
to event data properties are visible to all subsequent handlers in the batch.
