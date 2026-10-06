# Event Subscriptions

Polecat supports event subscriptions for push-based processing of events as they are appended.

## ISubscription Interface

Implement `ISubscription` to process events:

```cs
public class OrderNotificationSubscription : SubscriptionBase
{
    public override async Task ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken ct)
    {
        foreach (var @event in page.Events)
        {
            if (@event.Data is OrderCreated created)
            {
                // Send notification, update external system, etc.
                await SendNotification(created);
            }
        }
    }
}
```

## Registering Subscriptions

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");
    opts.Projections.Subscribe(new OrderNotificationSubscription());
});
```

## How Subscriptions Work

Subscriptions are processed by the async daemon alongside projections:

1. The daemon tracks progression via `pc_event_progression`
2. Events are loaded in batches
3. Your subscription's `ProcessEventsAsync` is called for each batch
4. Progression is updated after successful processing

## Subscriptions vs Projections

| Feature | Subscription | Projection |
| :--- | :--- | :--- |
| Purpose | Side effects (notifications, external systems) | Read model construction |
| Output | Arbitrary | Documents or flat tables |
| Replay | May not be idempotent | Should be idempotent |
| Processing | Sequential batches | Sequential batches |

## SubscriptionBase

The `SubscriptionBase` class provides a convenient base with default implementations. Override `ProcessEventsAsync` to handle events.

::: tip
Unlike projections, subscriptions are intended for side effects like sending emails, updating external systems, or triggering workflows. They are not expected to be idempotent or replayable — but see
[Shutdown, rebalance and duplicate delivery](#shutdown-rebalance-and-duplicate-delivery) below, because the
daemon cannot promise a batch is delivered only once.
:::

## Shutdown, Rebalance and Duplicate Delivery

A subscription's progression row is only advanced **after** `ProcessEventsAsync` returns successfully. That is
what makes a subscription safe to interrupt — nothing is marked done that did not happen — and it is also why a
subscription can see the same batch twice.

When a shard is stopped, the daemon drains it, bounded by
[`StopAndDrainTimeout`](/events/projections/async-daemon#graceful-shutdown-and-the-drain-timeout) (default
**5 seconds**). If your `ProcessEventsAsync` is still running when that bound expires, it is **cancelled, its
progression is not recorded, and the whole batch is delivered again** on the next start — to this node or to
whichever node picks the shard up.

So a subscription whose batch is slower than `StopAndDrainTimeout` will re-deliver that batch on every shutdown
and on every HotCold rebalance. Two ways out, and they are not exclusive:

```cs
// 1. Give the drain long enough to finish your slowest batch
opts.Projections.StopAndDrainTimeout = TimeSpan.FromSeconds(30);
```

2. Make the handling idempotent — deduplicate on the event's `Id`, or make the external call idempotent with an
   idempotency key. This is the only option that also covers a crash, which gets no drain at all.

::: warning
Honour the `CancellationToken` passed to `ProcessEventsAsync`. A subscription that ignores it keeps running after
the drain has given up on it, which is how a node that has lost a shard's lock under `DaemonMode.HotCold` ends up
working the same events as the node that now owns it.
:::
