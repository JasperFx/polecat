# Side Effects

::: tip
By default, side effects only fire during _continuous_ asynchronous projection execution.
They do not run during projection rebuilds. Inline projections can opt in via
[`EnableSideEffectsOnInlineProjections`](#side-effects-in-inline-projections).
:::

_Sometimes_, it can be valuable to emit new events during the processing of a projection
when you first know the new state of the projected aggregate documents. Or maybe what you
want to do is send a message reflecting the new state of an updated projection. Here are a
few scenarios that might lead you here:

- There's some kind of business logic that can be processed against an aggregate to "decide" what the system can do next
- You need to send updates about the aggregated projection state to clients via web sockets
- You need to replicate the Polecat projection data in a completely different database
- There are business processes that can be kicked off for updates to the aggregated state

To do any of this, you can override the `RaiseSideEffects()` method in any aggregated
projection that uses one of the following base classes:

1. `SingleStreamProjection<TDoc, TId>`
2. `MultiStreamProjection<TDoc, TId>`

Here's an example of that method overridden in a projection:

```cs
public class TripProjection : SingleStreamProjection<Trip, Guid>
{
    public static Trip Create(IEvent<TripStarted> @event) => new()
    {
        Id = @event.StreamId,
        Started = @event.Timestamp,
        Description = @event.Data.Description
    };

    public void Apply(TripEnded ended, Trip trip, IEvent @event)
    {
        trip.Ended = @event.Timestamp;
    }

    // Other Apply / ShouldDelete methods...

    public override ValueTask RaiseSideEffects(IDocumentSession session, IEventSlice<Trip> slice)
    {
        // Access to the current state as of the projection
        // event page being processed *right* now
        var currentTrip = slice.Snapshot;

        if (currentTrip.TotalMiles > 1000)
        {
            // Append a new event to this stream
            slice.AppendEvent(new PassedThousandMiles());

            // Append a new event to a different event stream by
            // first specifying a different stream id
            slice.AppendEvent(currentTrip.InsuranceCompanyId, new IncrementThousandMileTrips());

            // "Publish" outgoing messages when the event page is successfully committed
            slice.PublishMessage(new SendCongratulationsOnLongTrip(currentTrip.Id));

            // And yep, you can make additional changes to Polecat
            session.Store(new CompletelyDifferentDocument
            {
                Name = "New Trip Segment",
                OriginalTripId = currentTrip.Id
            });
        }

        return new ValueTask();
    }
}
```

A few important facts about this functionality:

- The `RaiseSideEffects()` method is only called during _continuous_ asynchronous projection
  execution. It is **not** called during projection rebuilds. For `Inline` projections,
  it is opt-in via [`EnableSideEffectsOnInlineProjections`](#side-effects-in-inline-projections).
- Events emitted during the side effect method are _not_ immediately applied to the current
  projected document value by Polecat
- You _can_ alter the aggregate value or replace it yourself in this side effect method to
  reflect new events, but the onus is on you the user to apply idempotent updates to the
  aggregate based on these new events in the actual handlers for the new events when those
  events are handled by the daemon in a later batch

## Routing Published Messages

By default, calls to `slice.PublishMessage(...)` are dropped — Polecat ships a no-op
`IMessageOutbox` so projections that do not need to emit messages incur zero overhead.

To actually deliver published messages, register an `IMessageOutbox` implementation on
the event store options:

```cs
builder.Services.AddPolecat(opts =>
{
    opts.Connection(builder.Configuration.GetConnectionString("polecat"));

    // Replace the default no-op outbox with one that hands the messages
    // off to your messaging infrastructure
    opts.Events.MessageOutbox = new MyCustomMessageOutbox();
});
```

Each implementation of `IMessageOutbox` vends a fresh `IMessageBatch` for every projection
update. The batch is enlisted as a post-commit listener on the projection update —
`BeforeCommitAsync` fires inside the projection's SQL transaction (right before `COMMIT`)
and `AfterCommitAsync` fires once the projection's database changes are durably committed.
This lets implementations choose between "at-least-once" patterns (persist the outgoing
messages to a database table inside the same transaction) and "best-effort" patterns
(flush to an external broker only after the projection write succeeds).

A first-class [Wolverine](https://wolverinefx.net) integration is on the roadmap that will
plug Wolverine's outbox in via `IntegrateWithWolverine()`, mirroring the existing
Marten/Wolverine bridge.

## Side Effects in Inline Projections

By default, Polecat only processes projection side effects during continuous asynchronous
processing. To process them when running projections under the `Inline` lifecycle as well,
flip the opt-in setting on the event store options:

```cs
builder.Services.AddPolecat(opts =>
{
    opts.Connection(builder.Configuration.GetConnectionString("polecat"));

    // Run RaiseSideEffects() for inline projections too
    opts.EventGraph.EnableSideEffectsOnInlineProjections = true;
});
```

When the flag is on, `slice.PublishMessage(...)` from an inline projection's
`RaiseSideEffects()` method enqueues the message into the configured `IMessageOutbox`'s
batch on the active document session. `BeforeCommitAsync` fires inside the session's SQL
transaction (right before `COMMIT`), and `AfterCommitAsync` fires once the session's
database changes are durably committed.

::: warning
Inline `RaiseSideEffects()` may **not** call `slice.AppendEvent(...)` — appending events
back into the same session that's currently committing them is not supported. Doing so
throws `InvalidOperationException`. Side effects from inline projections are limited to
published messages.
:::

