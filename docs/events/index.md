# Polecat as Event Store

Polecat provides a full-featured event store built on SQL Server 2025, following the same patterns as Marten's PostgreSQL-based event store.

## Key Concepts

- **Event** -- An immutable record of something that happened in your domain
- **Stream** -- A sequence of events related to a specific aggregate or entity
- **Aggregate** -- A domain object whose state is derived by replaying events
- **Projection** -- A read model built from events, either inline, live, or asynchronously

## Event Store Tables

Polecat uses three core tables (all prefixed with `pc_`):

| Table | Purpose |
| :--- | :--- |
| `pc_events` | All events with sequence IDs, stream references, JSON data |
| `pc_streams` | Stream metadata: version, type, timestamps |
| `pc_event_progression` | Async daemon progress tracking per projection |

## Stream Identity

Streams can be identified by either `Guid` or `string`:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection("...");

    // Default: Guid stream IDs
    opts.Events.StreamIdentity = StreamIdentity.AsGuid;

    // Alternative: String stream IDs
    opts.Events.StreamIdentity = StreamIdentity.AsString;
});
```

## Quick Example

```cs
// Define events
public record InvoiceCreated(decimal Amount, string Customer);
public record InvoicePaid(decimal AmountPaid, DateTimeOffset PaidAt);

// Append events
await using var session = store.LightweightSession();
var streamId = session.Events.StartStream<Invoice>(
    new InvoiceCreated(100m, "Acme Corp"),
    new InvoicePaid(100m, DateTimeOffset.UtcNow)
);
await session.SaveChangesAsync();

// Replay to aggregate
var invoice = await session.Events.AggregateStreamAsync<Invoice>(streamId);
```

See the [Quick Start](/events/quickstart) for a complete walkthrough.

## Projection Strategies

| Strategy | When Applied | Use Case |
| :--- | :--- | :--- |
| [Inline](/events/projections/inline) | Same transaction as event append | Strong consistency requirements |
| [Live](/events/projections/live-aggregates) | On-demand replay | Occasional reads, always current |
| [Async](/events/projections/async-daemon) | Background daemon | Eventually consistent read models |

## Event Appending

Polecat uses **QuickAppend** -- direct SQL `INSERT` statements with an `UPDATE...OUTPUT` pattern for version management. No stored procedures are used.

See [Appending Events](/events/appending) for details.
