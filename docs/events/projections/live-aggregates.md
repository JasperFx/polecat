# Live Aggregations

Live aggregation replays events on demand to build the current aggregate state without persisting a read model.

## Basic Usage

```cs
var order = await session.Events.AggregateStreamAsync<OrderSummary>(streamId);
```

This loads all events for the stream and applies them through the aggregate's `Create` and `Apply` methods.

## When to Use Live Aggregation

Live aggregation is best for:

- **Infrequently accessed aggregates** -- No need to maintain a persistent read model
- **Always-current state** -- No staleness, always reflects the latest events
- **Streams with few events** -- Low replay cost
- **Testing and debugging** -- Verify aggregate behavior

## Version Cap

Replay only up to a specific version:

```cs
var orderAtV5 = await session.Events.AggregateStreamAsync<OrderSummary>(streamId, version: 5);
```

## Timestamp Cap

Replay only events before a specific time:

```cs
var orderAtTime = await session.Events.AggregateStreamAsync<OrderSummary>(
    streamId,
    timestamp: DateTimeOffset.Parse("2024-06-15")
);
```

## UseIdentityMapForAggregates

Polecat offers a performance optimization that caches aggregates in a session-level identity map when using `FetchForWriting()`. When enabled, subsequent calls to `FetchLatest()` within the same session will return the cached instance instead of re-querying the database.

```cs
opts.Projections.UseIdentityMapForAggregates = true;
```

This is particularly valuable in CQRS command handlers that fetch an aggregate for writing, append events, and then need to return the updated aggregate state — avoiding a redundant database round trip.

::: warning
Only use this optimization if you are NOT mutating the aggregate outside of Polecat internals. This is safe with immutable event application patterns (Apply methods that set properties from event data).
:::

::: tip
Unlike Marten, Polecat defaults to lightweight sessions (no identity map tracking). This optimization adds aggregate-specific caching on top of lightweight sessions without switching to full identity map sessions.
:::

## Live vs Inline vs Async

| Strategy | Consistency | Storage | Performance |
| :--- | :--- | :--- | :--- |
| Live | Always current | None | Replay cost per read |
| Inline | Always current | Document table | Write overhead |
| Async | Eventually consistent | Document table | Background processing |
