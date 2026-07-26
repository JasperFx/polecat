# Batched Queries

Polecat supports batching multiple queries into a single database round-trip using `IBatchedQuery`.

## Creating a Batch

```cs
var batch = session.CreateBatchQuery();
```

## Batch Operations

### Load by ID

```cs
var userTask = batch.Load<User>(userId);
```

### Load Many

```cs
var usersTask = batch.LoadMany<User>(userId1, userId2, userId3);
```

### LINQ Query

```cs
var ordersTask = batch.Query<Order>()
    .Where(x => x.Status == "Active")
    .ToList();
```

## Executing the Batch

```cs
await batch.Execute();

// Now resolve the results
var user = await userTask;
var users = await usersTask;
var orders = await ordersTask;
```

All queries in the batch execute in a single database call, significantly reducing latency when you need to load multiple independent pieces of data.

## Query Plans

For reusable query specifications, implement `IBatchQueryPlan<T>`:

```cs
public class ActiveOrdersPlan : QueryListPlan<Order>
{
    protected override IQueryable<Order> Query(IQuerySession session)
    {
        return session.Query<Order>().Where(x => x.Status == "Active");
    }
}

// Use in a batch
var ordersTask = batch.QueryByPlan(new ActiveOrdersPlan());
await batch.Execute();
var orders = await ordersTask;
```

Query plans can also be used independently:

```cs
var orders = await session.QueryByPlanAsync(new ActiveOrdersPlan());
```

## Batched Event Store Fetches

`batch.Events` exposes the batched counterparts of `FetchStreamStateAsync` and `FetchStreamAsync`, so a
raw event-stream read can share the batch's single round trip with document loads and LINQ queries:

```cs
var batch = session.CreateBatchQuery();

var stateTask = batch.Events.FetchStreamState(streamId);
var eventsTask = batch.Events.FetchStream(streamId);
var orderTask = batch.Load<Order>(orderId);

await batch.Execute();

var state = await stateTask;    // StreamState?, null when the stream does not exist
var events = await eventsTask;  // IReadOnlyList<IEvent>, empty when the stream does not exist
var order = await orderTask;
```

Both come in `Guid` and `string` overloads for the two stream identity modes, and `FetchStream` carries the
same optional `version` / `timestamp` / `fromVersion` filters as `FetchStreamAsync`:

```cs
// Events up to and including version 5
var capped = batch.Events.FetchStream(streamId, version: 5);

// Everything appended from version 10 onward
var tail = batch.Events.FetchStream(streamId, fromVersion: 10);
```

## Event Stream Query Plans

`FetchStreamStatePlan` and `FetchStreamPlan` wrap those fetches as query plans. Both implement **both**
`IQueryPlan<T>` and `IBatchQueryPlan<T>`, so the same plan instance works standalone or in a batch:

```cs
// Standalone
var state  = await session.QueryByPlanAsync(new FetchStreamStatePlan(streamId));
var events = await session.QueryByPlanAsync(new FetchStreamPlan(streamId, version: 5));

// Batched — one round trip
var batch = session.CreateBatchQuery();
var stateFetcher  = batch.QueryByPlan(new FetchStreamStatePlan(streamId));
var eventsFetcher = batch.QueryByPlan(new FetchStreamPlan(streamId));
await batch.Execute();
```

::: tip
Implementing both interfaces matters beyond convenience. Through Wolverine's fetch-specification feature,
a plan that implements only `IBatchQueryPlan<T>` produces uncompilable generated code — so a custom plan
you intend to route through a handler's `Load` should implement the pair as well.
:::
