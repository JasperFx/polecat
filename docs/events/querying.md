# Querying Events

Polecat provides several ways to read events and aggregate state from streams.

## FetchStreamAsync

Load all events for a stream:

```cs
var events = await session.Events.FetchStreamAsync(streamId);

foreach (var @event in events)
{
    Console.WriteLine($"[{@event.Version}] {@event.EventTypeName}: {@event.Data}");
}
```

Events are returned in version order. Archived streams are automatically excluded.

## Stream Fetches as Query Plans

`FetchStreamStatePlan` and `FetchStreamPlan` wrap the two raw stream fetches as reusable query plans.
Both implement `IQueryPlan<T>` **and** `IBatchQueryPlan<T>`, so the same plan works standalone or inside
a batched query, and both offer `Guid streamId` / `string streamKey` constructor overloads:

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

`FetchStreamStatePlan` yields `null` when the stream does not exist; `FetchStreamPlan` yields an empty
list and carries `FetchStream`'s optional `version` / `timestamp` / `fromVersion` arguments.

The underlying batched fetchers are also available directly as `batch.Events.FetchStreamState(...)` and
`batch.Events.FetchStream(...)` — see [Batched Queries](/documents/querying/batched-queries#batched-event-store-fetches).

To return either straight from an ASP.NET Core endpoint, see the `StreamEventState` and `StreamEvents`
result types in [ASP.NET Core Integration](/documents/aspnetcore#streaming-event-stream-metadata-and-events).

## AggregateStreamAsync

Replay events to build the current aggregate state:

```cs
var party = await session.Events.AggregateStreamAsync<QuestParty>(streamId);
```

Polecat replays all events in the stream through the aggregate's `Apply`/`Create` methods to build the current state.

### With Version Cap

Replay only up to a specific version:

```cs
var partyAtV3 = await session.Events.AggregateStreamAsync<QuestParty>(streamId, version: 3);
```

### With Timestamp Cap

Replay only events before a specific timestamp:

```cs
var partyAtTime = await session.Events.AggregateStreamAsync<QuestParty>(streamId,
    timestamp: DateTimeOffset.Parse("2024-01-15"));
```

## FetchForWriting

Load an aggregate with its current version for optimistic concurrency:

```cs
var stream = await session.Events.FetchForWriting<QuestParty>(streamId);

Console.WriteLine(stream.Aggregate.Name);      // Current state
Console.WriteLine(stream.CurrentVersion);       // Current version

stream.AppendOne(new MembersDeparted(...));
await session.SaveChangesAsync();
```

## FetchForExclusiveWriting

Load with a pessimistic lock (SQL Server `UPDLOCK HOLDLOCK`):

```cs
var stream = await session.Events.FetchForExclusiveWriting<QuestParty>(streamId);
// Row is locked until transaction completes
```

## IEvent Interface

Each event returned from `FetchStreamAsync` implements `IEvent`:

| Property | Type | Description |
| :--- | :--- | :--- |
| `Id` | `Guid` | Unique event ID |
| `Sequence` | `long` | Global sequence number |
| `Version` | `int` | Position within the stream |
| `Data` | `object` | Deserialized event body |
| `EventTypeName` | `string` | Event type name (snake_case) |
| `Timestamp` | `DateTimeOffset` | When recorded |
| `StreamId` / `StreamKey` | `Guid` / `string` | Stream identifier |
| `TenantId` | `string` | Tenant identifier |
| `CorrelationId` | `string?` | Correlation ID |
| `CausationId` | `string?` | Causation ID |
| `Headers` | `Dictionary` | Custom headers |

## Querying Directly Against Event Data

### QueryRawEventDataOnly

You can issue LINQ queries against a specific event type's data. This searches the entire `pc_events` table filtered by event type, so it is primarily intended for diagnostics and troubleshooting:

```cs
// Query all MembersJoined events
var joinedEvents = await session.Events.QueryRawEventDataOnly<MembersJoined>()
    .ToListAsync();

// Count events of a specific type
var count = await session.Events.QueryRawEventDataOnly<MembersJoined>()
    .CountAsync();

// Filter by event data properties
var events = await session.Events.QueryRawEventDataOnly<MembersJoined>()
    .Where(x => x.Day == 1)
    .ToListAsync();

// Check if any events exist
var any = await session.Events.QueryRawEventDataOnly<MembersJoined>()
    .AnyAsync();
```

### QueryAllRawEvents

Query across all event types using the `IEvent` metadata properties:

```cs
// Query all events for a specific stream
var events = await session.Events.QueryAllRawEvents()
    .Where(x => x.StreamId == streamId)
    .OrderBy(x => x.Sequence)
    .ToListAsync();

// Filter by event metadata
var recentEvents = await session.Events.QueryAllRawEvents()
    .Where(x => x.Timestamp > cutoffDate)
    .ToListAsync();

// Filter by event type name
var joinedTypeName = store.Options.EventGraph
    .EventMappingFor(typeof(MembersJoined)).EventTypeName;
var events = await session.Events.QueryAllRawEvents()
    .Where(x => x.EventTypeName == joinedTypeName)
    .ToListAsync();

// Count events matching a condition
var count = await session.Events.QueryAllRawEvents()
    .CountAsync(x => x.Version == 1);

// Select specific metadata columns
var streamIds = await session.Events.QueryAllRawEvents()
    .Select(x => x.StreamId)
    .Distinct()
    .ToListAsync();
```

The queryable `IEvent` properties available for filtering and projection are:

| Property | SQL Column | Description |
| :--- | :--- | :--- |
| `Id` | `id` | Unique event ID |
| `Sequence` | `seq_id` | Global sequence number |
| `StreamId` | `stream_id` | Stream identifier (Guid) |
| `Version` | `version` | Position within the stream |
| `Timestamp` | `timestamp` | When recorded |
| `EventTypeName` | `type` | Event type name |
| `DotNetTypeName` | `dotnet_type` | .NET type name |
| `IsArchived` | `is_archived` | Archive flag |
| `TenantId` | `tenant_id` | Tenant identifier |
| `CorrelationId` | `correlation_id` | Correlation ID |
| `CausationId` | `causation_id` | Causation ID |

::: warning
These queries search the entire event table and should be used judiciously. For routine application queries, prefer projected views or tag-based queries.
:::

### AggregateToAsync

`AggregateToAsync<T>()` is a LINQ terminal that folds **every** event matched by an event query into a single live aggregate of type `T`, regardless of which stream each event came from. It uses the same conventional `Create`/`Apply` aggregation that `AggregateStreamAsync` uses, but over an arbitrary event query instead of one stream:

<!-- snippet: sample_aggregate_to_async -->
<a id='snippet-sample_aggregate_to_async'></a>
```cs
var questParty = await session.Events
    .QueryAllRawEvents()

    // You could of course chain all the Linq
    // Where()/OrderBy()/Take()/Skip() operators
    // you need here

    .AggregateToAsync<QuestParty>(token: TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Projections/aggregateto_linq_operator_tests.cs#L57-L68' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_aggregate_to_async' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

You can also seed the fold with initial state:

```cs
var initial = new QuestParty { Members = ["Lan"] };

var questParty = await session.Events.QueryAllRawEvents()
    .Where(x => x.StreamId == streamId)
    .AggregateToAsync(initial);
```

The aggregate's identity is stamped from the last queried event's stream (`StreamId` or `StreamKey` depending on the store's `StreamIdentity`), and `null` is returned when the query matches no events.

### AggregateToManyAsync

`AggregateToManyAsync<T>()` is a LINQ terminal that runs the events matched by an event query through the **multi-stream projection** registered for `T` and returns the aggregate it produces for each resulting identity. It drives the projection's real slicer/grouper, `EnrichEventsAsync`, and per-slice build against the live query session — the same building blocks the projection step-through and the async daemon use — so custom groupers that read present-day reference data from the session work for free. Nothing is persisted.

Given a multi-stream projection:

<!-- snippet: sample_aggregate_to_many_projection -->
<a id='snippet-sample_aggregate_to_many_projection'></a>
```cs
public record MoneyDeposited(Guid AccountId, int Amount);
public record AccountFrozen(Guid AccountId);

public class Balance
{
    public Guid Id { get; set; }
    public int Amount { get; set; }
}

public partial class BalanceProjection : MultiStreamProjection<Balance, Guid>
{
    public BalanceProjection()
    {
        Identity<MoneyDeposited>(e => e.AccountId);
        Identity<AccountFrozen>(e => e.AccountId);
    }

    public void Apply(MoneyDeposited e, Balance b) => b.Amount += e.Amount;

    public bool ShouldDelete(AccountFrozen e) => true;
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Projections/aggregate_to_many_tests.cs#L11-L35' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_aggregate_to_many_projection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

query any slice of the event store and fan it out to one aggregate per identity:

<!-- snippet: sample_aggregate_to_many -->
<a id='snippet-sample_aggregate_to_many'></a>
```cs
var aggregates = await session.Events.QueryAllRawEvents()
    .Where(e => e.StreamId == stream1 || e.StreamId == stream2)
    .AggregateToManyAsync<Balance>(TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Projections/aggregate_to_many_tests.cs#L115-L121' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_aggregate_to_many' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Each returned aggregate has its identity stamped from the projection's slice id. Aggregates whose event slice resolves to a delete (`ShouldDelete`) are omitted, an empty query returns an empty list, and calling it for an aggregate type with no registered projection throws `ArgumentException`.

Contrast `AggregateToAsync`, which folds every queried event into a single aggregate; `AggregateToManyAsync` fans them out through the projection's slicer to one aggregate per identity.

## QueryForNonStaleData

Wait for async projections to catch up before querying:

```cs
var orders = await session.Query<OrderSummary>()
    .QueryForNonStaleData()
    .Where(x => x.Status == "Active")
    .ToListAsync();
```

With a custom timeout:

```cs
var orders = await session.Query<OrderSummary>()
    .QueryForNonStaleData(TimeSpan.FromSeconds(10))
    .Where(x => x.Status == "Active")
    .ToListAsync();
```
