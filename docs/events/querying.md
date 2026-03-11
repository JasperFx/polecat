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

## AggregateStreamAsync

Replay events to build the current aggregate state:

```cs
var party = await session.Events.AggregateStreamAsync<QuestParty>(streamId);
```

If snapshots are enabled, Polecat loads the latest snapshot and only replays events after the snapshot version.

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
