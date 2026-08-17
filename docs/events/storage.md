# Event Storage

Polecat stores events in SQL Server 2025 using three core tables.

## pc_events

The main event log table:

| Column | Type | Description |
| :--- | :--- | :--- |
| `seq_id` | `bigint IDENTITY(1,1)` | Global sequence number (PK) |
| `id` | `uniqueidentifier` | Unique event ID |
| `stream_id` | `uniqueidentifier` or `nvarchar(250)` | Stream identifier |
| `version` | `int` | Position within the stream |
| `data` | `json` | Serialized event body |
| `bdata` | `varbinary(max)` | Binary event body, `NULL` unless the event type opted in — see [Binary Event Serialization](/events/binary-serialization) |
| `type` | `nvarchar(250)` | Event type name (snake_case) |
| `timestamp` | `datetimeoffset` | When the event was recorded |
| `tenant_id` | `nvarchar(250)` | Tenant identifier |
| `dotnet_type` | `nvarchar(500)` | Full .NET type name |
| `correlation_id` | `nvarchar(250)` | Request correlation |
| `causation_id` | `nvarchar(250)` | Event causation chain |
| `headers` | `json` | Custom metadata headers |
| `is_archived` | `bit` | Archive flag |

## pc_streams

Stream metadata:

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | `uniqueidentifier` or `nvarchar(250)` | Stream identifier (PK) |
| `type` | `nvarchar(250)` | Aggregate type name |
| `version` | `int` | Current stream version |
| `timestamp` | `datetimeoffset` | Last event timestamp |
| `created` | `datetimeoffset` | Stream creation time |
| `tenant_id` | `nvarchar(250)` | Tenant identifier |
| `is_archived` | `bit` | Archive flag |

## pc_event_progression

Async daemon progress tracking:

| Column | Type | Description |
| :--- | :--- | :--- |
| `name` | `nvarchar(250)` | Projection/subscription name (PK) |
| `last_seq_id` | `bigint` | Last processed sequence ID |
| `last_updated` | `datetimeoffset` | Last update timestamp |

## Event Type Naming

Polecat converts .NET event type names to snake_case for storage:

- `QuestStarted` → `quest_started`
- `MembersJoined` → `members_joined`
- `InvoiceLineItemAdded` → `invoice_line_item_added`

## Pre-registering Event Types

Event types are registered with the store on the fly the first time they are appended, so registering
them up front is optional. It is worth doing when the *reading* side may meet an event type before the
writing side does -- most commonly the async daemon in a separate process, which has to map an
`event_type_name` back to a .NET type it has not yet seen appended:

```cs
var store = DocumentStore.For(opts =>
{
    opts.Connection(connectionString);

    opts.Events.AddEventType<QuestStarted>();
    opts.Events.AddEventType(typeof(MembersJoined));
    opts.Events.AddEventTypes([typeof(MembersDeparted), typeof(QuestEnded)]);
});
```

This mirrors Marten's `StoreOptions.Events.AddEventType<T>()` / `AddEventTypes(...)`.

## Sequence IDs

Events are assigned a global, monotonically increasing sequence ID via SQL Server's `IDENTITY(1,1)`. This provides a total ordering of all events across all streams, which is critical for the async daemon's event processing.

## JSON Storage

Event data and headers are stored using SQL Server 2025's native `json` type, serialized with System.Text.Json.
