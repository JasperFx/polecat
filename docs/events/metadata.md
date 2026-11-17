# Event Metadata

Polecat stores rich metadata alongside each event.

## Built-in Metadata

Every event automatically includes:

| Field | Description |
| :--- | :--- |
| `Id` | Unique event identifier (Guid) |
| `Sequence` | Global sequence number (auto-incremented) |
| `Version` | Position within the stream |
| `Timestamp` | When the event was recorded |
| `EventTypeName` | Snake_case type name |
| `DotNetType` | Full .NET type name |

## Turning on Optional Metadata

Correlation id, causation id, headers and the user name are optional and each has its own column on
`pc_events`. Turn them on one at a time with `opts.Events.EnableCorrelationId`, `EnableCausationId`,
`EnableHeaders` and `EnableUserName`, or all four at once:

<!-- snippet: sample_polecat_enable_all_event_metadata -->
<a id='snippet-sample_polecat_enable_all_event_metadata'></a>
```cs
// Correlation id, causation id, headers and the user name
opts.Events.EnableAllEventMetadata();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Events/event_metadata_tests.cs#L216-L219' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_enable_all_event_metadata' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`EnableAllEventMetadata()` includes the user name, so the session's `LastModifiedBy` is stored on every
appended event. It's the same as Marten's `Events.MetadataConfig.EnableAll()`.

## Correlation and Causation

Track request flow and event chains:

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    CorrelationId = "http-request-abc",
    CausationId = "command-create-order"
});

session.Events.Append(streamId, new OrderCreated(...));
await session.SaveChangesAsync();
```

These values are stored in the `correlation_id` and `causation_id` columns on `pc_events`.

## Custom Headers

Attach arbitrary key-value metadata to events:

```cs
var action = session.Events.Append(streamId, new OrderCreated(...));
action.Headers = new Dictionary<string, object>
{
    ["user_agent"] = "MyApp/1.0",
    ["ip_address"] = "192.168.1.1"
};

await session.SaveChangesAsync();
```

Headers are stored as JSON in the `headers` column.

## Tenant ID

In multi-tenant configurations, the tenant ID is automatically recorded:

```cs
await using var session = store.LightweightSession(new SessionOptions
{
    TenantId = "tenant-abc"
});

session.Events.Append(streamId, new OrderCreated(...));
// Event is stored with tenant_id = "tenant-abc"
```

## Accessing Metadata

When loading events, all metadata is available on the `IEvent` wrapper:

```cs
var events = await session.Events.FetchStreamAsync(streamId);

foreach (var @event in events)
{
    Console.WriteLine($"Seq: {@event.Sequence}");
    Console.WriteLine($"Version: {@event.Version}");
    Console.WriteLine($"Time: {@event.Timestamp}");
    Console.WriteLine($"Correlation: {@event.CorrelationId}");
    Console.WriteLine($"Causation: {@event.CausationId}");
    Console.WriteLine($"Tenant: {@event.TenantId}");
    Console.WriteLine($"Headers: {JsonSerializer.Serialize(@event.Headers)}");
}
```

## Using Metadata in Projections

Projections can access event metadata through the `IEvent<T>` wrapper:

```cs
public class OrderSummary
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public string? CreatedBy { get; set; }

    public void Apply(IEvent<OrderCreated> @event)
    {
        CreatedAt = @event.Timestamp;
        CreatedBy = @event.Headers?["user"]?.ToString();
    }
}
```
