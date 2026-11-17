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

## Optional Metadata Columns

Correlation id, causation id, headers and the user name each have a column on `pc_events`. Since
Polecat 6 all four are on by default. Turn one off with `opts.Events.EnableCorrelationId`,
`EnableCausationId`, `EnableHeaders` or `EnableUserName` set to `false`, or all of them with
`opts.RestoreV5Defaults()`.

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
