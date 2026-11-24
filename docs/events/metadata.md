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

Correlation id, causation id, headers and the user name are optional. Each one is off by default, as in Polecat 5
and Marten 10, and you turn on the ones you want. Each has its own column on `pc_events`, which is created whatever
the settings say ([#1116](https://github.com/JasperFx/polecat/issues/1116)), so turning one on later needs no schema
migration; the setting only decides what an append writes and what a read returns.

::: tip Why turn them on
The correlation and causation ids are what tie an event back to the message or request that caused it. With them on,
[CritterWatch](https://critterwatch.jasperfx.net) can filter events by correlation id, causation id and user name in its
event query, and its conversation view follows a Wolverine message to the events its handler appended. OpenTelemetry
traces and your own diagnostics can do the same. Turn on the user name if you want `LastModifiedBy` recorded on every
event, and headers only if you read them yourself.

What they cost: each one adds a bound value to every appended event, so with all four on fewer events fit in one
command. The 6.0 alphas turned all four on by default; Marten measured them as most of what its 10.0 release still
cost on appends (about 11 KB more allocated per append and about 5% slower daemon appends), which is why both stores
keep them opt-in ([#1134](https://github.com/JasperFx/polecat/issues/1134),
[marten#6144](https://github.com/JasperFx/marten/issues/6144)). Polecat's own cost has not been measured separately.
:::

Turn them on one at a time with `opts.Events.EnableCorrelationId`, `EnableCausationId`, `EnableHeaders` and
`EnableUserName`, or all four at once:

<!-- snippet: sample_polecat_enable_all_event_metadata -->
<a id='snippet-sample_polecat_enable_all_event_metadata'></a>
```cs
// Correlation id, causation id, headers and the user name
opts.Events.EnableAllEventMetadata();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/event_metadata_tests.cs#L216-L219' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_enable_all_event_metadata' title='Start of snippet'>anchor</a></sup>
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
