# Natural Keys

Natural keys let you look up an event stream by a domain-meaningful identifier (like an order number or invoice code) instead of by its internal stream id. Polecat maintains a separate lookup table that maps natural key values to stream ids, so you can use `FetchForWriting` and `FetchLatest` with your natural key in a single database round-trip.

## When to Use Natural Keys

Use natural keys when:

- External systems or users reference aggregates by a business identifier (e.g., `"ORD-12345"`) rather than a `Guid` stream id
- You need to look up streams by a human-readable identifier without maintaining your own separate index
- Your aggregate has a stable "business key" that may occasionally change (natural keys support mutation)

## Declaring Natural Keys

Mark a property on your aggregate with `[NaturalKey]`, and mark the methods that set or change the key value with `[NaturalKeySource]`:

<!-- snippet: sample_polecat_natural_key_aggregate_types -->
<a id='snippet-sample_polecat_natural_key_aggregate_types'></a>
```cs
public record OrderNumber(string Value);

public partial class OrderAggregate
{
    public Guid Id { get; set; }

    [NaturalKey]
    public OrderNumber OrderNum { get; set; } = null!;

    public decimal TotalAmount { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public bool IsComplete { get; set; }

    [NaturalKeySource]
    public void Apply(NkOrderCreated e)
    {
        OrderNum = e.OrderNumber;
        CustomerName = e.CustomerName;
    }

    public void Apply(NkOrderItemAdded e)
    {
        TotalAmount += e.Price;
    }

    [NaturalKeySource]
    public void Apply(NkOrderNumberChanged e)
    {
        OrderNum = e.NewOrderNumber;
    }

    public void Apply(NkOrderCompleted e)
    {
        IsComplete = true;
    }
}

public partial class InvoiceAggregate
{
    public Guid Id { get; set; }

    [NaturalKey]
    public string InvoiceCode { get; set; } = string.Empty;

    public decimal Amount { get; set; }

    [NaturalKeySource]
    public void Apply(NkInvoiceCreated e)
    {
        InvoiceCode = e.Code;
        Amount = e.Amount;
    }
}

public record NkOrderCreated(OrderNumber OrderNumber, string CustomerName);
public record NkOrderItemAdded(string ItemName, decimal Price);
public record NkOrderNumberChanged(OrderNumber NewOrderNumber);
public record NkOrderCompleted;
public record NkInvoiceCreated(string Code, decimal Amount);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests.Shared/Events/NaturalKeyTypes.cs#L9-L71' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_natural_key_aggregate_types' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The `[NaturalKeySource]` attribute tells Polecat which `Create` / `Apply` methods produce or change the natural key value. Polecat uses this information to keep the lookup table in sync whenever events are appended.

## Event-to-Key Mappings

Every event type that sets or changes the natural key must be declared through the `[NaturalKeySource]` attribute. When Polecat processes events during an append operation, it extracts the key value from these mapped events and writes it to the lookup table.

Events that do not affect the natural key (like `NkOrderItemAdded` in the example above) do not need any mapping.

A `[NaturalKeySource]` method that cannot be bound to an extraction strategy is now a configuration-time
error — `AssembleAndAssertValidity()` throws an `InvalidProjectionException` naming the method and the
reason. Previously such a method registered nothing at all, silently, and the lookup table was simply
never written for that event type.

## Explicit Mappings with `NaturalKeyFor()`

When attribute discovery cannot bind your method — or when you would simply rather be explicit — register
the mapping directly from the projection:

```cs
public class OrderProjection: SingleStreamProjection<Order, Guid>
{
    public OrderProjection()
    {
        NaturalKeyFor(x => x
            // From the event body
            .SetBy<ProductRegistered>(e => new ProductCode(e.Code))
            // From the whole IEvent, when the key depends on event metadata
            .SetByEvent<ProductCodeChanged>(e => new ProductCode(e.Data.NewCode)));
    }
}
```

An explicit registration replaces any discovered mapping for the same event type, so it always wins over
the attribute.

::: warning
The natural key has to be a function of **the event alone**. The lookup table is maintained inline at
append time, where no prior aggregate exists under an `Async` snapshot lifecycle, so an extraction that
depends on the current aggregate state has nothing to read. This is the constraint the attribute path now
enforces as well.
:::

## Storage

Polecat automatically creates and manages a lookup table (prefixed with `pc_`) for each aggregate type that has a natural key configured. The table maps natural key values to stream ids and is:

- Created automatically during schema migrations
- Updated transactionally alongside event appends using `MERGE` statements
- Archive-aware via an `is_archived` bit column (archived streams are excluded from lookups)

You do not need to create or manage this table yourself.

::: info
Unlike Marten's PostgreSQL-based implementation which uses table partitioning for multi-tenancy, Polecat's SQL Server implementation uses an `is_archived` bit column and does not use partitioning for the natural key lookup table.
:::

## FetchForWriting by Natural Key

The primary use case for natural keys is looking up a stream for writing without knowing its stream id:

<!-- snippet: sample_polecat_fetch_for_writing_by_natural_key -->
<a id='snippet-sample_polecat_fetch_for_writing_by_natural_key'></a>
```cs
// FetchForWriting by the business identifier instead of stream id
var stream = await session2.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber, TestContext.Current.CancellationToken);

stream.Aggregate.ShouldNotBeNull();
stream.Aggregate!.OrderNum.ShouldBe(orderNumber);

// Append new events through the stream
stream.AppendOne(new NkOrderItemAdded("Gadget", 19.99m));
await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/natural_key_tests.cs#L49-L59' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_fetch_for_writing_by_natural_key' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This resolves the natural key to a stream id and fetches the aggregate in a single database round-trip. Polecat uses `WITH (UPDLOCK, HOLDLOCK)` hints to ensure safe concurrent access during the lookup.

## FetchLatest by Natural Key

For read-only access, you can use `FetchLatest` with a natural key:

<!-- snippet: sample_polecat_fetch_latest_by_natural_key -->
<a id='snippet-sample_polecat_fetch_latest_by_natural_key'></a>
```cs
// Read-only access by natural key
var aggregate = await session2.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber, TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/natural_key_tests.cs#L77-L80' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_fetch_latest_by_natural_key' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Mutability

Natural keys can change over the lifetime of a stream. When an event mapped with `[NaturalKeySource]` is appended, Polecat updates the lookup table with the new value using a `MERGE` statement. The old key value is replaced, so lookups using the previous key will no longer resolve to that stream.

## Null and Default Keys

What a `null` key value means depends on the source that produced it.

- An **explicit** source can remove a stream's key on purpose: a `[NaturalKeySource]` static method that returns the
  key type, or a `NaturalKeyFor()` registration. When it returns `null`, the lookup row is deleted, the stream can no
  longer be found by its old key, and that key is free for another stream to claim. That includes a strong-typed key
  whose wrapped value is null.
- A source Polecat **infers**, by reading a property of the key's type off the event or by running your `Apply`
  method, reads a `null` as "this event carries no key". The stream keeps its key. So an `Apply` that only sets the
  key when its event carries one does what it says.

A stream that has never been given a key has no lookup row, so it can't be found by natural key, but it can still be
fetched by its stream id.

<!-- snippet: sample_polecat_natural_key_sources_and_null -->
<a id='snippet-sample_polecat_natural_key_sources_and_null'></a>
```cs
public record ShipmentCode(string Value);

public record ShipmentBooked(ShipmentCode Code);

public record ShipmentUpdated(ShipmentCode? NewCode, string Notes);

public record ShipmentCodeWithdrawn;

public class Shipment
{
    public Guid Id { get; set; }

    [NaturalKey]
    public ShipmentCode? Code { get; set; }

    public string Notes { get; set; } = string.Empty;

    [NaturalKeySource]
    public void Apply(ShipmentBooked e) => Code = e.Code;

    // Polecat reads the key off the event here, so a ShipmentUpdated whose NewCode is null means
    // "this event carries no key", and the stream keeps its code
    [NaturalKeySource]
    public void Apply(ShipmentUpdated e)
    {
        if (e.NewCode != null) Code = e.NewCode;
        Notes = e.Notes;
    }

    // A static source that returns the key type is explicit, so its null removes the stream's key
    [NaturalKeySource]
    public static ShipmentCode? KeyFor(ShipmentCodeWithdrawn e) => null;

    public void Apply(ShipmentCodeWithdrawn e) => Code = null;
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/natural_key_null_sources.cs#L13-L51' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_natural_key_sources_and_null' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The same rule applies when a projection rebuild repopulates the lookup table, since a rebuild replays the same events.

In Polecat 5 a `null` key value was always ignored, so there was no way to remove a stream's key. Polecat 6 lets an
explicit source do that, and keeps ignoring a `null` from a source it infers, so a conditional `Apply` can't free a key
by accident (jasperfx#1287).

An `int` or `long` key of 0 is a key like any other: Polecat writes it to the lookup table.

`FetchForWriting` and `FetchForExclusiveWriting` refuse a null key with an `ArgumentNullException`, because a null key
can't match a stream. Check for a missing key before fetching. `FetchLatest` by a null key returns `null`.

## Clean and Maintenance Operations

The natural key lookup table is maintained automatically as part of normal event appending. If you need to rebuild the lookup table (for example, after a data migration), you can do so through Polecat's schema management tools as part of a projection rebuild.

A rebuild works from the events that are left. [Compacting a stream](/events/archiving#compacting-a-stream) deletes the events that carried its key, so for a compacted stream the key is read from the aggregate in the compaction marker instead, which needs the natural key member to survive serialization of that aggregate.

## Testing Considerations

When writing integration tests:

- Natural key lookups read the lookup table, which is written when the session commits. A key appended in the same
  session isn't found until `SaveChangesAsync()`: `FetchLatest` returns `null` and `FetchForWriting` throws. Commit
  the events that set the key before you look the stream up by it.
- `FetchForWriting` and `FetchForExclusiveWriting` by a natural key that no stream has throw an
  `InvalidOperationException`, unlike a fetch by a stream id that does not exist, which starts a new stream. To start a
  stream for a new key, use `StartStream`.

## Integration with Wolverine

Natural keys integrate with Wolverine's aggregate handler workflow. See the [Wolverine documentation on natural keys with Polecat](https://wolverinefx.io/guide/durability/polecat/event-sourcing#natural-keys) for details on how Wolverine resolves natural keys from command properties.
