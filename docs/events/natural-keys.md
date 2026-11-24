# Natural Keys

Natural keys let you look up an event stream by a domain-meaningful identifier (like an order number or invoice code) instead of by its internal stream id. Polecat maintains a separate lookup table that maps natural key values to stream ids, so you can use `FetchForWriting` and `FetchLatest` with your natural key in a single database round-trip, standalone or inside a batched query.

## When to Use Natural Keys

Use natural keys when:

- External systems or users reference aggregates by a business identifier (e.g., `"ORD-12345"`) rather than a `Guid` stream id
- You need to look up streams by a human-readable identifier without maintaining your own separate index
- Your aggregate has a stable "business key" that may occasionally change (natural keys support mutation)

## Declaring Natural Keys

Mark a property on your aggregate with `[NaturalKey]`, and mark the methods that set or change the key value with `[NaturalKeySource]`.
A setup the runtime would refuse, like two natural keys on one aggregate, is a build error (`JFXEVT010` to `JFXEVT017`):
see [Build Diagnostics: natural keys](https://shared-libs.jasperfx.net/codegen/diagnostics#natural-keys).

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

### Key Types

A natural key is an `int`, a `long` or a `string`, or a strong-typed identifier wrapping one of those, like the
`OrderNumber` above. A key can't have the same type as the store's stream identity, so a bare `string` key on a
`StreamIdentity.AsString` store, or a `Guid` key anywhere, is refused when the store is built: a fetch by `string`
on that store is a fetch by stream key, and would never reach the natural key. Wrap the key in a strong-typed
identifier instead. A bare `string` key on a `Guid` store is fine, like `InvoiceAggregate.InvoiceCode` above.

### Nullable Keys <Badge type="tip" text="6.0" />

A struct key can be declared `TKey?` on the aggregate, and a `[NaturalKeySource]` can return `TKey` or `TKey?`.
Polecat treats them as the same key:

<!-- snippet: sample_polecat_nullable_natural_key_aggregate -->
<a id='snippet-sample_polecat_nullable_natural_key_aggregate'></a>
```cs
public readonly record struct NkSku(string Value);

public record NkSkuListed(NkSku Sku, string Name);

public record NkSkuChanged(NkSku? NewSku);

public record NkSkuRestocked(int Quantity);

// The key is a struct, declared nullable on the aggregate, and set by sources returning both NkSku and NkSku?
public partial class NkSkuProduct
{
    public Guid Id { get; set; }

    [NaturalKey]
    public NkSku? Sku { get; set; }

    public string Name { get; set; } = string.Empty;
    public int Stock { get; set; }

    [NaturalKeySource]
    public static NkSku KeyFor(NkSkuListed e) => e.Sku;

    [NaturalKeySource]
    public static NkSku? KeyFor(NkSkuChanged e) => e.NewSku;

    public void Apply(NkSkuListed e)
    {
        Sku = e.Sku;
        Name = e.Name;
    }

    public void Apply(NkSkuChanged e) => Sku = e.NewSku;

    public void Apply(NkSkuRestocked e) => Stock += e.Quantity;
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests.Shared/Events/NaturalKeyTypes.cs#L95-L133' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_nullable_natural_key_aggregate' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

You always fetch by `TKey`, never by `TKey?`. Fetching `FetchLatest<NkSkuProduct, NkSku?>` throws an
`ArgumentException`. You know whether you have a key before you fetch, so handle the null yourself and fetch by the
key's own type.

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
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/natural_key_tests.cs#L61-L71' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_fetch_for_writing_by_natural_key' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

That's one round trip. Polecat resolves the key through the lookup table into the stream's id and version, and reads
the stream's events, all in the same command. An aggregate you've enrolled in the
aggregate write cache (`opts.Events.CacheAggregatesForWriting<T>()`) takes a second round trip, exactly like a fetch by stream id, so that the
events it reads start after the cached aggregate.

A key that no stream has gets you a handle on a new stream, just like a stream id that doesn't exist. Its aggregate is
`null`, it's at version 0, and the stream claims the key with the first event you append that sets it. That's how you
start a stream for a new order number without making one up:

```cs
var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);
if (stream.Aggregate == null)
{
    stream.AppendOne(new NkOrderCreated(orderNumber, "Alice"));
}
```

`FetchForWriting<T, TId>` also takes a strong-typed stream id or the stream id itself, so
`FetchForWriting<OrderAggregate, Guid>(id)` is the same fetch as `FetchForWriting<OrderAggregate>(id)`.

### Expected Version

Pass the version you expect the stream to be at, and Polecat refuses a stale one with a `ConcurrencyException`
(`EventStreamUnexpectedMaxEventIdException`) before it touches your session. No stream action, write cache claim or
identity map entry is left behind, so the same session can carry on:

<!-- snippet: sample_polecat_fetch_for_writing_by_natural_key_with_expected_version -->
<a id='snippet-sample_polecat_fetch_for_writing_by_natural_key_with_expected_version'></a>
```cs
var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, 3);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/natural_key_fetch_tests.cs#L62-L66' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_fetch_for_writing_by_natural_key_with_expected_version' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A key no stream has is at version 0, so an expected version of 0 starts the new stream and anything else is refused.

### Exclusive Writes

`FetchForExclusiveWriting<T, TKey>` opens the session's transaction and locks the stream row until
`SaveChangesAsync()` commits or the session is disposed, just like the stream id form. A second exclusive fetch of the
same key waits for the lock. A lock that can't be had in time is a `StreamLockedException`.

The key is resolved before the lock is taken, so Polecat resolves it once more under the lock. If the key moved to
another stream in between, the fetch is done again.

## FetchLatest by Natural Key

For read-only access, you can use `FetchLatest` with a natural key:

<!-- snippet: sample_polecat_fetch_latest_by_natural_key -->
<a id='snippet-sample_polecat_fetch_latest_by_natural_key'></a>
```cs
// Read-only access by natural key
var aggregate = await session2.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber, TestContext.Current.CancellationToken);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/natural_key_tests.cs#L89-L92' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_fetch_latest_by_natural_key' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

That's one round trip as well. An `Inline` aggregate is read from its stored document, and any other is built from its
events, as `FetchLatest` by stream id does. A key no stream has returns `null`, and so does a `null` key, without a
query.

With `opts.Projections.UseIdentityMapForAggregates` on, `FetchLatest` by key returns the aggregate your session already
holds when its current key matches, without a query, just as `FetchLatest` by stream id does.

### Batch Queries <Badge type="tip" text="6.0" />

Every fetch by natural key is one command, so it can go in a [batched query](/documents/querying/batched-queries)
with anything else:

<!-- snippet: sample_polecat_batch_fetch_by_natural_key -->
<a id='snippet-sample_polecat_batch_fetch_by_natural_key'></a>
```cs
var batch = session.CreateBatchQuery();

var summary = batch.Events.FetchLatest<NkOrderSummary, OrderNumber>(order);
var owner = batch.Events.FetchLatest<OrderAggregate, OrderNumber>(order);
var byStreamId = batch.Events.FetchLatest<OrderAggregate>(orderId);
var product = batch.Events.FetchLatest<NkSkuProduct, NkSku>(sku);
var missing = batch.Events.FetchLatest<NkSkuProduct, NkSku>(new NkSku("SKU-NONE"));

// One round trip for all five
await batch.Execute();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/batch_querying_by_natural_key.cs#L66-L79' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_batch_fetch_by_natural_key' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`batch.Events` has `FetchLatest`, `FetchForWriting` (with or without an expected version) and
`FetchForExclusiveWriting`, by stream id and by `<T, TId>`. The writing forms need a document session. An exclusive
fetch in a batch opens the session's transaction before the batch runs. A batch can't redo itself, so a stale
version, or a key that moved before its stream was locked, fails the whole batch.

### FetchManyForWriting by Natural Key

`FetchManyForWriting<T, TKey>` fetches several streams for writing by key. Polecat resolves every key in one round
trip, then reads the streams as the stream id form does. A key no stream has gets a new-stream handle in its place:

<!-- snippet: sample_polecat_fetch_many_for_writing_by_natural_key -->
<a id='snippet-sample_polecat_fetch_many_for_writing_by_natural_key'></a>
```cs
var streams = await session.Events.FetchManyForWriting<OrderAggregate, OrderNumber>([first, second, unknown]);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/natural_key_fetch_tests.cs#L218-L222' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_fetch_many_for_writing_by_natural_key' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

It refuses `TKey?`, a `null` key, and the same key twice.

## Mapped Natural Keys <Badge type="tip" text="6.0" />

When there are several aggregates over the same stream, they can usually all be found by the same natural key.
Rather than declaring it on each of them, declare it once on the aggregate that owns it, and _map_ it on the others
with `[MappedNaturalKey<TSource>]`, or `[NaturalKey(SourceProjection = typeof(TSource))]` if you're in F#. `TSource`
is the aggregate that owns the key, or its projection class:

<!-- snippet: sample_polecat_mapped_natural_key_aggregate -->
<a id='snippet-sample_polecat_mapped_natural_key_aggregate'></a>
```cs
// An async read model of an order stream. It owns no lookup: the order number resolves through
// OrderAggregate's pc_natural_key_orderaggregate table, which OrderAggregate's projection keeps inline.
public partial class NkOrderSummary
{
    public Guid Id { get; set; }

    [MappedNaturalKey<OrderAggregate>]
    public OrderNumber Number { get; set; } = null!;

    public int Items { get; set; }

    public static NkOrderSummary Create(NkOrderCreated e) => new() { Number = e.OrderNumber };

    public void Apply(NkOrderItemAdded e) => Items++;

    public void Apply(NkOrderNumberChanged e) => Number = e.NewOrderNumber;
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests.Shared/Events/NaturalKeyTypes.cs#L73-L93' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_mapped_natural_key_aggregate' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A mapped key has no lookup table of its own. It reads the table of the aggregate that owns the key, so register the
owner too:

<!-- snippet: sample_polecat_registering_mapped_natural_key_projections -->
<a id='snippet-sample_polecat_registering_mapped_natural_key_projections'></a>
```cs
// The owner maintains the order number lookup inline...
opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);

// ...and the async view is fetched through it
opts.Projections.Snapshot<NkOrderSummary>(SnapshotLifecycle.Async);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/mapped_natural_key_tests.cs#L22-L30' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_registering_mapped_natural_key_projections' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This is how you fetch an `Async` read model by its natural key. The owner keeps the lookup current inline, so the read
model can be fetched by key the moment its events commit, before the daemon has caught up, and Polecat answers with
the read model's current state either way:

<!-- snippet: sample_polecat_fetch_async_aggregate_by_mapped_natural_key -->
<a id='snippet-sample_polecat_fetch_async_aggregate_by_mapped_natural_key'></a>
```cs
// Current state, whether or not the daemon has caught up yet
var latest = await session.Events.FetchLatest<NkOrderSummary, OrderNumber>(orderNumber);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Events/mapped_natural_key_tests.cs#L60-L65' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_fetch_async_aggregate_by_mapped_natural_key' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`FetchForWriting`, `FetchForExclusiveWriting`, `FetchManyForWriting` and the batch forms all work by a mapped key too.
When the owner's key changes, the mapped key follows it.

An aggregate can own at most one key and map any number, as long as every key has a distinct type. When the
`DocumentStore` is built, Polecat checks that each mapped key's source is registered as a projection, owns a natural
key, and owns a key of the same type, where `TKey` and `TKey?` count as the same. A mapped key can't name its own
aggregate, or read another mapped key. Anything wrong is an `InvalidProjectionException` that says what to do about
it.

An aggregate in a [composite projection](/events/projections/composite) can't own a natural key, but it can map one
whose owner is registered outside the composite.

The [parked identities](/events/projections/async-daemon#parking-a-failing-identity) of an `Async` read model show its
owned key, or, when it owns none, its first mapped key.

## Registering on First Fetch

An aggregate that owns a natural key doesn't have to be registered. The first fetch by its key registers an `Inline`
projection for it, which keeps the lookup from then on. That's only for a key the aggregate owns: an aggregate with only
mapped keys has no lookup of its own to keep, so register it yourself.

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

`FetchForWriting` and `FetchForExclusiveWriting` refuse a null key with an `ArgumentOutOfRangeException`, the way they
refuse an empty `Guid` or a `null` stream key, because a null key can't match a stream, and there's no key for a new
stream to claim. That includes a strong-typed key whose value is `null`, which is what `default` of a struct wrapping a
string is. Check for a missing key before fetching. `FetchLatest` by a null key returns `null` without a query.

## Clean and Maintenance Operations

The natural key lookup table is maintained automatically as part of normal event appending. If you need to rebuild the lookup table (for example, after a data migration), you can do so through Polecat's schema management tools as part of a projection rebuild.

A rebuild works from the events that are left. [Compacting a stream](/events/archiving#compacting-a-stream) deletes the events that carried its key, so for a compacted stream the key is read from the aggregate in the compaction marker instead, which needs the natural key member to survive serialization of that aggregate.

## Testing Considerations

When writing integration tests:

- Natural key lookups read the lookup table, which is written when the session commits. A key appended in the same
  session isn't found until `SaveChangesAsync()`: `FetchLatest` returns `null`, and `FetchForWriting` hands you a
  new stream rather than the one you just started. Commit the events that set the key before you look the stream up
  by it.
- `FetchForWriting` and `FetchForExclusiveWriting` by a natural key that no stream has start a new stream, like a
  fetch by a stream id that does not exist.

## Integration with Wolverine

Natural keys integrate with Wolverine's aggregate handler workflow, for writes and reads, owned and mapped keys alike,
including the batched reads above. See the [Wolverine documentation on natural keys with Polecat](https://wolverinefx.io/guide/durability/polecat/event-sourcing#natural-keys) for details on how Wolverine resolves natural keys from command properties.
