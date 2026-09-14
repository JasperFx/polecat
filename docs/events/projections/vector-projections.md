# Vector Projections

Polecat could already *search* vectors and fuse that with keyword search. Nothing produced the
embeddings — you had to write them yourself, from outside the event store, and keep them in step by
hand.

A `VectorProjection<TDoc, TId>` closes that: declare which events carry text worth embedding, and the
projection calls your embedding model and writes an ordinary document.

## A worked example

```csharp
public record PageWritten(string PageId, string Text);
public record PageRemoved(string PageId);

// The document the projection writes. IVectorized<TId> is what supplies the three members the
// projection maintains.
public class PageVector : IVectorized<string>
{
    public string Id { get; set; } = null!;
    public string? Content { get; set; }
    public string? ContentHash { get; set; }
    public float[]? Embedding { get; set; }
}

public class PageVectorProjection(IEmbeddingProvider provider)
    : VectorProjection<PageVector, string>(provider)
{
    protected override void Configure(VectorProjectionMap<string> map)
    {
        map.Map<PageWritten>(e => e.Data.Text, e => e.Data.PageId);
        map.Delete<PageRemoved>(e => e.Data.PageId);
    }
}
```

Register it like any other projection, and declare the vector index on the document it writes:

```csharp
var store = DocumentStore.For(opts =>
{
    opts.Connection(connectionString);

    // Asynchronous only — see below. Registering it Inline is refused at store construction.
    opts.Projections.Add(new PageVectorProjection(myEmbeddingProvider), ProjectionLifecycle.Async);

    opts.Schema.For<PageVector>().VectorIndex(x => x.Embedding, dimensions: 1536);
});
```

That is all. Searching it is ordinary [vector search](/documents/querying/vector-search):

```csharp
var nearest = await session.VectorSearchAsync<PageVector>(
    x => x.Embedding, queryVector, limit: 10);
```

## It writes an ordinary document

That is the design decision that makes everything else follow, rather than a shortcut. The projection
writes a normal Polecat document through the session, so:

- **Conjoined tenancy applies** without the projection knowing about it — the write carries the
  session's tenant, so a tenanted store gets per-tenant embeddings for free.
- **Soft delete, the migration and the persisted computed column all apply** the way they do for any
  other document.
- **The embedding and the shard's progression row commit in one transaction**, because the write is
  queued onto the session rather than run on a connection of its own. An embedding cannot survive a
  rollback of the events that caused it.

## Embedding from the aggregate, not from one event

A content selector that sees **one event** cannot keep an embedding correct across a partial update.
Given `MemoryRevised { Title = null, Body = "new" }`, where null means "unchanged", returning the new
body re-embeds the document without its title, and returning null leaves the embedding stale. Both
answers are wrong, and there is no third one.

`MapFromAggregate` builds the text from the aggregate's **current state** instead:

```csharp
public record MemoryStarted(string Title, string Body);
public record MemoryRevised(string? Title, string? Body);   // null means "unchanged"

// An ordinary self-aggregating type, and `partial` like every other one in Polecat so the
// source generator can write its evolver.
public partial class Memory
{
    public Guid Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Body { get; set; } = string.Empty;

    public static Memory Create(MemoryStarted e) => new() { Title = e.Title, Body = e.Body };

    public void Apply(MemoryRevised e)
    {
        if (e.Title is not null) Title = e.Title;
        if (e.Body is not null) Body = e.Body;
    }
}

public class MemoryVectorProjection(IEmbeddingProvider provider)
    : VectorProjection<MemoryVector, Guid>(provider)
{
    protected override void Configure(VectorProjectionMap<Guid> map)
    {
        map.MapFromAggregate<Memory>(
            memory => $"{memory.Title}\n{memory.Body}",
            (typeof(MemoryStarted), e => e.StreamId),
            (typeof(MemoryRevised),  e => e.StreamId));
    }
}
```

The triggers say which events make a document's text worth rebuilding; the selector says what the
text is. Content hashing does the rest — an event that turns out not to move the built text costs no
model call at all, which is the common case precisely *because* the events are partial updates.

::: tip Polecat live-aggregates the stream, bounded at the page's last event
Not a snapshot read. The async daemon does not order shards against each other, so reading an async
snapshot could see state another shard has not caught up to and build the embedding from the wrong
text, with nothing reported. Bounding the aggregation at the version the page ends on also makes a
rebuild reproduce exactly what the original run wrote.
:::

::: warning The document id has to be the stream id
Because the aggregate is loaded by live aggregation, `TId` has to be the store's stream identity
type — `Guid` by default, `string` under `StreamIdentity.AsString` — and the trigger's id selector
has to name the stream (`e => e.StreamId`). Anything else is **refused when the store is built**,
naming the expected type, rather than aggregating nothing and silently never writing an embedding.
Key the projection on something else and map content from the events themselves with
`map.Map<TEvent>(content, id)`.
:::

`MapFromAggregate` and `Map<TEvent>` can be mixed across different event types, but one event type
cannot be mapped twice, and an event mapped for content cannot also be mapped for deletion — both are
refused, because the outcome would otherwise depend on registration order.

## Asynchronous only

Registering a vector projection as `Inline` is **refused by name at store construction**:

```
'PageVectorProjection' is registered Inline, but a vector projection is asynchronous only. ...
```

Embedding is a metered network round trip, and an Inline projection runs inside the caller's
`SaveChangesAsync`. A slow model or a provider outage would stall every writer's transaction for the
duration of the call.

The refusal is an ordinary `IValidatedProjection<StoreOptions>` implementation, asked by
`ProjectionGraph.AssertValidity` when the store is built.

::: warning This surfaces errors that used to pass in silence
A **bare** `IProjection` is registered through a `ProjectionWrapper`, and validity used to be checked
on the wrappers rather than on the projections they wrap — so a hand-written projection implementing
`IValidatedProjection<StoreOptions>` was never asked. JasperFx 2.70.0 fixes that. If you have such a
projection in your own code, its checks start running on this upgrade and may surface a configuration
error that was quietly passing before. That is the fix working, not a regression.
:::

## Unchanged content costs nothing

The projection stores a hash of the text it embedded alongside the vector — lowercase hex SHA-256 of
the UTF-8 text, and that spelling is part of the contract rather than an implementation detail,
because it is **persisted**. On the next page, text whose hash matches is skipped without calling the
model at all.

This is the common case rather than an optimisation for a corner: most events in a stream change
something the embedded text does not mention. The model is called **once per page**, for the batch of
documents whose content actually moved.

## Deletes address the row the map wrote

```csharp
map.Delete<PageRemoved>(e => e.Data.PageId);
```

There is deliberately no overload without an id selector. A projection keyed on a payload member
rather than the stream id needs its delete to use the same key, and making the two structurally
incapable of disagreeing beats checking that they agree. The common case costs `e => e.StreamId`.

## A selector that throws is not swallowed

If your content selector throws, the shard faults. It is not caught and converted into "no content
for this event" — that would silently drop the document out of the index with nothing reported
anywhere, and a faulted shard is the only outcome an operator can act on.

## Writing the embedding provider

`IEmbeddingProvider` comes from `JasperFx.Events.Vectors` and is shared across the Critter Stack, so
a provider written for Polecat works against Marten and Fisher unchanged:

```csharp
public class MyProvider : IEmbeddingProvider
{
    public int Dimensions => 1536;

    public async Task<ReadOnlyMemory<float>[]> GenerateEmbeddingsAsync(
        string[] texts, CancellationToken ct = default)
    {
        if (texts.Length == 0) return [];   // must not call the model
        // one call for the whole batch; return one vector per input, IN INPUT ORDER
    }
}
```

Vectors are paired with their texts **by position**, so returning a different number of vectors than
there were texts is refused rather than silently attaching embeddings to the wrong documents.

## What is shared, and what is Polecat's

Since Polecat 5.30 the body of this projection is `JasperFx.Events.Vectors`' — `VectorProjectionMap<TId>`
declares the mapping, and `VectorEmbeddingPlan<TId>` does the fold, the hashing, the unchanged-content
skip and the batched model call. Every store wrote that same code, and one copy is what stops a
fourth port from being a fourth dialect.

What stays Polecat's is the two things only a store can do: read the stored hashes, and write the
documents. Plus the live aggregation above, which is a decision each store makes for itself.

Fisher has the same projection with the same shape. Marten.PgVector's is older and differs in four
ways Polecat's deliberately does not reproduce — it writes on its own connection outside the event
transaction (marten#5421), hardcodes `Guid` identities (marten#5424), deletes by stream id regardless
of the configured id selector (marten#5422), and swallows selector exceptions (marten#5420).
