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
    protected override void Configure(VectorProjectionMap<PageVector, string> map)
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

## Asynchronous only

Registering a vector projection as `Inline` is **refused by name at store construction**:

```
'PageVectorProjection' is a vector projection and can only run asynchronously. ...
```

Embedding is a metered network round trip, and an Inline projection runs inside the caller's
`SaveChangesAsync`. A slow model or a provider outage would stall every writer's transaction for the
duration of the call.

## Unchanged content costs nothing

The projection stores a SHA-256 hash of the text it embedded alongside the vector. On the next page,
text whose hash matches is skipped without calling the model at all.

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

## Compared to the other stores

Fisher has the same projection with the same shape. Marten.PgVector's is older and differs in four
ways Polecat's deliberately does not reproduce — it writes on its own connection outside the event
transaction (marten#5421), hardcodes `Guid` identities (marten#5424), deletes by stream id regardless
of the configured id selector (marten#5422), and swallows selector exceptions (marten#5420).
