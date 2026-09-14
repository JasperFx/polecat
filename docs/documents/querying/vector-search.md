# Vector Search

::: warning Requires SQL Server 2025 or later
The `VECTOR` type arrived in SQL Server 2025. Azure SQL Edge and earlier versions do not have it, and
declaring a vector index against one fails when the schema is applied, naming the member and this
requirement.
:::

Polecat searches documents by embedding similarity over a member you declare. Store the vector the
way you store anything else, as a member on the document — a `float[]` is the usual choice, and
[others are accepted](#member-types) — and declare it:

```csharp
opts.Schema.For<Passage>().VectorIndex(x => x.Embedding, dimensions: 768);
```

then search it with a query vector from the same model:

```csharp
var nearest = await session.VectorSearchAsync<Passage>(x => x.Embedding, queryVector, limit: 5);
```

The API shape is Marten.PgVector's, and the types are the store-neutral ones from
`JasperFx.Events.Vectors`: `IEmbeddingProvider` for the model, `DistanceFunction` for the metric,
`VectorMatch<T>` for a scored result. Application code written against one Critter Stack store reads
the same against the others.

## End to end

**Polecat never calls a model.** It stores the vector you hand it and orders by distance to the
vector you search with; producing both is your code. The contract for that code is
`IEmbeddingProvider` from `JasperFx.Events.Vectors`, which arrives with Polecat's own `JasperFx.Events`
dependency. If your model is already a Microsoft.Extensions.AI generator (OpenAI, Azure OpenAI,
Ollama, ONNX), the `JasperFx.Events.MicrosoftExtensionsAI` package adapts it so you do not write a
provider by hand:

```csharp
using JasperFx.Events.MicrosoftExtensionsAI;
using JasperFx.Events.Vectors;

IEmbeddingGenerator<string, Embedding<float>> generator = /* from your M.E.AI provider package */;

IEmbeddingProvider embeddings = generator.AsEmbeddingProvider(dimensions: 768);
```

`dimensions` may be left out when the generator publishes a default dimension count in its metadata.
Passed explicitly it always wins, and either way it has to match the count the index declares.

Declare the index, embed the text before you store the document, and embed the search text with the
same provider:

```csharp
public class Passage
{
    public Guid Id { get; set; }
    public string Text { get; set; } = string.Empty;
    public float[]? Embedding { get; set; }
}

builder.Services.AddPolecat(options =>
{
    options.Connection("...");
    options.Schema.For<Passage>().VectorIndex(x => x.Embedding, dimensions: 768);
});

// Writing: embed first, then store the vector on the document like any other member.
var text = "Revenue rose on subscription renewals.";
var vector = await embeddings.GenerateEmbeddingAsync(text);

session.Store(new Passage { Id = Guid.NewGuid(), Text = text, Embedding = vector.ToArray() });
await session.SaveChangesAsync();

// Searching: embed the question with the same model, then search.
var queryVector = await embeddings.GenerateEmbeddingAsync("how did sales do this quarter?");
var nearest = await querySession.VectorSearchAsync<Passage>(x => x.Embedding, queryVector, limit: 5);
```

Call the model **before** `SaveChangesAsync`, not from inside the unit of work — it is a network round
trip, and nothing about it needs a transaction open. `GenerateEmbeddingAsync` is the one-text
convenience over `GenerateEmbeddingsAsync`, which takes a batch; models charge per call, so embed a
batch of documents in one call.

## Member types

The column is built from the JSON array the member serializes to, so any member that serializes as an
array of numbers is accepted:

- `float[]` or `double[]`
- `ReadOnlyMemory<float>` or `Memory<float>`
- `List<T>`, `IList<T>`, `IReadOnlyList<T>`, `ICollection<T>`, `IReadOnlyCollection<T>` or
  `IEnumerable<T>` of `float` or `double`

Anything else, including a `string` or a `ReadOnlyMemory<double>`, is refused when the store is
configured. The query vector is always a `ReadOnlyMemory<float>`, which a `float[]` converts to
implicitly.

## Scores

```csharp
var matches = await session.VectorSearchWithScoresAsync<Passage>(x => x.Embedding, queryVector, limit: 5);

// Distance is smaller-is-closer under every metric; for cosine it is 1 - similarity.
var confident = matches.Where(m => m.Distance < 0.3).Select(m => m.Document);
```

Every metric is a **distance**: smaller is closer. That is what lets one `ORDER BY` serve all three
and a similarity floor be one comparison. SQL Server returns the dot product already negated, so this
holds natively here with nothing to correct.

| `DistanceFunction` | SQL Server metric | Use for |
| :--- | :--- | :--- |
| `Cosine` (default) | `cosine` | text embeddings, which are trained for it and usually unit length |
| `L2` | `euclidean` | when magnitude carries meaning |
| `InnerProduct` | `dot` | unit vectors, where it equals cosine and is cheaper |

The declaration pins the default metric; a call can name another:

```csharp
var byMagnitude = await session.VectorSearchAsync<Passage>(
    x => x.Embedding, queryVector, limit: 5, distance: DistanceFunction.L2);
```

## Filtering

Both search calls take an optional `filter`, an ordinary LINQ predicate:

```csharp
var nearest = await session.VectorSearchAsync<Passage>(
    x => x.Embedding, queryVector, limit: 5,
    filter: x => x.Tenant == "acme" && x.PublishedOn > cutoff);
```

::: tip The filter runs BEFORE the limit, and that is the whole point
You get the top-k of the *filtered set*, not the filtered remains of the top-k. Applied the other way
round, a selective filter over a `limit: 5` search returns one or two documents instead of five, and
the ones it returns are not the five best matches that satisfy it.
:::

**It supports and refuses exactly what `Query<T>().Where(...)` does**, because it *is* that — the
predicate goes through the same parser, so a member the LINQ provider cannot translate is refused
here with the LINQ provider's own message, and a LINQ operator added tomorrow reaches vector search
the day it lands. There is no second dialect to learn or to drift.

The store's own implicit predicates still apply: conjoined tenancy, soft deletes, and the
`embedding IS NOT NULL` this search always adds. The filter is *in addition to* those, never instead
of them.

::: tip No recall caveat here
A store backed by an approximate index has to warn that a selective filter can return fewer than
`limit` rows, because the filter is applied to whatever the index scan produced and that scan has a
bound of its own (pgvector's `hnsw.ef_search`, default 40). Polecat has no such bound —
`VECTOR_DISTANCE` over a persisted computed column is an exact scan, and the predicate goes into the
same `WHERE`. The result is exactly the filtered top-k.
:::

The same `filter` is on [hybrid search](/documents/querying/hybrid-search), where it is applied to
**both** legs, and on [full text search](/documents/querying/full-text-search).

## Reaching it without naming Polecat

`VectorSearchAsync` is an extension method on Polecat's own `IQuerySession`, so code written against
the store-agnostic `JasperFx.Events.Documents.IDocumentReadOperations` could not call it at all. The
`Search` accessor closes that:

```csharp
IDocumentReadOperations reads = session;

var nearest = await reads.Search.VectorSearchAsync<Passage>(x => x.Embedding, queryVector, limit: 5);
```

See [Store-Neutral Search](/documents/querying/store-neutral-search).

## How it works, and why the column is computed

Declaring a vector adds one persisted computed column over the document body:

```sql
ALTER TABLE <table> ADD [vec_embedding] AS CAST(JSON_QUERY(data, '$.embedding') AS VECTOR(768)) PERSISTED;
```

The search is then one statement: the document's columns plus
`VECTOR_DISTANCE('cosine', [vec_embedding], @query)`, ordered by that distance, limited.

**Computing the column rather than writing it is the point.** The write path is untouched, the column
cannot drift from the document, and declaring a vector on a type that **already has rows** makes every
one of them searchable after that single `ALTER TABLE`, with no backfill. A column maintained on the
write path would go stale under any writer that is not Polecat, and would need a migration for
existing rows.

::: warning
`JSON_QUERY`, not `JSON_VALUE`. Every other computed column in Polecat extracts a scalar through
`JSON_VALUE`, which truncates at 4000 characters. A 768-float array is several times that, so the
scalar form cannot carry an embedding at all.
:::

## The one real limit: no approximate index

SQL Server will not build a `NONCLUSTERED` index over a vector column, and `CREATE VECTOR INDEX` on
current builds is the legacy DiskANN form: it requires `PREVIEW_FEATURES`, at least 100 rows, and
**makes the table read only**, which is not a trade a document table can make. So this is exact
k-nearest-neighbour over a scan. That is fine at thousands of rows and fine-to-slow at millions, and
`VectorSearchAsync` is the seam an approximate index would slot behind once the engine supports one
that permits writes.

That exact scan is also what makes the [filter](#filtering) exactly right rather than approximately
right, so the trade is not all one way.

## What is no longer missing

Vector search composes with LINQ through `OrderByVectorDistance`, so an ordering can be combined with
ordinary filters, paging and projections:

```csharp
var nearest = await session.Query<Passage>()
    .Where(x => x.Team == "red")
    .OrderByVectorDistance(x => x.Embedding, queryVector)
    .Take(5)
    .ToListAsync();
```

`VectorSearchAsync` stays, and the two are not redundant: it is the whole search in one call and
returns scores, where the LINQ form composes.

Embeddings can also be produced for you from an event stream — see
[Vector Projections](/events/projections/vector-projections). Hybrid search is likewise supported:
see [Hybrid Search](/documents/querying/hybrid-search).

## What is refused

| | Why |
| :--- | :--- |
| `VectorSearchAsync` on a type with no declared vector | There is nothing to search; the query would otherwise be valid SQL naming a column that does not exist |
| A member that is not the declared one | The search would silently run against the wrong column |
| A query vector whose length is not the declared `dimensions` | SQL Server would reject every row, one at a time |
| Declaring a member that cannot hold a vector, such as a `string` | Caught when the store is configured, not when the first search returns nothing |
| Declaring the same member twice, or a dimension count under one | Same |
| A `filter` the LINQ provider cannot translate | With the LINQ provider's own message — one parser, so one set of rules |

## Combining with keyword search

Vector search finds meaning near what you asked; [full text search](/documents/querying/full-text-search)
finds the words you actually typed. [Hybrid search](/documents/querying/hybrid-search) fuses both
rankings, which usually beats either alone.

## In the other stores

Fisher: [Vector Search](https://fisher.jasperfx.net/documents/querying/vector-search), over SQLite.
Marten: [Marten.PgVector](https://martendb.io/documents/pgvector), over PostgreSQL's pgvector.

`DistanceFunction`, `VectorMatch<T>`, `IEmbeddingProvider` and the `filter` argument are the shared
`JasperFx.Events.Vectors` types on all three, so the same call reads the same everywhere. What
differs is the index: pgvector has approximate ones and documents its recall caveats, Polecat scans
exactly.
