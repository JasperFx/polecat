# Vector Search

Polecat searches documents by embedding similarity over a member you declare. Store the vector the
way you store anything else, as a `float[]` on the document, and declare it:

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

Polecat never calls a model. Computing the embedding is yours, and
`JasperFx.Events.MicrosoftExtensionsAI` adapts any Microsoft.Extensions.AI generator (OpenAI, Azure
OpenAI, Ollama, ONNX) to `IEmbeddingProvider` so you do not write one by hand.

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

## What is not supported yet

**No approximate index.** SQL Server will not build a `NONCLUSTERED` index over a vector column, and
`CREATE VECTOR INDEX` on current builds is the legacy DiskANN form: it requires `PREVIEW_FEATURES`, at
least 100 rows, and **makes the table read only**, which is not a trade a document table can make. So
this is exact k-nearest-neighbour over a scan. That is fine at thousands of rows and fine-to-slow at
millions, and `VectorSearchAsync` is the seam an approximate index would slot behind once the engine
supports one that permits writes.

**Not reachable from LINQ.** A vector distance carries a bound parameter, and Polecat's `ORDER BY`
clauses are rendered as plain text, so the ordering cannot be expressed through `IQueryable` today.
The search runs as its own statement instead.

**No hybrid search**, because Polecat has no full-text search to fuse with yet.

## What is refused

| | Why |
| :--- | :--- |
| `VectorSearchAsync` on a type with no declared vector | There is nothing to search; the query would otherwise be valid SQL naming a column that does not exist |
| A member that is not the declared one | The search would silently run against the wrong column |
| A query vector whose length is not the declared `dimensions` | SQL Server would reject every row, one at a time |
| Declaring a member that cannot hold a vector, such as a `string` | Caught when the store is configured, not when the first search returns nothing |
| Declaring the same member twice, or a dimension count under one | Same |
