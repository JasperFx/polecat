# Store-Neutral Search

[Vector search](/documents/querying/vector-search) and
[hybrid search](/documents/querying/hybrid-search) are extension methods on Polecat's own
`IQuerySession`, and every other Critter Stack store does the same on its own session type. That is
fine in an application, which has picked a store. It is a wall for a library that has not.

`JasperFx.Events.Documents.IDocumentReadOperations` is the store-agnostic read contract — `Query<T>()`,
`LoadAsync<T>()`, `Events` — and code written against it compiles once and runs on Marten, Polecat or
Fisher. Until Polecat 5.30 it could not ask for "the ten documents nearest this embedding" at all,
because every store's answer to that question was behind a cast to store internals.

## The `Search` accessor

```csharp
using JasperFx.Events.Documents;
using JasperFx.Events.Vectors;

public class Retriever(IDocumentReadOperations reads)
{
    public Task<IReadOnlyList<Passage>> NearestAsync(ReadOnlyMemory<float> queryVector)
        => reads.Search.VectorSearchAsync<Passage>(x => x.Embedding, queryVector, limit: 10);
}
```

Four methods are reachable from it, and they are the same four Polecat's own extension methods
expose:

| | |
| :--- | :--- |
| `VectorSearchWithScoresAsync<T>` | Nearest by embedding, each with its distance — **smaller is closer** |
| `VectorSearchAsync<T>` | The same, documents only |
| `HybridSearchWithScoresAsync<T>` | A full-text ranking and a vector ranking fused, each with its fused score — **larger is better** |
| `HybridSearchAsync<T>` | The same, documents only |

Both take the same optional `filter`, so a store-neutral caller gets the filtered top-k too.

Nothing is reimplemented behind the accessor. Every call forwards to the extension method a Polecat
caller would have used, so conjoined tenancy, soft deletes, the vector-declaration refusals and the
full-text member inference are all the ones documented on those pages.

## Why an accessor and not members on the session

::: warning This is the one design decision on the page, and it is load-bearing
Polecat's extension methods are *already* named `VectorSearchWithScoresAsync` and
`HybridSearchWithScoresAsync`. Had those names been declared as members on an interface `IQuerySession`
implements, the instance member would beat the extension method at **every existing call site** —
silently, with no error, no warning, and against a different implementation than the one that knows
about Polecat's tenancy and soft-delete predicates. Keeping the contract behind `Search` makes that
collision impossible to have.
:::

For the same reason the contract declares only the two *scored* methods; the document-only forms are
extension methods over them, so a store implements two methods and the projection to documents cannot
differ between stores.

## What it does not change

The contract member carries a **throwing default implementation** on `IDocumentReadOperations`, so
this is additive — a store that has no similarity search leaves the default in place, and one that
does returns an implementation. Nothing you already wrote against Polecat's own session moves or
behaves differently.

The behaviour is not portable just because the API is. Polecat scans exactly and so returns exactly
the filtered top-k; a store with an approximate index may return fewer rows for a selective filter and
documents its own recall limits. Write against the contract for reach, and read each store's page for
what its numbers mean.
