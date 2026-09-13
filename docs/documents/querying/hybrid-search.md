# Hybrid Search

Hybrid search runs a [full-text search](/documents/querying/full-text-search) and a
[vector search](/documents/querying/vector-search) over the same documents and fuses their rankings.
It exists because the two legs fail in different places: keyword search misses a document that means
what you asked but says it differently, and vector search misses the exact term, the product code,
the surname. A document both legs rank well is a better answer than either leg's favourite.

Declare both indexes on the type, then search:

```csharp
opts.Schema.For<Passage>()
    .FullTextIndex(x => x.Body)
    .VectorIndex(x => x.Embedding, dimensions: 768);

var results = await session.HybridSearchAsync<Passage>(
    x => x.Embedding, "quarterly revenue", queryVector, limit: 10);
```

The shape matches Fisher's, so application code ports between the stores.

::: warning Requires SQL Server 2025 or later
The vector leg needs the `VECTOR` type, which arrived in SQL Server 2025. See
[Vector Search](/documents/querying/vector-search).
:::

## Scores

```csharp
var scored = await session.HybridSearchWithScoresAsync<Passage>(
    x => x.Embedding, "quarterly revenue", queryVector, limit: 10);
```

Larger is better. The score is a **fused rank**, not a similarity — see below.

## How the fusion works

Reciprocal rank fusion: `score(d) = Σ 1 / (k + rank(d))` over the legs that found the document, with
1-based ranks.

**Only the rankings are fused, never the scores.** A BM25 score and a vector distance are not on one
scale, and no normalization makes them comparable across corpora. RRF's appeal is that it needs
nothing but the ordering each leg already produces — which is also why a hybrid score tells you how
the legs agreed, and nothing about similarity in absolute terms.

## Options

```csharp
var results = await session.HybridSearchAsync<Passage>(
    x => x.Embedding, "quarterly revenue", queryVector, limit: 10,
    options: new HybridSearchOptions(
        K: 60,
        CandidateDepth: 100,
        Distance: DistanceFunction.Cosine,
        TextStyle: HybridTextStyle.Plain));
```

**`K`** is RRF's smoothing constant, conventionally 60. Larger flattens the difference between ranks;
smaller lets the top of each leg dominate. It must be at least 1 — zero would make the top-ranked
document of either leg score infinitely.

**`CandidateDepth`** is how deep each leg is read before fusing, defaulting to `max(limit × 4, 50)`.

::: tip CandidateDepth must exceed limit, and that is the entire point
A document ranked 40th by one leg and 1st by the other is exactly the result hybrid search exists to
surface. Reading only `limit` from each leg would never see it. A depth below `limit` is refused.
:::

**`Distance`** overrides the vector index's declared metric for this search.

**`TextStyle`** picks the operator the text leg uses: `Plain` (every term, any order) or `Phrase`
(adjacent and in order). Both are safe to hand a search box's raw contents, which is why the list is
short — Polecat's other full-text reach stays on `Query<T>()`, where a malformed query fails only the
thing you asked for rather than both legs of a fused search.

## Which member is searched

Polecat's full-text operators address a **member**, following Marten. The portable overload above
takes the *vector* member and uses the type's full-text index when there is exactly one — the common
case. When a type declares several text members, say which:

```csharp
opts.Schema.For<TwoTexts>()
    .FullTextIndex(x => x.Title, x => x.Body)
    .VectorIndex(x => x.Embedding, dimensions: 768);

var results = await session.HybridSearchWithScoresAsync<TwoTexts>(
    x => x.Body, x => x.Embedding, "quarterly revenue", queryVector, limit: 10);
```

The portable overload refuses rather than guessing, and names the declared members so the fix is
obvious.

## What applies without being restated

Both legs read through the ordinary search paths, so conjoined tenancy, soft deletes and the existing
refusals all behave exactly as they do for a plain full-text or vector search. Ties break
deterministically, so paging a fused result is stable between runs.
