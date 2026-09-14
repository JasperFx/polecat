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

`HybridSearchOptions`, `HybridMatch<T>` and `HybridTextStyle` all come from
`JasperFx.Events.Vectors` and are shared with Marten and Fisher, so this call reads the same against
any of the three.

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
        TextStyle: HybridTextStyle.PlainText));
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

**`Distance: null`** — the default — means "the metric the vector index declared", which is almost
always what a caller wants: an index built for one metric does not answer another one well. This was
the reason to share the options type rather than copy it. Marten's own copy defaulted it to `Cosine`,
so an index declared `L2` was searched by cosine there while Polecat and Fisher searched by L2 — the
same code, three stores, different answers, and nothing reported an error.

**`TextStyle`** picks the operator the text leg uses, and there are exactly two:

| | |
| :--- | :--- |
| `PlainText` (default) | Every term, in any order, with no query syntax at all |
| `WebStyle` | A search box's raw contents — `"quoted phrases"`, a leading `-` to exclude, a bare `or` between alternatives |

**Both are safe to hand a search box's raw contents, and that is why the list is short.** A store's
raw query syntax can be malformed, and a malformed query in one leg of a fused search fails the
*whole* call — where in a plain `Where(x => x.Body.PlainTextSearch(...))` it fails only the thing you
asked for. Polecat's other full-text reach stays on `Query<T>()` for that reason.

::: warning `HybridTextStyle.Phrase` was removed in Polecat 5.30
It has no counterpart in the shared enum and deliberately did not get one. Phrase search itself is
untouched — it is reachable, as it always was, through `Query<T>()`:

```csharp
var matches = await session.Query<Passage>()
    .Where(x => x.Body.PhraseSearch("fox in the snow"))
    .ToListAsync();
```

That is also the better place for it. The phrase leg never carried a ranking of its own — a document
contains the phrase or it does not — so fusing it weakened the fused order rather than improving it,
and when more documents contained the phrase than `CandidateDepth`, which of them were read was not
decided by relevance either. See the [migration guide](/migration-guide).
:::

The two remaining styles do not rank alike. `PlainText` reads the text leg through
[`FullTextSearchAsync`](/documents/querying/full-text-search#ranking), so it arrives in BM25 order.
`WebStyle` has no ranking of its own, so that leg reads through the LINQ operator in whatever order
the database returns, cut at `CandidateDepth`.

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

The explicit two-member overload exists only as `HybridSearchWithScoresAsync`. There is no
`HybridSearchAsync` that names both members, so select `m => m.Document` when you want only the
documents.

## Filtering

```csharp
var results = await session.HybridSearchAsync<Passage>(
    x => x.Embedding, "quarterly revenue", queryVector, limit: 10,
    filter: x => x.Team == "red");
```

::: tip The filter reaches BOTH legs, before each leg's candidate depth
Filtering only after the fusion would let rows you are about to discard consume the candidate depth,
so the fused order would be a ranking of a set that includes them. Filtering only one leg would be
worse still: the excluded document arrives through the other leg anyway.
:::

It is the same predicate, and the same parser, as
[vector search's](/documents/querying/vector-search#filtering) — so it supports and refuses exactly
what `Query<T>().Where(...)` does.

## The fusion is shared

The reciprocal rank fusion itself is `JasperFx.Events.Vectors.ReciprocalRankFusion`, public and
usable on its own over any number of ranked lists:

```csharp
IReadOnlyList<HybridMatch<Passage>> fused =
    ReciprocalRankFusion.Fuse([textRanking, vectorRanking], x => x.Id, limit: 10);
```

It fuses on the **key you give it** rather than on document identity, which is what makes it useful
beyond the call above: a snapshot document carrying the full-text index and a separate embedding
document — exactly the shape a [vector projection](/events/projections/vector-projections) writes —
are two different tables that share an id, and `HybridSearchAsync` searches one type. Rank the two
yourself, fuse on the id, and the scoring is the same one this page describes rather than a fourth
private copy.

## What applies without being restated

Both legs read through the ordinary search paths, so conjoined tenancy, soft deletes and the existing
refusals all behave exactly as they do for a plain full-text or vector search. Ties break
deterministically, so paging a fused result is stable between runs.

## In the other stores

Fisher: [Hybrid Search](https://fisher.jasperfx.net/documents/querying/hybrid-search), the shape this
page mirrors. Marten: the two legs are [Full Text Searching](https://martendb.io/documents/full-text)
and [Marten.PgVector](https://martendb.io/documents/pgvector).

The options record, the match type, the text-style enum and the fusion are one set of types in
`JasperFx.Events.Vectors` on all three stores, so a defaulting decision is made once rather than three
times. Reaching hybrid search without naming a store at all is
[Store-Neutral Search](/documents/querying/store-neutral-search).
