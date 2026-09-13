# Full Text Search

Polecat searches text over an index you declare on a member:

```csharp
opts.Schema.For<Article>().FullTextIndex(x => x.Body);
```

then query it from LINQ:

```csharp
var hits = await session.Query<Article>()
    .Where(x => x.Body.PlainTextSearch("quick brown fox"))
    .ToListAsync();
```

`PlainTextSearch` requires every term, in any order. `PhraseSearch` requires them adjacent and in
order:

```csharp
var exact = await session.Query<Article>()
    .Where(x => x.Body.PhraseSearch("quick brown fox"))
    .ToListAsync();
```

Both names mirror Marten's, so the same query reads the same against either store.

## This is Polecat's own index, not SQL Server's full-text engine

Worth knowing up front, because it sets expectations that nothing else will.

SQL Server has a full-text engine, and Polecat deliberately does not use it. It is absent from the
official `mssql/server` container images, it is refused outright in `master`, it cannot index a
computed column — and a Polecat document body is JSON — and it populates **asynchronously**, so a
document written and immediately searched may not be found.

Polecat maintains its own inverted index instead: one row per token in a side table beside the
document table, kept in step by a trigger. The consequences that matter to you:

- **A write is searchable the moment it commits.** No population lag, no waiting, no flaky tests.
- **It runs anywhere Polecat runs** — the stock container, Azure SQL Edge, Azure SQL Database.
- **Declaring an index on existing documents backfills them.** The index covers rows written before
  it existed, so you do not reindex by hand.

And the cost, stated plainly:

::: warning Tokenization is simple, and simple means literal
Terms are lowercased and split on whitespace and punctuation. There is **no stemming, no thesaurus,
and no language-aware word breaking**. `running` does not match `run`, and `mice` does not match
`mouse`. If you need linguistic matching, normalize the text yourself before storing it.
:::

Marten's `regConfig` overloads have no counterpart here — a PostgreSQL text-search configuration
means nothing against an index Polecat tokenizes itself — so they are deliberately absent rather
than accepted and quietly ignored.

## Ranking

`Where` answers *which* documents match. For *how well*, there is a ranked search scored with
[Okapi BM25](https://en.wikipedia.org/wiki/Okapi_BM25):

```csharp
var best = await session.FullTextSearchAsync<Article>(x => x.Body, "fox", limit: 10);

var scored = await session.FullTextSearchWithScoresAsync<Article>(x => x.Body, "fox", limit: 10);
var confident = scored.Where(m => m.Score > 1.0).Select(m => m.Document);
```

Larger is more relevant. A document mentioning a term three times in a short body outranks one
mentioning it once in a long one — term frequency up, length normalization down, which is what BM25
is for.

::: tip Scores compare within one result set, not between two
BM25's inverse-document-frequency term depends on the corpus, so a document's score moves as other
documents are written. Use it to order results, or as a relative floor inside one search. Do not
store it as a measure of relevance or compare it across searches.
:::

Ranking derives its statistics from the token rows at query time rather than from a maintained
statistics table, so a ranked search reads the token table for that member. This is correct before
it is fast; a large corpus with tight latency requirements is a reason to measure.

## Several members

`FullTextIndex` takes more than one member, and each is searched independently:

```csharp
opts.Schema.For<Article>().FullTextIndex(x => x.Title, x => x.Body);

var hits = await session.Query<Article>()
    .Where(x => x.Title.PlainTextSearch("fox") || x.Body.PlainTextSearch("fox"))
    .ToListAsync();
```

## An empty search matches nothing

A search whose text has no terms — empty, whitespace, or punctuation alone — returns no documents
rather than all of them. A search box the user has not typed into should not return the entire
table, and Marten's `plainto_tsquery('')` behaves the same way.

## Combining with vector search

Full-text search and [vector search](/documents/querying/vector-search) answer different questions:
one finds the words you asked for, the other finds meaning near what you asked for.
[Hybrid search](/documents/querying/hybrid-search) fuses both.
