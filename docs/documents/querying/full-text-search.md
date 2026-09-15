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

`WebStyleSearch` takes a search box's raw contents, so you can hand it whatever the user typed:

```csharp
var hits = await session.Query<Article>()
    .Where(x => x.Body.WebStyleSearch("\"quick brown\" fox -turtle"))
    .ToListAsync();
```

- bare words are **required**
- `"quoted text"` is a **phrase**
- a leading `-` **excludes**
- a bare `or` separates **alternatives**

It mirrors Marten's operator, which uses PostgreSQL's `websearch_to_tsquery`. Two differences are
worth knowing: Polecat does not stem (see below), and `or` splits at the top level — `a b or c` is
`(a AND b) OR (c)`, where PostgreSQL binds it as `a AND (b OR c)`. The simpler rule is the one that
can be explained to someone typing into a box; if you need real boolean precedence, compose
`PlainTextSearch` and `PhraseSearch` with C#'s own `&&` and `||`, where the precedence is the
language's rather than ours.

A query of nothing but exclusions matches nothing — "not this" is not a search.

`PrefixSearch` matches from the **start** of a term, which is what a search-as-you-type box wants —
the last word a user has typed is a prefix of what they mean, not a word:

```csharp
var suggestions = await session.Query<Article>()
    .Where(x => x.Body.PrefixSearch("qui"))   // finds "quick"
    .ToListAsync();
```

Each word is a prefix and **all of them are required**, so `PrefixSearch("qui bro")` wants a document
with a term starting `qui` and another starting `bro`. This mirrors Marten's operator of the same
name, which appends PostgreSQL's `:*` to each word of a `to_tsquery`. A whole term is a prefix of
itself, so `PrefixSearch` is a superset of `PlainTextSearch`.

::: warning From the start of a term, not anywhere inside one
`PrefixSearch("uic")` does not find `quick`. The token table stores whole terms, so a substring
search against it is not a slow query — it is an empty result that looks like an answer. Marten
reaches that capability through a separate ngram index and Fisher through a trigram tokenizer;
Polecat has neither yet.
:::

An empty or all-punctuation prefix matches nothing rather than everything, the same answer
`PlainTextSearch` gives an empty search — a search box on first render should not return the whole
table. That also disposes of LIKE pattern syntax: `%`, `_` and `[` are punctuation to the tokenizer,
so they are stripped before the prefix is ever compared and cannot act as wildcards.

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

### Tuning BM25

The two BM25 parameters default to the conventional values — `k1 = 1.2` for term-frequency saturation
and `b = 0.75` for length normalization — and both ranked calls take a `FullTextSearchOptions` to
change them:

```csharp
// A title field: short by nature, so do not penalize length at all.
var titles = await session.FullTextSearchAsync<Article>(
    x => x.Title, "fox", options: new FullTextSearchOptions(B: 0));

// Ignore term frequency entirely — a document's score becomes the sum of its terms' IDF.
var idfOnly = await session.FullTextSearchAsync<Article>(
    x => x.Body, "fox", options: new FullTextSearchOptions(K1: 0));
```

`k1` must not be negative and `b` must be within `[0, 1]`; anything else is refused at construction
with an `ArgumentOutOfRangeException` naming the parameter, rather than silently producing a ranking
that looks plausible and is not. `k1 = 0` and `b = 0` are both legal and both meaningful.

They are per call rather than per index on purpose: one corpus is often queried two ways, and binding
the constants to the index would force a second index to say so. They bind as SQL parameters, so a
different `k1` reuses the same query plan.

::: warning Scores from different options are not comparable
This is on top of BM25 scores already not being comparable across corpora. A relevance floor tuned
against the defaults is meaningless against `b = 0`. Ordering within one result set is what these are
safe for.
:::

A [hybrid search](/documents/querying/hybrid-search) scores its text leg with the defaults.
`HybridSearchOptions` is shared across the Critter Stack and cannot carry Polecat-only constants.

::: tip Scores compare within one result set, not between two
BM25's inverse-document-frequency term depends on the corpus, so a document's score moves as other
documents are written. Use it to order results, or as a relative floor inside one search. Do not
store it as a measure of relevance or compare it across searches.
:::

Ranking derives its statistics from the token rows at query time rather than from a maintained
statistics table, so a ranked search reads the token table for that member. This is correct before
it is fast; a large corpus with tight latency requirements is a reason to measure.

### Filtering a ranked search

Both ranked calls take an optional `filter`, the same LINQ predicate
[vector search](/documents/querying/vector-search#filtering) takes:

```csharp
var best = await session.FullTextSearchAsync<Article>(
    x => x.Body, "fox", limit: 10, filter: x => x.Team == "red");
```

It is applied **before** the limit, so you get the best `limit` of the filtered set rather than the
filtered remains of the best `limit` — the same reason it matters there. A `Where` on the LINQ
operator is still the right tool when you do not need the ranking; this exists so a ranked search can
be scoped without losing its ordering.

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

## Conjoined multi-tenancy

Every full-text path is tenant-scoped, and so is the index behind it. A document id is only unique
*per tenant* under conjoined tenancy — the document table puts `tenant_id` in its primary key —
so the token table carries the tenant too, and each of `PlainTextSearch`, `PhraseSearch`,
`PrefixSearch`, `WebStyleSearch`, `FullTextSearchWithScoresAsync` and the text leg of a hybrid search
sees only the session tenant's documents.

The token table is maintained by a trigger and backfilled when the index is declared, and both
are keyed on `(doc_id, tenant_id)` rather than on `doc_id` alone. They were not always, and
[#625](https://github.com/JasperFx/polecat/issues/625) had three consequences worth knowing if
you are upgrading across it:

* `PhraseSearch` could assemble a phrase from **two tenants'** documents, returning a document that
  does not contain the phrase.
* An ordinary write in one tenant **deleted another tenant's tokens**, dropping that document
  out of every full-text path until it was next written.
* Declaring the index over existing rows could skip a tenant whose id was already indexed by
  another.

::: tip
Upgrading repairs itself. The trigger is `CREATE OR ALTER` and the backfill runs whenever
storage is ensured, so the corrected trigger replaces the old one and the corrected backfill
restores the tokens the old one destroyed. Nothing has to be rebuilt by hand.
:::

A single-tenant store is unaffected — its document table has no `tenant_id` column at all, and
an id is a complete key on its own.

## Combining with vector search

Full-text search and [vector search](/documents/querying/vector-search) answer different questions:
one finds the words you asked for, the other finds meaning near what you asked for.
[Hybrid search](/documents/querying/hybrid-search) fuses both.

## In the other stores

Fisher: [Full Text Search](https://fisher.jasperfx.net/documents/querying/linq/full-text), over
SQLite's FTS5. Marten: [Full Text Searching](https://martendb.io/documents/full-text), over
PostgreSQL's `tsvector`.
