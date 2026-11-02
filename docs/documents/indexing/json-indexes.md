# JSON Indexes

Polecat can create a native SQL Server 2025 **`CREATE JSON INDEX`** over a document's `data` column —
the SQL Server counterpart to Marten's `GinIndexJsonData()` GIN index on the JSONB column.

Unlike a [computed index](/documents/indexing/computed-indexes), which materializes one persisted
computed column per path, a **single** JSON index covers **many** JSON paths at once, with no per-path
computed columns.

::: danger A JSON index only serves `JSON_CONTAINS`, and LINQ does not emit that
The optimizer seeks a JSON index for `JSON_CONTAINS` and for nothing else tried here. Everything Polecat's
LINQ provider writes is `JSON_VALUE` or `OPENJSON`, so **declaring a JSON index does not make a
`Query<T>()` faster.** Measured on SQL Server 2025 over 100,000 documents with `$.userName` indexed:

| Predicate | Plan | Reads |
|---|---|---|
| `JSON_VALUE(data, '$.userName') = @p` | Clustered Index Scan | 2015 |
| `JSON_VALUE(data, '$.userName' RETURNING nvarchar(max)) = @p` | Clustered Index Scan | 2015 |
| `JSON_VALUE(data, '$.userName' RETURNING varchar(250)) = @p` | Clustered Index Scan | 2015 |
| `JSON_PATH_EXISTS(data, '$.userName') = 1` | Clustered Index Scan | 2015 |
| `JSON_CONTAINS(data, @p, '$.userName') = 1` | Seek on the JSON index, then the primary key | 6 |

A `JSON_CONTAINS` you write yourself through `session.AdvancedSql.QueryAsync<T>(...)` is what it is for. To
speed up a LINQ predicate, use a [computed index](/documents/indexing/computed-indexes).
:::

::: warning
Native JSON indexes require the native `json` column type, so they are only available when
`UseNativeJsonType = true` (the default) against **SQL Server 2025**. `CREATE JSON INDEX` is invalid
against `nvarchar(max)` storage, so Polecat throws a clear `InvalidOperationException` if you configure
one on a store that isn't using the native `json` type — use a [computed index](/documents/indexing/computed-indexes)
there instead. JSON indexes are a preview, on-prem SQL Server 2025 feature.
:::

## Indexing Specific Paths

Use `JsonIndex(...)` in `Schema.For<T>()`, passing a single member or an anonymous type for the paths
you want covered:

```cs
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = "...";

    // Index two paths with one JSON index
    opts.Schema.For<User>().JsonIndex(x => new { x.UserName, x.Department });
});
```

This produces one JSON index over both paths:

```sql
SET QUOTED_IDENTIFIER ON;
CREATE JSON INDEX [jidx_pc_doc_user] ON [myschema].[pc_doc_user] (data)
    FOR ('$.userName', '$.department');
```

SQL Server can then use the index for queries against either path:

```cs
var users = await session.Query<User>()
    .Where(x => x.Department == "Engineering")
    .ToListAsync();
```

## Indexing the Whole Document

Omit the expression to index the entire JSON body (no `FOR` clause) — the direct analog of Marten's
`GinIndexJsonData()`:

```cs
opts.Schema.For<User>().JsonIndex();
```

```sql
SET QUOTED_IDENTIFIER ON;
CREATE JSON INDEX [jidx_pc_doc_user] ON [myschema].[pc_doc_user] (data);
```

## Optimizing for Array Search

When you query *inside* a JSON array property (for example `JSON_CONTAINS` over a list), set
`OptimizeForArraySearch` to emit `WITH (OPTIMIZE_FOR_ARRAY_SEARCH = ON)`:

```cs
opts.Schema.For<User>().JsonIndex(x => x.Roles, idx => idx.OptimizeForArraySearch = true);
```

```sql
SET QUOTED_IDENTIFIER ON;
CREATE JSON INDEX [jidx_pc_doc_user] ON [myschema].[pc_doc_user] (data)
    FOR ('$.roles') WITH (OPTIMIZE_FOR_ARRAY_SEARCH = ON);
```

## Customizing a JSON Index

The `JsonIndex()` methods accept an optional `Action<JsonIndex>` to customize the index:

```cs
opts.Schema.For<User>().JsonIndex(x => x.UserName, idx =>
{
    // Override the auto-derived index name (default: jidx_<table>)
    idx.IndexName = "jidx_user_custom";

    // Tune for array searches
    idx.OptimizeForArraySearch = true;

    // Set a FILLFACTOR (1–100)
    idx.FillFactor = 80;
});
```

## Constraints

Native JSON indexes carry a number of SQL Server restrictions, all documented on the `JsonIndex` type:

* Native `json` storage only (`UseNativeJsonType = true`).
* The table's clustered primary key must be ≤ 128 bytes. Polecat's single-tenant `id` primary key
  satisfies this, but **per-tenant tables whose primary key prepends a `varchar` `tenant_id` can exceed
  the limit**, in which case SQL Server rejects the index.
* Only **one** JSON index is allowed per `json` column, so a table has at most one JSON index. Polecat
  keys idempotency on the table, so re-applying the schema never creates a duplicate.
* Indexed paths may not overlap (e.g. `$.a` and `$.a.b`).
* No `UNIQUE`, filtered (`WHERE`), `INCLUDE`, range/`ORDER BY`, or `LIKE` / `IS NULL` support — use a
  [computed index](/documents/indexing/computed-indexes) for any of those.

JSON indexes are therefore **additive** to computed-column indexes, not a replacement: reach for a JSON
index to back `JSON_CONTAINS` over many paths in SQL you write yourself, and a computed index for anything
LINQ asks, or when you need uniqueness, filtering, covering columns, ordering, or portable `nvarchar(max)`
storage.
