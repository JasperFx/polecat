# Paging

Polecat provides built-in pagination support via the `IPagedList<T>` interface.

## ToPagedListAsync

The simplest way to paginate results:

```cs
var pagedList = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .ToPagedListAsync(pageNumber: 1, pageSize: 20);
```

::: tip
Page numbers are 1-based, not 0-based.
:::

## IPagedList Properties

The returned `IPagedList<T>` includes:

| Property | Description |
| :--- | :--- |
| `TotalItemCount` | Total items across all pages |
| `PageCount` | Total number of pages |
| `PageNumber` | Current page number (1-based) |
| `PageSize` | Items per page |
| `HasPreviousPage` | Whether a previous page exists |
| `HasNextPage` | Whether a next page exists |
| `IsFirstPage` | Whether this is the first page |
| `IsLastPage` | Whether this is the last page |
| `FirstItemOnPage` | 1-based index of first item on this page |
| `LastItemOnPage` | 1-based index of last item on this page |

## How It Works

`ToPagedListAsync` executes two queries:

1. A `COUNT(*)` query for the total number of matching documents
2. A `SELECT` with `OFFSET/FETCH` for the current page's data

Both queries run against the same filter criteria.

## Manual Paging

You can also page manually with `Skip` and `Take`:

```cs
var page2 = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .Skip(20)
    .Take(10)
    .ToListAsync();
```

::: warning
Always use `OrderBy` with paging to ensure consistent results across pages.
:::

## Keyset (Cursor) Pagination

Offset paging (`Skip`/`Take`, `ToPagedListAsync`) gets more expensive the deeper you go — the
database still scans and discards every skipped row. **Keyset** (a.k.a. seek or cursor)
pagination is instead **constant cost at any depth**: each page seeks directly to where the
previous one ended. It is ideal for infinite scroll, "load more", and export feeds.

`ToJsonPageByCursorAsync` fetches one page as raw JSON plus an opaque continuation cursor:

```cs
// First page: cursor = null
var page = await session.Query<User>()
    .OrderBy(x => x.LastName).ThenBy(x => x.Id)
    .ToJsonPageByCursorAsync(cursor: null, pageSize: 25);

// page.ItemsJson  -> "[ {...}, {...}, ... ]"  (raw persisted document JSON)
// page.Count      -> number of items on this page
// page.NextCursor -> opaque cursor for the next page, or null at the end of the set

// Next page: pass the previous page's NextCursor
var next = await session.Query<User>()
    .OrderBy(x => x.LastName).ThenBy(x => x.Id)
    .ToJsonPageByCursorAsync(page.NextCursor, 25);
```

When a page returns fewer than `pageSize` items, `NextCursor` is `null` — you have reached the
end of the set.

### Rules

- **You must `OrderBy`.** A query with no ordering is rejected.
- **The terminal ordering key must be the document identity (`Id`).** This guarantees a *total
  order*, so pagination can never skip or duplicate rows across ties. End your ordering with
  `.ThenBy(x => x.Id)` (or order by `Id` alone). A non-identity terminal key is rejected.
- Mixed ascending/descending orderings are supported; the seek predicate flips direction per key.

### How it works

Polecat composes a keyset seek predicate
`(k1 > v1) OR (k1 = v1 AND k2 > v2) OR …` (direction-flipped per ordering key), using the
**same** SQL locators as the `ORDER BY` so the seek boundary lines up with the sort. The last
row's sort-key values are carried in an opaque, versioned (`v1:`) base64-JSON cursor. Cursor
values are **typed on decode** by the query's ordering key types (the cursor never dictates
types) and enter the query as **bound parameters** — no injection.

::: tip SQL Server ordering
String seeks use the column's collation and `uniqueidentifier` (`Guid`) seeks use SQL Server's
byte-group ordering — the same ordering the `ORDER BY` uses, so the boundary always matches.
(Note this differs from PostgreSQL `uuid` ordering, so cursors are not portable across stores.)
:::
