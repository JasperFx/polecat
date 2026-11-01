# Searching on String Fields

Polecat's LINQ provider translates .NET string methods into SQL Server JSON path queries.

## Contains

```cs
var results = await session.Query<User>()
    .Where(x => x.LastName.Contains("son"))
    .ToListAsync();
```

## StartsWith

```cs
var results = await session.Query<User>()
    .Where(x => x.LastName.StartsWith("Sm"))
    .ToListAsync();
```

## EndsWith

```cs
var results = await session.Query<User>()
    .Where(x => x.Email.EndsWith("@example.com"))
    .ToListAsync();
```

## Equals

```cs
var results = await session.Query<User>()
    .Where(x => x.Email.Equals("admin@example.com"))
    .ToListAsync();
```

## IsNullOrEmpty

```cs
var results = await session.Query<User>()
    .Where(x => !string.IsNullOrEmpty(x.Email))
    .ToListAsync();
```

`IsNullOrEmpty` matches what .NET does: null and the empty string, and not a value of nothing but
spaces.

## IsNullOrWhiteSpace

```cs
var results = await session.Query<User>()
    .Where(x => !string.IsNullOrWhiteSpace(x.Nickname))
    .ToListAsync();
```

Whitespace means what `char.IsWhiteSpace` means, so tabs, line breaks and the Unicode space separators
all count, whatever the database's collation is.

## Trim

```cs
var results = await session.Query<User>()
    .Where(x => x.Code.Trim() == "ABC")
    .ToListAsync();
```

`TrimStart` and `TrimEnd` work the same way, and all three remove the same characters
`IsNullOrWhiteSpace` looks for. The comparison after the trim follows the database collation, as every
other string comparison does.

## Strings over 4000 characters

A string member can be any length, and predicates see the whole of it, including `==`, `!=`, null checks,
`IsOneOf` and the `Contains` family.

That has a small cost, because SQL Server's plain `JSON_VALUE` answers `NULL` for anything over 4000
characters, so Polecat has to read through `JSON_VALUE(... RETURNING nvarchar(max))` on native `json`
storage, or `OPENJSON` on `nvarchar(max)` storage. On native storage that is about 1.7x the CPU of a plain
read on a table scan. A `==` or an `IsOneOf` against a value of 4000 characters or fewer does not pay it,
because a stored value longer than that cannot equal it, so those keep the plain read unless they are
negated.

An [indexed](/documents/indexing/computed-indexes) member keeps using its index. The column's width is the
limit of what the index can see: a value wider than the column reads as `NULL` through it on native storage
and as truncated on `nvarchar(max)` storage, so a `!=` will not see that row. A `==` or `IsOneOf` against a
value too wide for the column goes to the document instead. Set `SqlType` on the index if a member can
outgrow the default `varchar(250)`.

::: warning Only predicates
`Select(x => x.Text)`, `OrderBy`, `GroupBy` and `Distinct` over a string member still read through plain
`JSON_VALUE`, so a value over 4000 characters reads as `NULL` there. To get one back, select the document.
:::

## Case Sensitivity

SQL Server string comparisons follow the database collation by default. For case-insensitive searches, configure your database collation accordingly.
