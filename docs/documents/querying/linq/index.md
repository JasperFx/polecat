# Querying Documents with LINQ

Polecat provides a custom LINQ provider that translates .NET LINQ queries into SQL Server queries against the JSON document data.

## Basic Queries

```cs
// Simple filter
var smiths = await session.Query<User>()
    .Where(x => x.LastName == "Smith")
    .ToListAsync();

// With ordering
var sorted = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .ThenBy(x => x.FirstName)
    .ToListAsync();

// First or default
var first = await session.Query<User>()
    .FirstOrDefaultAsync(x => x.Email == "alice@example.com");
```

## Aggregates

```cs
var count = await session.Query<User>().CountAsync();
var any = await session.Query<User>().AnyAsync(x => x.Internal);
```

## Paging

```cs
var page = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .Skip(20)
    .Take(10)
    .ToListAsync();
```

Or use the built-in paging support:

```cs
var pagedList = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .ToPagedListAsync(pageNumber: 2, pageSize: 10);
```

See [Paging](/documents/querying/linq/paging) for more details.

## How It Works

LINQ queries are translated to SQL using `JSON_VALUE()` to extract properties from the JSON document:

```sql
SELECT data FROM pc_doc_user
WHERE JSON_VALUE(data, '$.lastName') = @p0
ORDER BY JSON_VALUE(data, '$.firstName')
```

The LINQ provider supports:

- Equality and comparison operators
- String operations (Contains, StartsWith, EndsWith)
- Boolean logic (And, Or, Not)
- Null checks
- Collection operations (Any, All, Contains)
- Arithmetic operations
- Nested property access

See [Supported LINQ Operators](/documents/querying/linq/operators) for a complete list.

## When a Query Cannot Be Translated

A query Polecat cannot turn into SQL is **refused**, with `Polecat.Linq.BadLinqExpressionException`.
That type derives from `JasperFx.BadLinqExpressionException`, so store-agnostic code can catch the
shared type and have it work on Polecat and Fisher alike.

The refusal is a correctness guarantee rather than an inconvenience: the alternative to throwing is a
query that returns plausible but wrong rows. The message names what *is* accepted in that position,
and the ways through are:

- rewrite the condition over document members, which is what translates;
- run it as raw SQL through `session.AdvancedSql.QueryAsync<T>(...)`;
- materialize the query and finish the work in memory with LINQ-to-Objects.

An **operator** Polecat cannot translate is refused the same way. `Order()`, `OrderDescending()`,
`Reverse()`, `TakeWhile()`, `SkipWhile()`, `Union()`, `Concat()`, `Except()`, `Join()` and
`DefaultIfEmpty()` are not translated, and a query carrying one is rejected rather than run without
it — ignoring an ordering gives you rows in an arbitrary order that look sorted, and ignoring a
filter gives you *more* rows than you asked for. Both read as answers. `Cast<T>()`, `OfType<T>()`
(when `T` is the element type) and `AsQueryable()` are genuine no-ops and are allowed through; a
*narrowing* `OfType<TSubClass>()` is refused, because document subclasses are queried with
`Query<TSubClass>()`.

A plain `NotSupportedException` means something else, and is deliberately kept distinct: an
unsupported *API* rather than an untranslatable expression. Synchronous execution
(`ToList()`/`foreach` over a `Query<T>()`) and calling a marker method such as `PlainTextSearch`
outside a query are the two you are likely to meet.
