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

## Members of .NET Types

A document is stored as JSON, so a member like `x.N.Value` or `x.Items.Count` has no key of its own
there: it exists only on the .NET type. Polecat decides this from the type the member is read off,
never from its name, so `x.Stats.Count` on a class of your own with a `Count` property is a normal JSON
key. These translate wherever a member can be written, including `Where`, `Select`, `OrderBy`,
`GroupBy`, the JSON streaming and cursor paging paths and event queries:

| Read off | Member | SQL |
|---|---|---|
| `Nullable<T>` | `.Value` | the stored value, so `x.N.Value == 5` is the same as `x.N == 5` |
| `Nullable<T>` | `.HasValue` | `IS NOT NULL` in a predicate, a `CASE` where a value is needed |
| `string` | `.Length` | `DATALENGTH / 2`, which counts trailing spaces as .NET does |
| array, list, set, dictionary | `.Length`, `.Count` | `SELECT COUNT(*) FROM OPENJSON(...)` |
| `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly` | `.Year`, `.Month`, `.Day`, `.Hour`, `.Minute`, `.Second`, `.Millisecond`, `.DayOfYear` | `DATEPART(...)` |
| `DateTime`, `DateTimeOffset`, `DateOnly` | `.DayOfWeek` | counted from a known Monday, so `SET DATEFIRST` cannot change it |
| `DateTime`, `DateTimeOffset` | `.Date` | `CONVERT(date, ...)` |

Three things differ from running the same code in memory:

- `.Value` of an empty nullable does not throw. The row has no value, so it matches no predicate, sorts
  as a `NULL`, and a scalar `Select(x => x.N.Value)` reads it as `default`. Guard with `x.N.HasValue` if
  that matters.
- `.Count` of a null collection is `0` rather than a `NullReferenceException`, which is what `IsEmpty()`
  has always done.
- The parts of a `DateTimeOffset` are the ones it was stored with, as in .NET. The parts of a `DateTime`
  stored with an offset are the ones in that offset.

A collection's `Count` needs a subquery, and SQL Server allows none in a `GROUP BY` key or inside an
aggregate over the group, so `GroupBy(x => x.Items.Count)` and `g.Sum(x => x.Items.Count)` are refused.
`Where`, `Select`, `OrderBy` and `Sum(x => x.Items.Count)` over the whole query are fine.

**Any other member of those types is refused**, along with the members of `TimeSpan`, `Guid`, `decimal`
and the numeric types. That covers `x.CreatedAt.Ticks`, `x.CreatedAt.Offset`, `x.Span.TotalHours`,
`x.Items.Capacity`, `x.Map.Keys`, the `Length` of a `byte[]` (stored as a base64 string), and the `Count`
of a collection class of your own, which can mean anything. A member reached any other way than from
the document is refused too: `OrderBy(x => captured.Name)`, `OrderBy(x => x.Name.Trim().Length)`, or
`.Select(x => x.Name).Where(n => n.Length > 3)`. Each of these used to read a JSON path that could never
exist and quietly match nothing.

A complex `Select` that materializes through `ToListAsync()` is not affected, because it runs in memory:
`Select(x => new { x.CreatedAt.Ticks })` works, and only the JSON streaming calls refuse it.

Values with no document in them are worked out before the query runs, which is what makes the usual
optional filter work. The right side of a decided `||` or `&&` is never evaluated, exactly as in C#:

```cs
int? excluded = null;

var results = await session.Query<User>()
    .Where(x => !excluded.HasValue || x.Age != excluded.Value)
    .ToListAsync();
```
