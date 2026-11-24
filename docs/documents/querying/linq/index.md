# Querying Documents with LINQ

Polecat has its own LINQ provider. It translates the standard
[LINQ](https://learn.microsoft.com/en-us/dotnet/csharp/linq/) operators, plus some Polecat-specific ones, into SQL
Server queries against the JSON document data. You start a query with `IQuerySession.Query<T>()` or
`IDocumentSession.Query<T>()`, which returns an `IPolecatQueryable<T>`, an
[`IQueryable<T>`](https://learn.microsoft.com/en-us/dotnet/api/system.linq.iqueryable-1) for the document type `T`.

::: warning
Every Polecat query runs asynchronously. The synchronous `ToList()`, `First()`, `Count()`, `foreach` and the rest
still compile, because they come from `System.Linq`, but they throw `NotSupportedException`. Use `ToListAsync()`,
`FirstAsync()`, `CountAsync()` and the other `*Async()` operators from `Polecat.Linq`, or
[`ToAsyncEnumerable()`](#streaming-results) to stream the results.
:::

## What Polecat Supports {#what-polecat-supports}

The tables below list the LINQ operators and expression shapes Polecat translates to SQL. Each row links to an
example. Polecat does not run part of a query in memory to make up for a shape it cannot translate: it refuses the
query, and the last table says which exception you get and what to do instead. The exception is a `Select()`: one
that builds an object, or computes a value with nothing after it, runs in memory over the documents the query
read.

### Filtering with Where() {#supported-filtering}

| Shape | Example |
| --- | --- |
| `==`, `!=`, `>`, `>=`, `<` and `<=` on members, members of members (`x.Address.City`), and between two members | [Comparison Operators](/documents/querying/linq/operators#comparison-operators) |
| `&&`, `\|\|` and `!`, and boolean members: `x.Flag`, `!x.Flag`, `x.Flag == false` | [Boolean Logic](/documents/querying/linq/operators#boolean-logic) |
| Arithmetic and `%` inside a predicate | [Arithmetic](/documents/querying/linq/operators#arithmetic) |
| `== null`, `!= null`, and `HasValue` and `Value` of a nullable | [Null Checks](/documents/querying/linq/operators#null-checks) |
| Captured variables, statics, and the optional filter `!captured.HasValue \|\| x.Age != captured.Value` | [Captured Values and Optional Filters](#optional-filters) |
| A local array, `List<T>` or `HashSet<T>`: `ids.Contains(x.Id)`, a SQL `IN`. Also `IsOneOf()` and `In()` | [Contains() over a Local Collection](/documents/querying/linq/operators#contains-over-a-local-collection) |
| Parts of a date: `.Year`, `.Month`, `.Day`, `.Hour`, `.DayOfWeek`, `.Date` and the rest | [Members of .NET Types](#members-of-net-types) |
| Full-text search: `PlainTextSearch()`, `PhraseSearch()`, `PrefixSearch()`, `WebStyleSearch()` | [Full Text Search](/documents/querying/full-text-search) |
| Soft-deleted documents: `MaybeDeleted()`, `IsDeleted()`, `DeletedSince()`, `DeletedBefore()` | [Querying Soft-Deleted Documents](/documents/deletes#querying-soft-deleted-documents) |
| Metadata: `CreatedSince()`, `CreatedBefore()`, `ModifiedSince()`, `ModifiedBefore()` | [Metadata LINQ Extensions](/documents/metadata#metadata-linq-extensions) |
| Conjoined tenancy: `AnyTenant()` and `TenantIsOneOf()` | [Querying Across Tenants](#querying-across-tenants) |
| Waiting for async projections first: `QueryForNonStaleData()` | [Per-Query](/events/projections/async-daemon#per-query) |
| C# 15 union members on .NET 11: `x.Payment.Value is Card`, `((Card)x.Payment.Value).Last4` | [Querying C# Union Members](#union-members) |

### Strings {#supported-strings}

| Shape | Example |
| --- | --- |
| `StartsWith()`, `EndsWith()` and `Contains()` | [Searching on String Fields](/documents/querying/linq/strings#contains) |
| `==`, `!=` and `Equals()`, comparing the whole value, trailing spaces included | [Equals](/documents/querying/linq/strings#equals) |
| `Equals(value, StringComparison.OrdinalIgnoreCase)` | [Case Sensitivity](/documents/querying/linq/strings#case-sensitivity) |
| `string.IsNullOrEmpty()` and `string.IsNullOrWhiteSpace()` | [IsNullOrEmpty](/documents/querying/linq/strings#isnullorempty) |
| `ToLower()`, `ToUpper()`, their `Invariant` forms, `Trim()`, `TrimStart()` and `TrimEnd()`, chained | [Chaining](/documents/querying/linq/strings#chaining) |
| `Length` | [Members of .NET Types](#members-of-net-types) |
| Values over 4000 characters, in a predicate | [Strings over 4000 characters](/documents/querying/linq/strings#strings-over-4000-characters) |

### Collections and Child Collections {#supported-collections}

| Shape | Example |
| --- | --- |
| `Contains(value)` on a collection the document holds | [Contains](/documents/querying/linq/child-collections#contains) |
| The `Count` and `Length` properties | [Count](/documents/querying/linq/child-collections#count) |
| `IsEmpty()`, true for an empty or `null` collection | [IsEmpty](/documents/querying/linq/child-collections#isempty) |
| An element by position, `x.Values[1]`, `x.Grid[1][0]`, `ElementAt(1)`, and a dictionary key, `x.Scores["math"]` | [Indexing](/documents/querying/linq/child-collections#indexing) |
| Members of an object inside the document | [Nested Property Access](/documents/querying/linq/child-collections#nested-property-access) |

### Projections with Select() {#supported-select}

| Shape | Example |
| --- | --- |
| A member, or a member of a member, read in SQL | [Projection](/documents/querying/linq/operators#projection) |
| An anonymous type, a DTO or a record, built in memory from each document | [Projection](/documents/querying/linq/operators#projection) |
| The same, built in SQL with `JSON_OBJECT` by `ToJsonArrayAsync()` and the other JSON calls | [Projecting with Select()](/documents/querying/query-json#projecting-with-select) |
| `Where()`, `OrderBy()`, `Distinct()` and aggregates after a `Select()`, reading what it projected | [Operators after a Select](#operators-after-a-select) |

### SelectMany() {#supported-selectmany}

| Shape | Example |
| --- | --- |
| Flatten a collection of child documents or of values | [SelectMany](/documents/querying/linq/child-collections#selectmany) |
| `Where()`, `OrderBy()`, `Select()`, `Distinct()` of a scalar, `First()`, `Single()`, `Count()`, `Any()` and aggregates after it | [SelectMany](/documents/querying/linq/child-collections#selectmany) |
| `Take()` and `Skip()` before it page the documents, and after it page the flattened rows | [SelectMany](/documents/querying/linq/child-collections#selectmany) |

### Ordering and Paging {#supported-ordering}

| Shape | Example |
| --- | --- |
| `OrderBy()`, `OrderByDescending()`, `ThenBy()` and `ThenByDescending()` | [Ordering](/documents/querying/linq/operators#ordering) |
| A computed key, `OrderBy(x => -x.Number)`, an element by position, or a dictionary key | [Computed keys](/documents/querying/linq/operators#computed-keys) |
| `Take()` and `Skip()`, and an aggregate after them computed over the page | [Paging](#paging) |
| `ToPagedListAsync()` | [Paging](/documents/querying/linq/paging#topagedlistasync) |
| Keyset paging with `ToJsonPageByCursorAsync()` | [Keyset (Cursor) Pagination](/documents/querying/linq/paging#keyset-cursor-pagination) |

### Results, Aggregates and Element Operators {#supported-results}

| Shape | Example |
| --- | --- |
| `ToListAsync()` | [Basic Queries](#basic-queries) |
| `ToAsyncEnumerable()` | [Streaming Results](#streaming-results) |
| `FirstAsync()`, `FirstOrDefaultAsync()`, `SingleAsync()`, `SingleOrDefaultAsync()`, `LastAsync()` and `LastOrDefaultAsync()` | [Aggregation](/documents/querying/linq/operators#aggregation) |
| `AnyAsync()`, `CountAsync()`, `LongCountAsync()`, `MinAsync()`, `MaxAsync()`, `SumAsync()` and `AverageAsync()` | [Aggregation](/documents/querying/linq/operators#aggregation) |
| `Distinct()` | [Distinct](/documents/querying/linq/operators#distinct) |
| `DistinctBy()` on the documents, or after a `Select()` | [DistinctBy](/documents/querying/linq/operators#distinctby) |
| Raw JSON: `ToJsonArrayAsync()`, `StreamPagedJsonArray()` and `ToJsonFirstWithVersionAsync()` | [Querying for Raw JSON](/documents/querying/query-json) |

### Grouping and Joins {#supported-grouping-and-joins}

| Shape | Example |
| --- | --- |
| `GroupBy()` on one or more keys, with `Count()`, `LongCount()`, `Sum()`, `Min()`, `Max()` and `Average()` | [GroupBy](/documents/querying/linq/operators#groupby) |
| `Where()` after `GroupBy()`, as `HAVING` | [HAVING](/documents/querying/linq/operators#having-where-after-groupby) |
| Ordering, paging and counting groups | [Ordering and Paging Groups](/documents/querying/linq/operators#ordering-and-paging-groups) |
| `GroupJoin()` followed by `SelectMany()`, as an inner or left join | [Joins](#joins) |
| `Join()`, `LeftJoin()` and `RightJoin()` | [Joins](#joins) |

### Not Supported {#not-supported}

| Shape | What happens | Do this instead |
| --- | --- | --- |
| Synchronous execution: `ToList()`, `ToArray()`, `foreach`, `First()`, `Count()`, `Any()`, `Sum()`, and anything else that enumerates the query, including .NET 9's `CountBy()`, `AggregateBy()` and `Index()` | `NotSupportedException` | `ToListAsync()` and the other `*Async()` operators, or `ToAsyncEnumerable()`. See [CountBy(), AggregateBy(), and Index()](/documents/querying/linq/operators#countby-aggregateby-and-index) |
| `ToAsyncEnumerable()` in a file without `using Polecat.Linq;` | Binds to .NET 10's `AsyncEnumerable.ToAsyncEnumerable()`, which enumerates the query synchronously: `NotSupportedException` | Add `using Polecat.Linq;` |
| `Include()` | Polecat has no `Include()` | Load the related documents with a second query, or both in one [batched query](/documents/querying/batched-queries) |
| `Order()`, `OrderDescending()`, `Reverse()`, `TakeWhile()`, `SkipWhile()`, `Concat()`, `Union()`, `Except()`, `Zip()` and `DefaultIfEmpty()` outside a join | `BadLinqExpressionException` | Load the rows with `ToListAsync()` and apply the operator in memory, or use `session.AdvancedSql.QueryAsync<T>(sql)` |
| A narrowing `OfType<TSubClass>()` | `BadLinqExpressionException` | `Query<TSubClass>()` |
| A method call in `Where()` Polecat has no translation for, such as `SequenceEqual()`, `Regex.IsMatch()`, `CompareTo()`, `string.Compare()`, or `StartsWith()`, `EndsWith()` or `Contains()` with a `StringComparison` | `BadLinqExpressionException`, naming the calls that do translate | Rewrite it over the calls in the tables above, or use `session.AdvancedSql.QueryAsync<T>(sql)` |
| `Any()`, `Any(predicate)`, `All()` and the `Count()` method on a child collection | `BadLinqExpressionException` | The `Count` property, `IsEmpty()` or `Contains()`, or raw SQL with `OPENJSON`. See [Querying within Child Collections](/documents/querying/linq/child-collections) |
| A computed `OrderBy()` key, or a computed `Select()` followed by another operator, that SQL Server would not compute the way .NET does: a `Math` call, `? :`, `??`, `Substring()`, decimal division, a narrowing cast, floating point `%` | `BadLinqExpressionException` | Load the rows and order or compute in memory. See [Computed keys](/documents/querying/linq/operators#computed-keys) |
| An ordering with an `IComparer<T>` on a key that reads the document | `BadLinqExpressionException` | Rely on the database collation, or order in memory. See [Ordering](/documents/querying/linq/operators#ordering) |
| `OrderBy()`, `GroupBy()` or `DistinctBy()` on a member stored as a JSON object or array | `BadLinqExpressionException` | Use one of its members |
| A member of a .NET type other than those under [Members of .NET Types](#members-of-net-types), such as `Ticks`, `TotalHours`, `Keys` or `Capacity` | `BadLinqExpressionException` | Store the value you need as a member of the document |
| `Select()`, `OrderBy()`, `Take()`, `Skip()` or `Distinct()` before a `GroupBy()` | `BadLinqExpressionException` | Only `Where()` goes in front of a `GroupBy()` |
| A second `SelectMany()`, or one after `Select()`, `GroupBy()` or `Distinct()` | `BadLinqExpressionException` | One `SelectMany()` per query, straight off the documents |
| `FullJoin()`, and the join shapes listed under [Joins](#joins) | `BadLinqExpressionException` | Query each side and combine in memory, or use `session.AdvancedSql.QueryAsync<T>(sql)` |
| `GroupJoin()` as the last operator | `BadLinqExpressionException` | Follow it with `SelectMany()`, or use `Join()` or `LeftJoin()` |
| A negative index, `x.Values[-1]` | `BadLinqExpressionException` | Index from the start of the array |
| `ToJsonArrayAsync()` and the other raw JSON calls over a `Select()` that is not a simple member projection | `BadLinqExpressionException` | `ToListAsync()`, which runs the projection in memory |
| Union shapes Polecat cannot tell apart in the stored JSON | `BadLinqExpressionException` | See [Querying C# Union Members](#union-members) |

## Basic Queries {#basic-queries}

<!-- snippet: sample_polecat_linq_basic_queries -->
<a id='snippet-sample_polecat_linq_basic_queries'></a>
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

// Aggregates
var count = await session.Query<User>().CountAsync();
var any = await session.Query<User>().AnyAsync(x => x.Internal);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L92-L111' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_basic_queries' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Paging

<!-- snippet: sample_polecat_linq_paging -->
<a id='snippet-sample_polecat_linq_paging'></a>
```cs
var page = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .Skip(20)
    .Take(10)
    .ToListAsync();

// Or with the page count and total, in one call
var pagedList = await session.Query<User>()
    .OrderBy(x => x.LastName)
    .ToPagedListAsync(pageNumber: 2, pageSize: 10);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L117-L128' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_paging' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

An aggregate after `Skip()` or `Take()` is computed over the page, as it is in LINQ to Objects. See
[Paging](/documents/querying/linq/paging) for more details.

## Streaming Results

`ToAsyncEnumerable()` runs the query and hands its rows out as they are read, so a large result set is
never held in memory all at once:

<!-- snippet: sample_polecat_linq_streaming_results -->
<a id='snippet-sample_polecat_linq_streaming_results'></a>
```cs
await foreach (var user in session.Query<User>()
                   .Where(x => x.Internal)
                   .OrderBy(x => x.LastName)
                   .ToAsyncEnumerable(cancellationToken))
{
    await writer.WriteLineAsync($"{user.LastName},{user.FirstName}");
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L134-L142' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_streaming_results' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

- The query runs when enumeration starts, not when `ToAsyncEnumerable()` is called.
- The session's connection stays busy until the enumeration ends or is disposed. Finish the `await
  foreach` (or break out of it) before using the session for anything else.
- Whole documents, scalar and projected `Select()`s, `Distinct()` and `GroupBy()` stream row by row. A
  join (`GroupJoin`, `Join`, `LeftJoin`, `RightJoin`), and the event-store queryables (`QueryAllRawEvents()`, `QueryRawEventDataOnly<T>()`), read
  their whole result first and then hand it out.
- Opening the query goes through the store's resilience pipeline. A failure part-way through is not
  retried, because rows have already been handed out.

Import `Polecat.Linq`. Without it, .NET 10's `System.Linq.AsyncEnumerable.ToAsyncEnumerable()` binds
instead and fails, because Polecat has no synchronous execution.

## How It Works

LINQ queries are translated to SQL using `JSON_VALUE()` to extract properties from the JSON document:

```sql
SELECT data FROM pc_doc_user
WHERE JSON_VALUE(data, '$.lastName') = @p0
ORDER BY JSON_VALUE(data, '$.firstName')
```

[What Polecat Supports](#what-polecat-supports) lists what translates, and
[Supported LINQ Operators](/documents/querying/linq/operators) has the examples.

## When a Query Cannot Be Translated {#when-a-query-cannot-be-translated}

A query Polecat cannot turn into SQL is **refused**, with `JasperFx.BadLinqExpressionException` —
the shared type, so store-agnostic code catches the same exception on Polecat and Fisher alike.

The refusal is a correctness guarantee rather than an inconvenience: the alternative to throwing is a
query that returns plausible but wrong rows. The message names what *is* accepted in that position,
and the ways through are:

- rewrite the condition over document members, which is what translates;
- run it as raw SQL through `session.AdvancedSql.QueryAsync<T>(...)`;
- materialize the query and finish the work in memory with LINQ-to-Objects.

An **operator** Polecat cannot translate is refused the same way. `Order()`, `OrderDescending()`,
`Reverse()`, `TakeWhile()`, `SkipWhile()`, `Union()`, `Concat()`, `Except()`, `FullJoin()` and
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

## Joins {#joins}

`Join()`, and .NET 10's `LeftJoin()` and `RightJoin()`, translate to one SQL `JOIN` of two document
tables. Each is the `GroupJoin(...).SelectMany(...)` (+ `DefaultIfEmpty()`) form written for you, so the two
spellings answer the same; `RightJoin()` is the left join with its two sources swapped.

<!-- snippet: sample_polecat_linq_left_join -->
<a id='snippet-sample_polecat_linq_left_join'></a>
```cs
var rows = await session.Query<Customer>()
    .Where(c => c.City == "Seattle")
    .LeftJoin(session.Query<Order>(), c => c.Id, o => o.CustomerId,
        (c, o) => new { c.Name, Amount = o == null ? (decimal?)null : o.Amount })
    .OrderBy(x => x.Name)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L161-L168' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_left_join' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`Where()`, `OrderBy()`, `Skip()`/`Take()`, `First()`, `Single()`, `Count()`, `Any()`, `Distinct()` and
`Select()` can follow the join. The result selector runs in memory, so on a left or right join test the
side that may be missing for `null`, as above. A join is refused, naming the operator, when the GroupJoin
path cannot answer it exactly: an operator other than `Where()` before the join on either source, an inner
source that is not a filtered `session.Query<T>()`, a composite or computed key, the `IEqualityComparer`
overload, a join of a join, a `Where()` after a left join on the side that may have no match (it would
filter before joining and keep the rows it should remove), and `AnyTenant()`/`TenantIsOneOf()` on the
outer source of a `RightJoin()` (put it on the inner source, where it covers both sides). `FullJoin()` is
not translated.

The `GroupJoin(...).SelectMany(...)` form can be written by hand too, with `DefaultIfEmpty()` for a left join.
A `GroupJoin()` that isn't followed by a `SelectMany()` is refused:

<!-- snippet: sample_polecat_linq_group_join -->
<a id='snippet-sample_polecat_linq_group_join'></a>
```cs
// An inner join
var inner = await session.Query<Customer>()
    .GroupJoin(session.Query<Order>(), c => c.Id, o => o.CustomerId, (c, orders) => new { c, orders })
    .SelectMany(x => x.orders, (x, o) => new { x.c.Name, o.Amount })
    .ToListAsync();

// A left join: DefaultIfEmpty() keeps the customers with no orders
var left = await session.Query<Customer>()
    .GroupJoin(session.Query<Order>(), c => c.Id, o => o.CustomerId, (c, orders) => new { c, orders })
    .SelectMany(x => x.orders.DefaultIfEmpty(), (x, o) => new { x.c.Name, Amount = o == null ? 0m : o.Amount })
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L174-L186' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_group_join' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

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
of a collection class of your own, which can mean anything. A member of something that is not stored is
refused too: `OrderBy(x => x.Name.Trim().Length)`. Each of these used to read a JSON path that could never
exist and quietly match nothing. A value that reads nothing from the document, `OrderBy(x => captured.Name)`,
is worked out before the query runs instead, and `.Select(x => x.Name).Where(n => n.Length > 3)` reads the
`Length` of the stored name through the projection.

A complex `Select` that materializes through `ToListAsync()` is not affected, because it runs in memory:
`Select(x => new { x.CreatedAt.Ticks })` works, and only the JSON streaming calls refuse it.

## Operators after a Select {#operators-after-a-select}

An operator written after a `Select` reads what the `Select` projected, as it does in LINQ-to-Objects:

<!-- snippet: sample_polecat_linq_operators_after_select -->
<a id='snippet-sample_polecat_linq_operators_after_select'></a>
```cs
var cities = await session.Query<Order>()
    .Select(x => x.ShippingAddress)
    .Where(a => a.City != "Oslo")
    .OrderBy(a => a.City)
    .ToListAsync();

var names = await session.Query<User>()
    .Select(x => new { Label = x.LastName })
    .Where(y => y.Label.StartsWith("Sm"))
    .OrderBy(y => y.Label)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L192-L204' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_operators_after_select' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

That works for a member the projection assigned, in an anonymous type or an object initializer, and it
means what writing the same expression against the document would mean. Where the tree does not say what a
member holds, it is refused: a member a `new Dto { ... }` left unset, because no row holds its default, and
any member of a constructor call like `new Dto(x.Name)`.

A projected scalar is read the same way, computed ones included: `Select(x => x.Number * 2).Where(n => n > 4)`
filters on `x.Number * 2 > 4`, `Select(x => x.Name).OrderBy(n => n.Length)` orders by the name's length, and
`Select(x => x.Number % 3).Distinct()`, `.Sum()`, `.Min()`, `.Max()` and `.Average()` compute the projected
value in SQL. A computed value is translated by the same exact rules as a
[computed ordering key](/documents/querying/linq/operators#computed-keys), and refused where it has none. A
plain computed `Select` with nothing after it is still computed in memory, by .NET.

A second `Select` reads the first in the same way.

## Captured Values and Optional Filters {#optional-filters}

Values with no document in them, such as a captured local, a static or a member of either, are worked out
before the query runs, and bound as parameters. That is what makes the usual optional filter work. The right
side of a decided `||` or `&&` is never evaluated, exactly as in C#:

<!-- snippet: sample_polecat_linq_optional_filter -->
<a id='snippet-sample_polecat_linq_optional_filter'></a>
```cs
int? excluded = null;

var results = await session.Query<User>()
    .Where(x => !excluded.HasValue || x.Age != excluded.Value)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L149-L155' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_optional_filter' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Querying Across Tenants {#querying-across-tenants}

Under conjoined tenancy, every query is limited to the session's tenant. `AnyTenant()` lifts that, and
`TenantIsOneOf()` replaces it with a list of tenants:

<!-- snippet: sample_polecat_linq_across_tenants -->
<a id='snippet-sample_polecat_linq_across_tenants'></a>
```cs
await using var session = store.QuerySession(new SessionOptions { TenantId = "tenant-a" });

// Every tenant's documents, not only tenant-a's
var everyone = await session.Query<User>()
    .AnyTenant()
    .ToListAsync();

// Only these tenants' documents
var some = await session.Query<User>()
    .TenantIsOneOf("tenant-a", "tenant-c")
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L210-L222' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_across_tenants' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A single-tenant document table has no `tenant_id` column, so on a store without conjoined tenancy neither one
filters anything. See [Multi-Tenanted Documents](/documents/multi-tenancy).

## Querying C# Union Members {#union-members}

On .NET 11, with C# 15 unions, a document member can be a union. The compiler refuses a union pattern in an
expression tree (`x.Payment is Card` is CS8122), so a query reaches the active case through the union's `Value`:
a type test, a member of a case through a cast or `as`, and a null test, which is true of a default union. They
work in `Where()`, `OrderBy()` and `Select()`:

<!-- snippet: sample_polecat_linq_union_members -->
<a id='snippet-sample_polecat_linq_union_members'></a>
```cs
// Which case the union holds
var paidByCard = await session.Query<UnionOrder>()
    .Where(x => x.Payment.Value is UnionCard)
    .ToListAsync();

// A member of one case, through a cast or as
var oneCard = await session.Query<UnionOrder>()
    .Where(x => ((UnionCard)x.Payment.Value!).Last4 == "5678")
    .ToListAsync();

var bigCash = await session.Query<UnionOrder>()
    .Where(x => (x.Payment.Value as UnionCash)!.Amount > 50m)
    .OrderByDescending(x => ((UnionCash)x.Payment.Value!).Amount)
    .ToListAsync();

// A default union holds nothing
var unpaid = await session.Query<UnionOrder>()
    .Where(x => x.Payment.Value == null)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.EventSourcingTests/Unions/union_documentation_samples.cs#L11-L31' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_union_members' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A primitive case is read as itself: `(int)x.Code.Value > 5`. Polecat refuses with `BadLinqExpressionException`,
rather than guessing, a type that is not one of the union's cases, cases the stored JSON can't tell apart
(identical shapes, two cases written as the same JSON kind, or cases told apart only by nullable properties), a
nested union case, a union compared to a value or ordered or grouped by, and a type test on anything but a union's
`Value`. On .NET 10 no type is a union, so none of this applies.
