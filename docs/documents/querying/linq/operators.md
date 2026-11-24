# Supported LINQ Operators

Polecat's LINQ provider supports the following operators and methods. The
[summary](/documents/querying/linq/#what-polecat-supports) on the LINQ landing page lists every shape in one place,
along with what is not supported.

## Comparison Operators

`==`, `!=`, `>`, `>=`, `<` and `<=` work on members of the document, members of members (`x.Address.City`), and
between two members:

<!-- snippet: sample_polecat_linq_comparison_operators -->
<a id='snippet-sample_polecat_linq_comparison_operators'></a>
```cs
var adults = await session.Query<User>()
    .Where(x => x.Age >= 18 && x.Age < 65)
    .ToListAsync();

var notTwentyFive = await session.Query<User>()
    .Where(x => x.Age != 25)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L230-L238' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_comparison_operators' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Boolean Logic

<!-- snippet: sample_polecat_linq_boolean_logic -->
<a id='snippet-sample_polecat_linq_boolean_logic'></a>
```cs
var activeAdults = await session.Query<User>()
    .Where(x => x.Active && x.Age > 18)
    .ToListAsync();

var activeOrAdmin = await session.Query<User>()
    .Where(x => x.Active || x.Admin)
    .ToListAsync();

var inactive = await session.Query<User>()
    .Where(x => !x.Active)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L245-L257' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_boolean_logic' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`x.Flag == false` and `x.Flag != true` work too.

## String Operations

`StartsWith()`, `EndsWith()`, `Contains()`, `Equals()`, `string.IsNullOrEmpty()`, `string.IsNullOrWhiteSpace()`,
`ToLower()`, `ToUpper()`, `Trim()` and `Length` are translated. See
[Searching on String Fields](/documents/querying/linq/strings).

## Null Checks

<!-- snippet: sample_polecat_linq_null_checks -->
<a id='snippet-sample_polecat_linq_null_checks'></a>
```cs
var withNickname = await session.Query<User>()
    .Where(x => x.Nickname != null)
    .ToListAsync();

var unranked = await session.Query<User>()
    .Where(x => !x.Rank.HasValue)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L264-L272' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_null_checks' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`.HasValue` and `.Value` of a nullable are covered under
[Members of .NET Types](/documents/querying/linq/#members-of-net-types).

## Arithmetic

`+`, `-`, `*`, `/` and `%` work between members and values:

<!-- snippet: sample_polecat_linq_arithmetic -->
<a id='snippet-sample_polecat_linq_arithmetic'></a>
```cs
var bigOrders = await session.Query<Product>()
    .Where(x => x.Price * x.Quantity > 100)
    .ToListAsync();

var discounted = await session.Query<Product>()
    .Where(x => x.Total - x.Discount < 50)
    .ToListAsync();

var everyThird = await session.Query<Product>()
    .Where(x => x.Number % 3 == 0)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L278-L290' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_arithmetic' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Collection Operations

### Contains() over a Local Collection {#contains-over-a-local-collection}

`Contains()` over a local array, `List<T>` or `HashSet<T>` is a SQL `IN`, and `IsOneOf()` and `In()` are the same
thing written from the member's side:

<!-- snippet: sample_polecat_linq_contains_over_a_local_collection -->
<a id='snippet-sample_polecat_linq_contains_over_a_local_collection'></a>
```cs
// A SQL IN over a local array, List<T> or HashSet<T>
var ids = new[] { id1, id2, id3 };
var chosen = await session.Query<User>()
    .Where(x => ids.Contains(x.Id))
    .ToListAsync();

// The same, written the other way round
var named = await session.Query<User>()
    .Where(x => x.LastName.IsOneOf("Smith", "Jones"))
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L296-L307' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_contains_over_a_local_collection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A long list is bound as one JSON array parameter rather than one parameter per value, so it doesn't run into
SQL Server's limit of 2100 parameters to a command.

### Child Collections

`Contains()`, the `Count` and `Length` properties, `IsEmpty()`, an index (`x.Values[1]`) and a dictionary key
(`x.Scores["math"]`) work on a collection the document holds. `Any()`, `All()` and the `Count()` method are not
translated on a child collection. See [Querying within Child Collections](/documents/querying/linq/child-collections).

## Ordering

```cs
.OrderBy(x => x.LastName)
.OrderByDescending(x => x.CreatedDate)
.ThenBy(x => x.FirstName)
.ThenByDescending(x => x.Age)
```

A member stored as a JSON object or array has no value to sort by, so `OrderBy`, `GroupBy` and `DistinctBy`
refuse one with `BadLinqExpressionException`. Use one of its members. A class with a JSON converter that
stores it as a number or a string is ordered by what it stores.

An ordering with an `IComparer<T>`, such as `StringComparer.OrdinalIgnoreCase`, is refused when its key reads
the document, because SQL Server orders by the column's collation, not by the comparer. On the default
case-insensitive collation, a plain `OrderBy()` already ignores case.

### Computed keys

A key computed from the document is translated, as long as SQL Server computes exactly the value .NET would:

<!-- snippet: sample_polecat_linq_computed_ordering_keys -->
<a id='snippet-sample_polecat_linq_computed_ordering_keys'></a>
```cs
var byNegatedNumber = await session.Query<Product>()
    .OrderBy(x => -x.Number)
    .ToListAsync();

var byRemainder = await session.Query<Product>()
    .OrderBy(x => x.Number % 3).ThenBy(x => x.Price - x.Number * 2m)
    .ToListAsync();

var byText = await session.Query<Product>()
    .OrderBy(x => x.Code + x.Name).ThenBy(x => x.Name.ToUpper())
    .ToListAsync();

var byLength = await session.Query<Product>()
    .OrderBy(x => x.Name.Length).ThenByDescending(x => x.Level.ToString())
    .ToListAsync();

var byElement = await session.Query<Product>()
    .OrderBy(x => x.Values[1]).ThenBy(x => x.Children[0].Name)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L313-L333' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_computed_ordering_keys' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

That is negation, arithmetic (with integer division and `%` truncating as they do in .NET), widening casts,
string concatenation, `ToUpper()`, `ToLower()`, `Length`, `ToString()` of an integer or a `Guid`, and indexes
into the document's arrays and lists. A key without an exact translation is refused rather than ordered by
something else: decimal division (SQL Server rounds the scale), `%` over a floating point or decimal value, a
narrowing cast (`(byte)256` is `0`), a conditional, `??`, a `Math` call, `Substring`, and the text of a
`double` or a date. Integer overflow is the one difference: SQL Server raises an error where unchecked .NET
wraps. The same keys translate over the rows of a join and over the child documents a `SelectMany()`
flattens.

A key that reads nothing from the document, such as a literal, a static, a captured local or
`Settings.SortKeys[1]`, is the same for every row, so it orders nothing, as in LINQ-to-Objects. It is dropped,
along with any comparer it carries: `OrderBy(x => sortKey).ThenBy(x => x.Name)` orders by `Name`. A comparer
on a key that does read the document is still refused, because SQL Server orders by its collation.

## Projection

<!-- snippet: sample_polecat_linq_select -->
<a id='snippet-sample_polecat_linq_select'></a>
```cs
var names = await session.Query<User>()
    .Select(x => new { x.FirstName, x.LastName })
    .ToListAsync();

var emails = await session.Query<User>()
    .Select(x => x.Email)
    .ToListAsync();

var firstEmail = await session.Query<User>()
    .Select(x => x.Email)
    .FirstOrDefaultAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L339-L351' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_select' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A member, or a member of a member, is read in SQL. A projection into an anonymous type, a DTO or a record is
built in memory from each document by `ToListAsync()`, so anything you can write in C# works there. The JSON
streaming calls build a simple one in SQL instead: see
[Projecting with Select()](/documents/querying/query-json#projecting-with-select). An operator after the
`Select()` reads what it projected: see [Operators after a Select](/documents/querying/linq/#operators-after-a-select).

### Distinct

<!-- snippet: sample_polecat_linq_distinct -->
<a id='snippet-sample_polecat_linq_distinct'></a>
```cs
var cities = await session.Query<Product>()
    .Select(x => x.City).Distinct()
    .ToListAsync();

var places = await session.Query<Product>()
    .Select(x => new { x.City, x.State }).Distinct().OrderBy(x => x.City)
    .ToListAsync();

var howMany = await session.Query<Product>()
    .Select(x => new { x.City, x.State }).Distinct()
    .CountAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L357-L369' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_distinct' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`Distinct()` runs in SQL, over the scalar a `Select` projected or over the members of an anonymous type or
object initializer. A computed scalar, `Select(x => x.Number % 4).Distinct()`, is translated as an ordering
key is, and compared in SQL. The members of an anonymous type have to be scalar members of the document,
because SQL can only compare columns: a member that is an object, a computed value or a constructor call is
refused with `BadLinqExpressionException`. So is an ordering by something the `Select` did not project, and a `Sum`, `Min`,
`Max` or `Average` after it. Project in memory after `ToListAsync()` for any of those.

::: warning SQL decides what is equal
Rows are compared by the database's collation, so on the default case-insensitive one `abc` and `ABC` are
one row, and so are `abc` and `abc  `, because `=` pads with spaces. Which of the spellings comes back is up
to SQL Server. Adding the length to a `DISTINCT` would mean reading every value twice, so it is left to mean
what it means in SQL.
:::

### DistinctBy

`DistinctBy()` keeps one row per key. On the documents themselves it keeps one whole document per key, the
first one in the query's ordering. After a `Select()` into an anonymous type or a DTO, the key is one of the
projected members:

<!-- snippet: sample_polecat_linq_distinct_by -->
<a id='snippet-sample_polecat_linq_distinct_by'></a>
```cs
// One whole document per city: the first in the ordering wins
var cheapestInEachCity = await session.Query<Product>()
    .OrderBy(x => x.Price)
    .DistinctBy(x => x.City)
    .ToListAsync();

// Or after a projection, on one of its members
var oneNamePerCity = await session.Query<Product>()
    .Select(x => new { x.City, x.Name })
    .DistinctBy(x => x.City)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L375-L387' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_distinct_by' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A computed key is refused with `BadLinqExpressionException`.

## Aggregation

<!-- snippet: sample_polecat_linq_aggregates -->
<a id='snippet-sample_polecat_linq_aggregates'></a>
```cs
var count = await session.Query<User>().CountAsync();
var big = await session.Query<User>().LongCountAsync();
var anyActive = await session.Query<User>().AnyAsync(x => x.Active);

var first = await session.Query<User>().OrderBy(x => x.Age).FirstAsync();
var single = await session.Query<User>().Where(x => x.Email == "alice@example.com").SingleOrDefaultAsync();
var oldest = await session.Query<User>().OrderBy(x => x.Age).LastAsync();

var youngest = await session.Query<User>().MinAsync(x => x.Age);
var eldest = await session.Query<User>().MaxAsync(x => x.Age);
var totalAge = await session.Query<User>().SumAsync(x => x.Age);
var averageAge = await session.Query<User>().AverageAsync(x => x.Age);
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L393-L406' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_aggregates' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

`FirstAsync()`, `FirstOrDefaultAsync()`, `LastAsync()`, `CountAsync()` and `AnyAsync()` take an inline
predicate. `SingleAsync()`, `SingleOrDefaultAsync()` and `LastOrDefaultAsync()` don't: put a `Where()` in front of
them. `LastAsync()` and `LastOrDefaultAsync()` read the last row of the query's ordering, so give the query one.

## GroupBy

Polecat supports the `GroupBy()` LINQ operator for grouping documents by one or more keys and computing aggregate values. GroupBy translates to SQL `GROUP BY` with aggregate functions like `COUNT`, `SUM`, `MIN`, `MAX`, and `AVG`.

### Simple Key with Aggregates

<!-- snippet: sample_polecat_group_by_simple_key_with_count -->
<a id='snippet-sample_polecat_group_by_simple_key_with_count'></a>
```cs
[Fact]
public async Task group_by_simple_key_with_count()
{
    await StoreSeedDataAsync();

    await using var query = theStore.QuerySession();
    var results = await query.Query<LinqTarget>()
        .GroupBy(x => x.Color)
        .Select(g => new { Color = g.Key, Count = g.Count() })
        .ToListAsync(TestContext.Current.CancellationToken);

    results.Count.ShouldBe(3);
    results.Single(x => x.Color == TargetColor.Blue).Count.ShouldBe(2);
    results.Single(x => x.Color == TargetColor.Green).Count.ShouldBe(3);
    results.Single(x => x.Color == TargetColor.Red).Count.ShouldBe(1);
}
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/group_by_operator.cs#L22-L41' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_group_by_simple_key_with_count' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A string key is compared by the database's collation, so on the default case-insensitive one `abc` and `ABC`
are one group. Trailing spaces do count: `abc` and `abc  ` are two groups, as they are in .NET.

### Composite Key

You can group by multiple properties using an anonymous type:

<!-- snippet: sample_polecat_linq_group_by_composite_key -->
<a id='snippet-sample_polecat_linq_group_by_composite_key'></a>
```cs
var results = await session.Query<LinqTarget>()
    .GroupBy(x => new { x.Color, x.Name })
    .Select(g => new { Color = g.Key.Color, Text = g.Key.Name, Count = g.Count() })
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L412-L417' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_group_by_composite_key' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### Where Before GroupBy

Filter documents before grouping with a standard `Where()` clause:

<!-- snippet: sample_polecat_linq_group_by_where_before -->
<a id='snippet-sample_polecat_linq_group_by_where_before'></a>
```cs
var results = await session.Query<LinqTarget>()
    .Where(x => x.Age > 20)
    .GroupBy(x => x.Color)
    .Select(g => new { Color = g.Key, Count = g.Count() })
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L423-L429' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_group_by_where_before' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### HAVING (Where After GroupBy)

Filter groups with a `Where()` clause after `GroupBy()` -- this translates to SQL `HAVING`:

<!-- snippet: sample_polecat_linq_group_by_having -->
<a id='snippet-sample_polecat_linq_group_by_having'></a>
```cs
// The Where() after GroupBy() becomes a SQL HAVING clause
var results = await session.Query<LinqTarget>()
    .GroupBy(x => x.Color)
    .Where(g => g.Count() > 1)
    .Select(g => new { Color = g.Key, Count = g.Count() })
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L435-L442' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_group_by_having' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### Supported Aggregates

- `g.Count()` / `g.LongCount()` -- `COUNT(*)`
- `g.Count(x => x.Age > 30)` / `g.LongCount(...)` -- `COUNT(CASE WHEN ... THEN 1 END)`, with the predicate's values bound as parameters
- `g.Sum(x => x.Property)` -- `SUM(property)`
- `g.Min(x => x.Property)` -- `MIN(property)`
- `g.Max(x => x.Property)` -- `MAX(property)`
- `g.Average(x => x.Property)` -- `AVG(property)`

Any of these, and `g.Key`, can be used in a `Select`, in a `Where` after the `GroupBy`, and in an ordering.

### Ordering and Paging Groups

An ordering after `GroupBy` can address the group, or the member of the `Select` projection, or the
scalar a `Select` produced, and `Skip` and `Take` page the groups:

<!-- snippet: sample_polecat_linq_group_by_ordering_and_paging -->
<a id='snippet-sample_polecat_linq_group_by_ordering_and_paging'></a>
```cs
var page = await session.Query<LinqTarget>()
    .GroupBy(x => x.Color)
    .Select(g => new { Color = g.Key, Total = g.Sum(x => x.Age) })
    .OrderByDescending(x => x.Total)
    .Skip(10).Take(10)
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L448-L455' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_group_by_ordering_and_paging' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

A `Skip` with no ordering pages by the group key. `Select`, `OrderBy`, `Take`, `Skip` and `Distinct` are
refused in front of a `GroupBy`, because SQL Server groups the filtered rows first and they could not mean
what the LINQ says. Only `Where` goes there.

### Terminal Operators

`CountAsync` and `LongCountAsync` count the groups, not the rows, and `AnyAsync` asks whether there are
any. Those three do not need a `Select`. `FirstAsync`, `SingleAsync`, `LastAsync` and their `OrDefault`
forms return one group's projection, and `SumAsync`, `MinAsync`, `MaxAsync` and `AverageAsync` aggregate a
projected value across the groups. An inline predicate filters the groups, as a `Where` there does.

`ToJsonArrayAsync` streams a projected object directly. The other raw JSON operations read whole documents
and refuse a `GroupBy`. A `Select` after the `Select` is refused, as is `DistinctBy`.

`GroupBy` needs `JSON_OBJECT`, so SQL Server 2022 or later, for any projection into an object.

## CountBy(), AggregateBy(), and Index() (.NET 9) {#countby-aggregateby-and-index}

The `CountBy`, `AggregateBy`, and `Index` operators added in .NET 9 are **not supported** on a Polecat query.
.NET only added them to `Enumerable`, with no `IQueryable` overloads, so a query provider never sees them, and
calling one on a Polecat query enumerates the query synchronously, which throws `NotSupportedException`. They
do not quietly run in memory.

Express a `CountBy` as `GroupBy(...).Select(...)`, which Polecat translates to a SQL `GROUP BY`:

<!-- snippet: sample_polecat_linq_count_by_as_group_by -->
<a id='snippet-sample_polecat_linq_count_by_as_group_by'></a>
```cs
// Translated to SQL: SELECT color, COUNT(*) ... GROUP BY color
var counts = await session.Query<LinqTarget>()
    .GroupBy(x => x.Color)
    .Select(g => new { Color = g.Key, Count = g.Count() })
    .ToListAsync();
```
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.LinqTests/Linq/linq_documentation_samples.cs#L461-L467' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_linq_count_by_as_group_by' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

There is no SQL equivalent for `AggregateBy`'s arbitrary accumulator function. For it, or for `Index`, load the
rows with `ToListAsync()` and apply the operator to the list.

## Paging

```cs
.Skip(20)
.Take(10)
.ToPagedListAsync(pageNumber, pageSize)
.ToJsonPageByCursorAsync(cursor, pageSize)
```

See [Paging](/documents/querying/linq/paging).

## Result Materialization

```cs
.ToListAsync()
.ToAsyncEnumerable()
.ToJsonArrayAsync()
```

`ToListAsync()` returns an `IReadOnlyList<T>`. There is no `ToArrayAsync()`.
[`ToAsyncEnumerable()`](/documents/querying/linq/#streaming-results) hands the rows out as they are read, and
the raw JSON calls are covered in [Querying for Raw JSON](/documents/querying/query-json).
