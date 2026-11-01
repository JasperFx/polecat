# Supported LINQ Operators

Polecat's LINQ provider supports the following operators and methods.

## Comparison Operators

```cs
.Where(x => x.Age == 25)
.Where(x => x.Age != 25)
.Where(x => x.Age > 18)
.Where(x => x.Age >= 18)
.Where(x => x.Age < 65)
.Where(x => x.Age <= 65)
```

## Boolean Logic

```cs
.Where(x => x.Active && x.Age > 18)
.Where(x => x.Active || x.Admin)
.Where(x => !x.Deleted)
```

## String Operations

```cs
.Where(x => x.Name.Contains("john"))
.Where(x => x.Name.StartsWith("J"))
.Where(x => x.Name.EndsWith("son"))
.Where(x => x.Name == "John")
.Where(x => string.IsNullOrEmpty(x.Name))
```

See [Searching on String Fields](/documents/querying/linq/strings) for more details.

## Null Checks

```cs
.Where(x => x.Email != null)
.Where(x => x.Email == null)
```

## Arithmetic

```cs
.Where(x => x.Price * x.Quantity > 100)
.Where(x => x.Total - x.Discount < 50)
```

## Collection Operations

```cs
// Check if a value is in a list
var ids = new[] { id1, id2, id3 };
.Where(x => ids.Contains(x.Id))

// Check child collections
.Where(x => x.Tags.Any())
.Where(x => x.Tags.Contains("priority"))
```

## Ordering

```cs
.OrderBy(x => x.LastName)
.OrderByDescending(x => x.CreatedDate)
.ThenBy(x => x.FirstName)
.ThenByDescending(x => x.Age)
```

## Projection

```cs
.Select(x => new { x.FirstName, x.LastName })
.Select(x => x.Email)
```

## Aggregation

```cs
.CountAsync()
.LongCountAsync()
.AnyAsync()
.AnyAsync(x => x.Active)
.FirstAsync()
.FirstOrDefaultAsync()
.SingleAsync()
.SingleOrDefaultAsync()
.MinAsync(x => x.Age)
.MaxAsync(x => x.Age)
.SumAsync(x => x.Amount)
.AverageAsync(x => x.Score)
```

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
<sup><a href='https://github.com/JasperFx/polecat/blob/main/src/Polecat.Tests/Linq/group_by_operator.cs#L22-L41' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_polecat_group_by_simple_key_with_count' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

### Composite Key

You can group by multiple properties using an anonymous type:

```cs
var results = await session.Query<LinqTarget>()
    .GroupBy(x => new { x.Color, x.Name })
    .Select(g => new { Color = g.Key.Color, Text = g.Key.Name, Count = g.Count() })
    .ToListAsync();
```

### Where Before GroupBy

Filter documents before grouping with a standard `Where()` clause:

```cs
var results = await session.Query<LinqTarget>()
    .Where(x => x.Age > 20)
    .GroupBy(x => x.Color)
    .Select(g => new { Color = g.Key, Count = g.Count() })
    .ToListAsync();
```

### HAVING (Where After GroupBy)

Filter groups with a `Where()` clause after `GroupBy()` -- this translates to SQL `HAVING`:

```cs
var results = await session.Query<LinqTarget>()
    .GroupBy(x => x.Color)
    .Where(g => g.Count() > 1)
    .Select(g => new { Color = g.Key, Count = g.Count() })
    .ToListAsync();
```

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

```cs
var page = await session.Query<LinqTarget>()
    .GroupBy(x => x.Color)
    .Select(g => new { Color = g.Key, Total = g.Sum(x => x.Age) })
    .OrderByDescending(x => x.Total)
    .Skip(10).Take(10)
    .ToListAsync();
```

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

## CountBy(), AggregateBy(), and Index() (.NET 9)

The `CountBy`, `AggregateBy`, and `Index` operators added in .NET 9 are **not translated to SQL by Polecat**, because .NET only added them to `Enumerable` (there are no `IQueryable` overloads, so a query provider never sees them). Calling them on a Polecat query therefore **runs client-side**: the full result set is pulled into memory first and the operator is applied there with LINQ to Objects -- the same behavior you get with EF Core.

For large tables, express a `CountBy` as `GroupBy(...).Select(...)`, which Polecat does translate to a SQL `GROUP BY`:

```cs
// Translated to SQL: select count(*), color ... group by color
var counts = await session.Query<Target>()
    .GroupBy(x => x.Color)
    .Select(g => new { Color = g.Key, Count = g.Count() })
    .ToListAsync();
```

There is no efficient SQL equivalent for `AggregateBy`'s arbitrary accumulator function; `Index` corresponds to a window `ROW_NUMBER()` but is likewise only an in-memory operator today. If/when .NET ships `Queryable` overloads for these, Polecat can revisit native translation.

## Paging

```cs
.Skip(20)
.Take(10)
.ToPagedListAsync(pageNumber, pageSize)
```

## Result Materialization

```cs
.ToListAsync()
.ToArrayAsync()
.ToJsonArrayAsync()
```
