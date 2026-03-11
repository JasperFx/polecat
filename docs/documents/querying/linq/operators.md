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
- `g.Sum(x => x.Property)` -- `SUM(property)`
- `g.Min(x => x.Property)` -- `MIN(property)`
- `g.Max(x => x.Property)` -- `MAX(property)`
- `g.Average(x => x.Property)` -- `AVG(property)`

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
