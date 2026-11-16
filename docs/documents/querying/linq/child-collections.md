# Querying within Child Collections

A collection is stored as a JSON array inside the document. These are the questions Polecat can ask of
one in a predicate:

| Query | SQL |
|---|---|
| `x.Tags.Contains("admin")` | `OPENJSON` over the array |
| `x.Tags.Count`, `x.Scores.Length` | `SELECT COUNT(*) FROM OPENJSON(...)` |
| `x.Tags.IsEmpty()` | the same count, against `0` |
| `x.Tags == null`, `x.Tags != null` | whether the array is there at all |

## Indexing

An index into an array or a list, or a key of a dictionary, is read as a JSON path anywhere a member can be:

```cs
.Where(x => x.Values[1] == 3)
.Where(x => x.Grid[1][0] > 6 && x.Children[0].Name == "first")
.OrderBy(x => x.Values.ElementAt(2))
.Select(x => x.Scores["math"])
```

The index has to be known before the query runs: a constant, a captured local or a static. One past the end
reads as missing, so it matches no comparison and a scalar `Select` of it reads the type's default, where
LINQ-to-Objects would throw. A negative index is refused.

## SelectMany

`SelectMany()` flattens a collection the documents hold, of child documents or of values, enums and strings:

```cs
var lines = await session.Query<Order>()
    .Where(x => x.Status == "open")
    .OrderBy(x => x.Number).Take(10)
    .SelectMany(x => x.Lines)
    .Where(l => l.Quantity > 1)
    .ToListAsync();

var tags = await session.Query<Post>().SelectMany(x => x.Tags).Distinct().ToListAsync();

var rows = await session.Query<Order>()
    .SelectMany(x => x.Lines, (x, l) => new { x.Number, l.Sku })
    .ToListAsync();
```

It is `CROSS APPLY OPENJSON(data, '$.lines')`. Everything before it chooses the parents, so a `Take()` or
`Skip()` in front of it pages the documents, and one after it pages the flattened rows. The rows come back in
LINQ's order: the parents' ordering, then each element's position in its collection. `Where`, `OrderBy`,
`Select`, `Distinct` (of a scalar), `First`, `Single`, `Count`, `Any` and the aggregates all work after it.
One `SelectMany()` is translated per query, and not after a `Select`, `GroupBy` or `Distinct`. The JSON
streaming, cursor paging and batched query paths refuse it, because its rows are not documents.

::: warning `Any`, `All` and `Count()` are not translated
`x.Items.Any()`, `x.Items.Any(i => i.Quantity > 10)`, `x.Items.All(...)` and the `x.Items.Count()` method
are all refused with a `BadLinqExpressionException`, rather than being run against nothing. So are
`OrderBy` or `Select` over a child's members. For "has any", use the `Count` property or `IsEmpty()`. To
filter on a child's members, run the query as raw SQL through `session.AdvancedSql.QueryAsync<T>(...)`
with `OPENJSON`, or materialize the query and finish the work in memory.
:::

## Contains

Check if a simple collection contains a value:

```cs
public class User
{
    public Guid Id { get; set; }
    public List<string> Tags { get; set; } = new();
}

var admins = await session.Query<User>()
    .Where(x => x.Tags.Contains("admin"))
    .ToListAsync();
```

The value is compared with each element the way `==` compares strings: all of it, under the database's
collation.

## Count

`Count` and `Length` are the collection's own properties, and they translate wherever a member can be
written, apart from a `GroupBy` key or an aggregate over a group. See
[Members of .NET Types](/documents/querying/linq/#members-of-net-types).

```cs
var busy = await session.Query<User>()
    .Where(x => x.Tags.Count > 2)
    .ToListAsync();
```

## IsEmpty

```cs
var untagged = await session.Query<User>()
    .Where(x => x.Tags.IsEmpty())
    .ToListAsync();
```

A null collection counts as empty here. `x.Tags == null` tells the two apart.

## Nested Property Access

An object inside the document is not a collection, and its members can be queried directly:

```cs
public class Order
{
    public Guid Id { get; set; }
    public Address ShippingAddress { get; set; } = new();
}

var nyOrders = await session.Query<Order>()
    .Where(x => x.ShippingAddress.City == "New York")
    .ToListAsync();
```
