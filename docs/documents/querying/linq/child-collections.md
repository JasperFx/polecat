# Querying within Child Collections

A collection is stored as a JSON array inside the document. These are the questions Polecat can ask of
one in a predicate:

| Query | SQL |
|---|---|
| `x.Tags.Contains("admin")` | `OPENJSON` over the array |
| `x.Tags.Count`, `x.Scores.Length` | `SELECT COUNT(*) FROM OPENJSON(...)` |
| `x.Tags.IsEmpty()` | the same count, against `0` |
| `x.Tags == null`, `x.Tags != null` | whether the array is there at all |

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
