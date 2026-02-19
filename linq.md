# Polecat LINQ Support

Polecat provides a LINQ querying interface that mirrors Marten's API, translating C# expression trees into SQL Server 2025 queries using `JSON_VALUE()` and `OPENJSON()` for JSON property access.

## Getting Started

```csharp
await using var session = store.QuerySession();

var results = await session.Query<User>()
    .Where(x => x.Age > 21)
    .OrderBy(x => x.Name)
    .ToListAsync();
```

All queries are **async-only** — synchronous execution is not supported.

## Supported Operators

### Filtering (Where)

```csharp
// Equality
.Where(x => x.Name == "Alice")

// Comparison operators: ==, !=, <, >, <=, >=
.Where(x => x.Age > 21)

// Boolean properties
.Where(x => x.IsActive)
.Where(x => !x.IsActive)

// Null checks
.Where(x => x.Name == null)
.Where(x => x.Name != null)

// Logical AND / OR
.Where(x => x.Age > 21 && x.IsActive)
.Where(x => x.Name == "Alice" || x.Name == "Bob")

// Nested properties
.Where(x => x.Address.City == "New York")

// Enum properties
.Where(x => x.Color == TargetColor.Red)

// Closure variables
var minAge = 21;
.Where(x => x.Age >= minAge)

// Chained Where (AND)
.Where(x => x.Age > 21).Where(x => x.IsActive)
```

**SQL Mapping:**

| C# Expression | SQL Server Translation |
|---|---|
| `x.Name == "foo"` | `JSON_VALUE(data, '$.name') = @p0` |
| `x.Age > 21` | `CAST(JSON_VALUE(data, '$.age') AS int) > @p0` |
| `x.IsActive` | `JSON_VALUE(data, '$.isActive') = 'true'` |
| `x.Id == guid` | `id = @p0` (uses id column directly) |
| `x.Address.City == "NY"` | `JSON_VALUE(data, '$.address.city') = @p0` |
| `x.Name == null` | `JSON_VALUE(data, '$.name') IS NULL` |

### Ordering

```csharp
.OrderBy(x => x.Name)
.OrderByDescending(x => x.Age)
.OrderBy(x => x.Name).ThenBy(x => x.Age)
.OrderBy(x => x.Name).ThenByDescending(x => x.CreatedAt)
```

### Pagination

```csharp
.Take(10)                   // TOP 10
.Skip(20).Take(10)          // OFFSET 20 FETCH NEXT 10
.OrderBy(x => x.Name).Skip(5).Take(5)  // Paged results
```

### Single Value Operators

```csharp
// First / FirstOrDefault
var user = await session.Query<User>().FirstAsync(x => x.Name == "Alice");
var user = await session.Query<User>().FirstOrDefaultAsync(x => x.Age > 99);

// Single / SingleOrDefault
var user = await session.Query<User>().SingleAsync(x => x.Id == id);

// Last / LastOrDefault (requires OrderBy)
var user = await session.Query<User>().OrderBy(x => x.Age).LastAsync();

// Count / LongCount
var count = await session.Query<User>().CountAsync();
var count = await session.Query<User>().CountAsync(x => x.Age > 21);

// Any
var exists = await session.Query<User>().AnyAsync();
var exists = await session.Query<User>().AnyAsync(x => x.Name == "Alice");
```

### Aggregations

```csharp
var total = await session.Query<User>().SumAsync(x => x.Age);
var min   = await session.Query<User>().MinAsync(x => x.Age);
var max   = await session.Query<User>().MaxAsync(x => x.Age);
var avg   = await session.Query<User>().AverageAsync(x => x.Score);
```

### String Methods

```csharp
.Where(x => x.Name.Contains("ali"))          // LIKE '%ali%'
.Where(x => x.Name.StartsWith("A"))          // LIKE 'A%'
.Where(x => x.Name.EndsWith("ce"))           // LIKE '%ce'
.Where(x => string.IsNullOrEmpty(x.Name))    // IS NULL OR = ''

// Case-insensitive comparison
.Where(x => x.Name.Equals("alice", StringComparison.OrdinalIgnoreCase))
```

### Select Projections

```csharp
// Scalar projection
var names = await session.Query<User>()
    .Select(x => x.Name)
    .ToListAsync();

// Anonymous type
var results = await session.Query<User>()
    .Select(x => new { x.Name, x.Age })
    .ToListAsync();

// DTO mapping
var results = await session.Query<User>()
    .Select(x => new UserDto { Name = x.Name, Age = x.Age })
    .ToListAsync();

// Distinct
var uniqueNames = await session.Query<User>()
    .Select(x => x.Name)
    .Distinct()
    .ToListAsync();
```

### Collection Queries (OPENJSON)

```csharp
// Collection contains a value
.Where(x => x.Tags.Contains("csharp"))
// SQL: @p0 IN (SELECT [value] FROM OPENJSON(data, '$.tags'))

// Collection is empty
.Where(x => x.Tags.IsEmpty())
// SQL: (SELECT COUNT(*) FROM OPENJSON(data, '$.tags')) = 0

// List contains a member value (IN clause)
var names = new List<string> { "Alice", "Bob" };
.Where(x => names.Contains(x.Name))
// SQL: JSON_VALUE(data, '$.name') IN (@p0, @p1)
```

### Polecat Extension Methods

```csharp
// IsOneOf — SQL IN clause
.Where(x => x.Name.IsOneOf("Alice", "Bob", "Charlie"))
.Where(x => x.Color.IsOneOf(Color.Red, Color.Blue))

// In — synonym for IsOneOf
.Where(x => x.Age.In(25, 30, 35))

// IsEmpty — collection empty check
.Where(x => x.Tags.IsEmpty())
```

### Multi-Tenancy Extensions

```csharp
// Query across ALL tenants (removes tenant_id filter)
var allUsers = await session.Query<User>()
    .AnyTenant()
    .ToListAsync();

// Query specific tenants
var results = await session.Query<User>()
    .TenantIsOneOf("tenant-a", "tenant-c")
    .ToListAsync();
```

## Supported Property Types

| C# Type | SQL CAST |
|---|---|
| `string` | (none — `JSON_VALUE` returns `nvarchar`) |
| `int` | `CAST(... AS int)` |
| `long` | `CAST(... AS bigint)` |
| `decimal` | `CAST(... AS decimal(18,6))` |
| `double` | `CAST(... AS float)` |
| `float` | `CAST(... AS real)` |
| `bool` | String comparison (`'true'` / `'false'`) |
| `Guid` | (none — compared as string) |
| `DateTime` | `CAST(... AS datetime2)` |
| `DateTimeOffset` | `CAST(... AS datetimeoffset)` |
| `enum` (AsInteger) | `CAST(... AS int)` |
| `enum` (AsString) | (none — compared as string) |

## Unsupported Features

These Marten LINQ features are **not available** in Polecat:

| Feature | Marten Implementation | Why Not SQL Server |
|---|---|---|
| `IsSupersetOf()` | PostgreSQL `@>` (jsonb containment) | No containment operator; would require element-by-element OPENJSON comparison |
| `IsSubsetOf()` | PostgreSQL `<@` (contained-by) | Same as above |
| Full-text search | `to_tsvector`/`to_tsquery` with `@@` | SQL Server FTS requires full-text indexes on columns, not JSON type |
| GIN-indexed JSON queries | PostgreSQL GIN indexes on jsonb | No equivalent index type for JSON in SQL Server |
| `Include()` (eager loading) | Lateral joins with jsonb | Technically feasible but complex; not yet implemented |
| `SelectMany()` on collections | `jsonb_array_elements()` lateral join | Feasible with `OPENJSON` + `CROSS APPLY`; not yet implemented |
| Compiled queries | `ICompiledQuery<T,TOut>` | Not yet implemented |
| `MatchesSql()` raw fragments | PostgreSQL-specific SQL | Not yet implemented |
| Dictionary member access | `jsonb_path_query` | Feasible; not yet implemented |
| `Nullable<T>.HasValue` | Direct IS NOT NULL | Use `x.Prop != null` instead |
