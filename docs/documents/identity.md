# Document Identity

Every document in Polecat must have a unique identity. Polecat supports several identity strategies.

## Supported ID Types

### Guid (Default)

```cs
public class User
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}
```

When `Id` is `Guid.Empty`, Polecat will automatically assign a new `Guid` on `Store()`.

### String

```cs
public class UserByEmail
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
}
```

String IDs must be assigned by the application before storing.

### Int / Long with HiLo

```cs
public class Invoice
{
    public int Id { get; set; }
    public decimal Amount { get; set; }
}
```

Numeric IDs are automatically assigned using the [HiLo algorithm](#hilo-sequences).

## Strongly Typed IDs

Polecat supports [strong typed identifiers](https://en.wikipedia.org/wiki/Strongly_typed_identifier) using immutable `struct` types that wrap one of the supported primitive ID types (`Guid`, `string`, `int`, or `long`).

### Supported Patterns

Polecat automatically detects wrapper types via JasperFx's `ValueTypeInfo`. Two patterns are supported:

**1. Record struct with constructor (recommended):**

```cs
public record struct OrderId(Guid Value);

public class Order
{
    public OrderId Id { get; set; }
    public string Name { get; set; } = "";
}
```

**2. Struct with static builder method:**

```cs
public readonly struct TaskId
{
    private TaskId(Guid value) => Value = value;
    public Guid Value { get; }
    public static TaskId From(Guid value) => new TaskId(value);
}

public class TaskDoc
{
    public TaskId Id { get; set; }
    public string Title { get; set; } = "";
}
```

These patterns are compatible with libraries like [Vogen](https://github.com/SteveDunn/Vogen) and [StronglyTypedId](https://github.com/andrewlock/StronglyTypedId).

### Supported Inner Types

| Wrapper Pattern | ID Generation |
|---|---|
| `record struct InvoiceId(Guid Value)` | Auto-assigned sequential Guid |
| `record struct OrderItemId(int Value)` | HiLo sequence |
| `record struct IssueId(long Value)` | HiLo sequence |
| `record struct TeamId(string Value)` | Manual assignment required |

### Usage

Strong-typed IDs work transparently with all Polecat operations:

```cs
// Store with auto-assigned Guid wrapper
var order = new Order { Name = "Widget" };
session.Store(order);
await session.SaveChangesAsync();
// order.Id is now assigned

// Load by inner value
var loaded = await query.LoadAsync<Order>(order.Id.Value);

// LINQ queries work with the wrapper type directly
var result = await query.Query<Order>()
    .Where(x => x.Id == order.Id)
    .FirstOrDefaultAsync();

// IsOneOf for multiple IDs
var results = await query.Query<Order>()
    .Where(x => x.Id.IsOneOf(id1, id2, id3))
    .ToListAsync();

// Delete by inner value
session.Delete<Order>(order.Id.Value);

// Check existence
var exists = await query.CheckExistsAsync<Order>(order.Id.Value);
```

All of the following operations are supported:

- `Store()` / `Insert()` / `Update()` with automatic ID assignment
- `LoadAsync()` by inner value
- `Delete()` by inner value or by document
- `CheckExistsAsync()` by inner value
- LINQ `Where`, `OrderBy`, `IsOneOf`
- Identity map sessions
- Bulk insert via `BulkInsertAsync()`
- Batch queries

::: tip
For strongly typed Guid IDs, Polecat will auto-assign the inner Guid value if it's empty, just like regular Guid IDs.
:::

## HiLo Sequences

For `int` and `long` ID types, Polecat uses the HiLo algorithm to generate unique IDs efficiently without round-tripping to the database for every insert.

### How It Works

1. The application reserves a block of IDs (the "Hi" value) from the `pc_hilo` table
2. IDs within the block are assigned sequentially in memory (the "Lo" values)
3. When the block is exhausted, a new "Hi" value is reserved

### Configuration

```cs
// Global defaults
opts.HiloSequenceDefaults.MaxLo = 500; // default is 1000

// Per-document type via attribute
[HiloSequence(MaxLo = 100)]
public class Invoice
{
    public int Id { get; set; }
    public decimal Amount { get; set; }
}
```

### Resetting the Sequence Floor

```cs
await store.Advanced.ResetHiloSequenceFloor<Invoice>();
```

This scans existing documents and resets the HiLo sequence to start above the highest existing ID.

## ID Member Resolution

Polecat looks for an `Id` property on your document class. The property must be public with a getter and setter.
