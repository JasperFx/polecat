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

Polecat supports [strong typed identifiers](https://en.wikipedia.org/wiki/Strongly_typed_identifier) —
types that wrap one of the supported primitive ID types (`Guid`, `string`, `int`, or `long`) so that an
`OrderId` can never be passed where a `CustomerId` was meant.

### Supported Patterns

Polecat detects wrapper types through JasperFx's `ValueTypeInfo`. A type qualifies when it exposes
exactly one public, readable property wrapping a supported inner type, plus a way to build it from that
value — either a matching constructor or a public static factory method. Two shapes follow:

**1. Record struct with constructor:**

```cs
public record struct OrderId(Guid Value);

public class Order
{
    public OrderId Id { get; set; }
    public string Name { get; set; } = "";
}
```

**2. Struct with a static builder method:**

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

There is **no naming requirement** — neither the wrapper type nor its inner property has to be called
anything in particular. `Value` and `From` are simply the conventional names, and the ones both source
generators below emit.

### Using Vogen or StronglyTypedId

Hand-rolled wrappers work, but a generator gives you equality, comparison, `ToString()`, and — most
importantly — a `System.Text.Json` converter that writes the *inner* value. That last part is what lets
Polecat treat the member as a scalar in SQL. Polecat's test suite exercises both libraries directly.

[Vogen](https://github.com/SteveDunn/Vogen) emits a private constructor plus a static `From` factory:

```cs
[ValueObject<Guid>]
public readonly partial struct InvoiceId;

public class Invoice
{
    // Polecat will use this as the Invoice's identity
    public InvoiceId? Id { get; set; }
    public string Name { get; set; } = "";
}
```

[StronglyTypedId](https://github.com/andrewlock/StronglyTypedId) emits a public constructor:

```cs
[StronglyTypedId(Template.Int)]
public readonly partial struct OrderId;

public class Order
{
    public OrderId Id { get; set; }
    public string Name { get; set; } = "";
}
```

::: warning
**Vogen identities must be declared `Nullable`** — `InvoiceId? Id`, as above. Vogen forbids an
uninitialized value object, so reading `.Value` off a `default` instance throws. Polecat inspects the
identity to decide whether one needs assigning, and on a non-nullable Vogen id that inspection throws
before Polecat can see that the id was unset. StronglyTypedId permits `default` and so can be declared
either way.
:::

### Identity Assignment

| Inner type | ID generation |
| --- | --- |
| `Guid` | Auto-assigned sequential `Guid` |
| `int` | [HiLo sequence](#hilo-sequences) |
| `long` | [HiLo sequence](#hilo-sequences) |
| `string` | You assign it — Polecat never generates string identities |

The identity column is created with the **inner** type (`uniqueidentifier`, `int`, `bigint`,
`varchar`), not a serialized form of the wrapper.

### Usage

```cs
// Store with an auto-assigned Guid wrapper
var order = new Order { Name = "Widget" };
session.Store(order);
await session.SaveChangesAsync();
// order.Id is now assigned

// Load by the wrapper itself
var loaded = await query.LoadAsync<Order>(order.Id);

// ...or by the inner value; both resolve the same row
var alsoLoaded = await query.LoadAsync<Order>(order.Id.Value);

// LINQ queries take the wrapper type directly
var result = await query.Query<Order>()
    .Where(x => x.Id == order.Id)
    .FirstOrDefaultAsync();

// IsOneOf for multiple ids
var results = await query.Query<Order>()
    .Where(x => x.Id.IsOneOf(id1, id2, id3))
    .ToListAsync();

// Select projects the wrapper back out
var ids = await query.Query<Order>()
    .Select(x => x.Id)
    .ToListAsync();

// Delete by inner value
session.Delete<Order>(order.Id.Value);

// Check existence
var exists = await query.CheckExistsAsync<Order>(order.Id.Value);
```

Supported across:

- `Store()` / `Insert()` / `Update()`, with automatic identity assignment
- `LoadAsync()` by the wrapper **or** by the inner value; `LoadManyAsync()` by inner value
- `Delete()` by inner value or by document
- `CheckExistsAsync()` by inner value
- LINQ `Where`, `OrderBy` / `OrderByDescending`, `IsOneOf`, `Select`, `Count`
- Paging via `ToPagedListAsync()`
- Identity map sessions
- Bulk insert via `BulkInsertAsync()`
- Batched queries — `Load`, `LoadMany`, `CheckExists`, and `Query<T>()`
- Aggregate identities in event projections

::: warning
`Delete()` and `CheckExistsAsync()` take the **inner** value (`order.Id.Value`), not the wrapper. Only
`LoadAsync()` accepts either — see below.
:::

#### Loading by the wrapper

`LoadAsync<T>(object id)` is the overload a strong typed identifier binds to, and it accepts either
spelling: the wrapper (`order.Id`) or the inner value (`order.Id.Value`) resolve the same row, identity
map included. The `Guid` / `string` / `int` / `long` overloads stay preferred by overload resolution, so
adding it moved no existing call site — only an argument that fits none of them lands here.

```cs
var byWrapper = await query.LoadAsync<Order>(order.Id);
var byInner = await query.LoadAsync<Order>(order.Id.Value);
```

Passing an identity of any other type throws `ArgumentException` naming the document type and both id
types, rather than failing later as an id-type mismatch.

This is also the store-agnostic spelling: it satisfies
`JasperFx.Events.Documents.IDocumentReadOperations.LoadAsync<T>(object, CancellationToken)`, so source
shared with Marten can do a by-id load of a strong-typed-id document against either store
([#472](https://github.com/JasperFx/polecat/issues/472),
[jasperfx#665](https://github.com/JasperFx/jasperfx/issues/665)).

### Value Types on Other Members

A wrapper is not limited to the identity. Any document member can be one, and it stays queryable:

```cs
[ValueObject<Guid>]
public readonly partial struct TeacherId;

public class ClassRoom
{
    public Guid Id { get; set; }
    public TeacherId Teacher { get; set; }
    public string Subject { get; set; } = "";
}

// Filter, order and project by the wrapper
var rooms = await query.Query<ClassRoom>()
    .Where(x => x.Teacher == teacherId)
    .OrderBy(x => x.Teacher)
    .ToListAsync();

var teacherIds = await query.Query<ClassRoom>()
    .Select(x => x.Teacher)
    .ToListAsync();
```

This works because the generator's JSON converter writes the inner value, so the member lands in the
document JSON as a bare scalar. Polecat types the SQL from the inner type and unwraps the parameter
for you. The same unwrapping applies to:

- **Computed indexes** — `Schema.For<ClassRoom>().Index(x => x.Teacher)` creates a
  `uniqueidentifier` computed column, and the LINQ predicate matches it so the index stays seekable
- **Flat table projections** — `map.Map(x => x.Account)` creates a column of the inner type

::: tip
A wrapper written by hand, with no JSON converter, serializes as a nested object (`{"value": ...}`) and
will **not** match a scalar predicate. Use Vogen or StronglyTypedId for any value type you intend to
query on a non-identity member.
:::

A type with more than one public property — `record struct Money(decimal Amount, Guid CurrencyId)` —
is not a strong typed identifier and is left alone as a nested JSON object, so `x.Amount.CurrencyId`
keeps resolving as a nested path.

### Registering a Value Type

Polecat discovers value types on its own, so **you never need to register one**. The API exists purely
so that a single store-configuration file can compile against both Marten and Polecat:

```cs
// Compiles and means the same thing under either store
opts.RegisterValueType<AlertId>();
opts.RegisterValueType(typeof(AlertId));
```

Both overloads return JasperFx's `ValueTypeInfo`, matching Marten's signature. Registration resolves
the type eagerly and validates it, so passing something that cannot be a value wrapper throws
`InvalidValueTypeException` at configuration time rather than failing at the first query.

::: tip
Marten's docs describe a timing issue where value types used in LINQ have to be registered before the
first expression is evaluated. Polecat has no such issue — it resolves on demand and caches — so
registration here is genuinely optional.
:::

### Not Currently Supported

- No `LoadManyAsync()`, `Delete()` or `CheckExistsAsync()` overload taking the wrapper itself
  (`LoadAsync()` does — see [Loading by the wrapper](#loading-by-the-wrapper))
- No `Include()` LINQ operator
- No compiled queries
- F# single-case discriminated unions as identities

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

## Natural Keys with the [Identity] Attribute

If your document type uses a property name other than `Id` for its identity, you can use the `[Identity]`
attribute to designate the identity property. This is common when migrating from other databases or when
the natural key of the document has a more descriptive name.

```cs
using Polecat.Attributes;

public class Customer
{
    [Identity]
    public string CustomerCode { get; set; } = "";

    public string Name { get; set; } = "";
}
```

With the `[Identity]` attribute, Polecat will use `CustomerCode` as the document identity instead of
looking for a conventional `Id` property. All standard operations work the same way:

```cs
// Store
var customer = new Customer { CustomerCode = "CUST-001", Name = "Acme" };
session.Store(customer);
await session.SaveChangesAsync();

// Load by the identity value
var loaded = await query.LoadAsync<Customer>("CUST-001");

// Delete
session.Delete<Customer>("CUST-001");
```

### Priority

When both an `[Identity]` attribute and a conventional `Id` property exist on the same document type,
the `[Identity]` attribute takes priority:

```cs
public class LegacyDoc
{
    public Guid Id { get; set; }  // Ignored by Polecat

    [Identity]
    public string DocumentId { get; set; } = "";  // Used as the identity
}
```

### Supported Types

The `[Identity]` attribute works with all the same ID types as the conventional `Id` property:
`Guid`, `string`, `int`, `long`, and [strongly typed ID wrappers](#strongly-typed-ids).

## ID Member Resolution

Polecat resolves the identity property in the following priority order:

1. A property marked with `[Identity]` attribute
2. A public property named `Id`

The property must be public with both a getter and setter.
