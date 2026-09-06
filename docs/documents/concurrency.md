# Optimistic Concurrency

Polecat supports two forms of optimistic concurrency control to prevent lost updates.

## Guid-Based Versioning (IVersioned)

Each save generates a new Guid version. Concurrent modifications are detected when the expected version doesn't match:

```cs
public class Order : IVersioned
{
    public Guid Id { get; set; }
    public Guid Version { get; set; }
    public string Description { get; set; } = "";
}
```

Usage:

```cs
// Load and modify
var order = await session.LoadAsync<Order>(orderId);
// order.Version is automatically populated

order.Description = "Updated";
session.Store(order);
await session.SaveChangesAsync();
// order.Version is now a new Guid

// If another session modified the order between load and save,
// SaveChangesAsync throws ConcurrencyException
```

### UpdateExpectedVersion

Explicitly set the expected version for concurrency checks:

```cs
session.UpdateExpectedVersion(order, expectedGuidVersion);
```

## Numeric Revisions (IRevisioned)

An integer revision counter that increments on each save:

```cs
public class Order : IRevisioned
{
    public Guid Id { get; set; }
    public int Version { get; set; }
    public string Description { get; set; } = "";
}
```

A supplied revision is the **target version** — the revision the document is being asked to *become* —
and it is accepted only when it is **strictly greater** than the revision currently stored. A revision
of `0` means "auto": increment whatever is stored, whatever that is.

```cs
var order = await session.LoadAsync<Order>(orderId);
// order.Version == 1 (after first save)

order.Description = "Updated";
session.UpdateRevision(order, order.Version + 1);   // target revision 2
await session.SaveChangesAsync();
// order.Version == 2
```

::: warning
`Store(order)` passes the document's own `Version` as the target revision, so re-storing a document
still carrying the revision it was loaded at raises a `ConcurrencyException` — the loaded value is
equal to what is stored, and equal is not greater. Either name the next revision with
`UpdateRevision(order, order.Version + 1)`, or set `order.Version = 0` to take the auto path.
:::

An explicit revision may also **jump**, which is what lets a caller adopt a revision decided somewhere
else — an upstream version, a stream version, an imported record:

```cs
session.UpdateRevision(order, 10);   // from 3 straight to 10
await session.SaveChangesAsync();
// order.Version == 10 — exactly the revision named, not 11
```

### Explicit revisions on insert

A brand-new document carrying a non-zero `Version` is stored at **exactly** that revision rather than
normalised to 1, and the strictly-greater guard then applies from there:

```cs
session.Insert(new Order { Id = id, Version = 7, Description = "Imported" });
await session.SaveChangesAsync();
// the row lands at revision 7; the next auto store lands at 8
```

Leave `Version` at its default `0` to start a document at revision 1.

## Long Numeric Revisions (ILongVersioned)

`ILongVersioned` is the 64-bit counterpart of `IRevisioned` — identical behavior, but the revision is
a `long` instead of an `int`:

```cs
public class CustomerOrderHistory : ILongVersioned
{
    public Guid Id { get; set; }
    public long Version { get; set; }
    public string Description { get; set; } = "";
}
```

Usage mirrors `IRevisioned`, with a `long` overload of `UpdateRevision` for explicit checks:

```cs
var view = await session.LoadAsync<CustomerOrderHistory>(id);
view.Description = "Updated";
session.UpdateRevision(view, 4_000_000_000L);   // the TARGET revision, above whatever is stored
await session.SaveChangesAsync();
```

::: tip
Prefer `ILongVersioned` over `IRevisioned` for `MultiStreamProjection`-derived views whose `Version`
tracks the **global event sequence number**. That sequence is monotonic across every stream the view
folds in and can climb past `Int32.MaxValue` (~2.1 billion) on a busy store, where an `int` revision
would overflow. A plain single-stream aggregate, whose `Version` is just that stream's event count,
is fine with `IRevisioned`.
:::

Both interfaces persist into the same `version` column, which is always `bigint` (Decision D2) so the
two are storage-compatible: `IRevisioned` values fit and are downcast on read, while `ILongVersioned`
carries the full 64-bit value. Existing tables with an `int` version column are widened to `bigint`
in place on the next schema migration — a non-destructive `ALTER COLUMN`, never a drop/recreate.

## Configuration

### Auto-Detection

Polecat automatically detects concurrency mode from interfaces:

- Implements `IVersioned` → Guid-based versioning
- Implements `IRevisioned` → Numeric revisions (int)
- Implements `ILongVersioned` → Numeric revisions (long)

### Manual Configuration

```cs
opts.Policies.ForDocument<Order>(mapping =>
{
    mapping.UseOptimisticConcurrency = true; // Guid-based
    // OR
    mapping.UseNumericRevisions = true; // Integer-based
});
```

::: warning
`UseOptimisticConcurrency` and `UseNumericRevisions` are mutually exclusive. Choose one per document type.
:::

## ConcurrencyException

When a concurrent modification is detected, Polecat throws `ConcurrencyException` (from JasperFx):

```cs
try
{
    await session.SaveChangesAsync();
}
catch (ConcurrencyException ex)
{
    // Handle the conflict -- reload, merge, or notify the user
}
```

## How It Works

- **First save** (version is zero/empty): No concurrency check -- the document is inserted
- **Subsequent saves**: The MERGE/UPDATE statement includes a version check in its WHERE clause
- **Version sync**: After a successful save, the new version is read back from the database via OUTPUT clause and synced to the in-memory document
- **LINQ queries**: Version columns are included in SELECT, and version properties are synced on deserialization
