# Partial Updates / Patching

Polecat provides a patching API for making targeted updates to documents without loading and re-saving the entire document. Under the hood, this uses SQL Server's `JSON_MODIFY()` function.

## Basic Usage

```cs
// Patch a document by ID
session.Patch<Order>(orderId)
    .Set(x => x.Status, "Shipped");

await session.SaveChangesAsync();
```

## Available Operations

### Set a Value

```cs
session.Patch<Order>(orderId)
    .Set(x => x.Status, "Completed")
    .Set(x => x.ShippedDate, DateTimeOffset.UtcNow);
```

Set nested properties:

```cs
session.Patch<Order>(orderId)
    .Set(x => x.Address.City, "New York");
```

### Increment a Numeric Value

```cs
session.Patch<Order>(orderId)
    .Increment(x => x.ItemCount, 1);

// Decrement by using a negative value
session.Patch<Order>(orderId)
    .Increment(x => x.ItemCount, -1);
```

### Append to a Collection

```cs
session.Patch<Order>(orderId)
    .Append(x => x.Tags, "priority");
```

Append only if the value doesn't already exist:

```cs
session.Patch<Order>(orderId)
    .AppendIfNotExists(x => x.Tags, "priority");
```

### Insert at a Specific Position

```cs
session.Patch<Order>(orderId)
    .Insert(x => x.Tags, "urgent", index: 0);
```

### Remove from a Collection

```cs
session.Patch<Order>(orderId)
    .Remove(x => x.Tags, "obsolete");
```

### Duplicate a Value

Copy a value to multiple destinations:

```cs
session.Patch<Order>(orderId)
    .Duplicate(x => x.BillingAddress, x => x.ShippingAddress);
```

### Rename a Property

```cs
session.Patch<Order>(orderId)
    .Rename("oldPropertyName", x => x.NewPropertyName);
```

If a document has no `oldPropertyName`, its `NewPropertyName` is left as it is, so a rename can safely run again over documents that already have the new shape.

### Delete a Property

```cs
session.Patch<Order>(orderId)
    .Delete("obsoleteField");

// Or via expression
session.Patch<Order>(orderId)
    .Delete(x => x.ObsoleteField);
```

## Patching with a Where Clause

Apply patches to multiple documents matching a condition:

```cs
session.Patch<Order>(x => x.Status == "Pending")
    .Set(x => x.Status, "Cancelled");

await session.SaveChangesAsync();
```

## How It Works

Each patch operation generates a SQL `UPDATE` statement using `JSON_MODIFY()`:

```sql
UPDATE pc_doc_order
SET data = JSON_MODIFY(data, '$.status', 'Shipped')
WHERE id = @id
```

For collection operations, Polecat uses `OPENJSON` and `STRING_AGG` to manipulate JSON arrays.

::: tip
Patching is more efficient than loading, modifying, and re-saving a document because only the changed values are sent to the database, and it doesn't require reading the full document first.
:::
