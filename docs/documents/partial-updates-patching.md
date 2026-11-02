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

If `Address` is missing or null, it is created, however many levels are missing. A parent that holds some other value, such as a number, is left alone. This goes for everything that writes a value, including `Increment`, `Append`, `Insert` and `Duplicate`. `Delete` and `Remove` never create anything.

Set a dictionary entry with its indexer, whatever the type of the key:

```cs
session.Patch<Order>(orderId)
    .Set(x => x.NotesByDay[new DateOnly(2026, 11, 2)], "Shipped")
    .Set(x => x.Labels["a.key with spaces"], "value");
```

The key is written exactly the way System.Text.Json writes it in the document, including any `DictionaryKeyPolicy` and `Encoder` you have configured, so this finds the stored entry on any host culture. `PropertyNamingPolicy` is not applied to a key, because the serializer doesn't apply it either. An index into a list, as in `x.Items[0]`, isn't supported and throws `NotSupportedException`.

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

`Remove`, `AppendIfNotExists` and `InsertIfNotExists` match an element exactly, whatever the database's collation: `"ABC"` is not `"abc"`, `"abc "` is not `"abc"`, and the string `"1"` is not the number `1`.

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
SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), N'$.status', 'Shipped')
WHERE id = @id
```

For collection operations, Polecat uses `OPENJSON` and `STRING_AGG` to manipulate JSON arrays.

::: tip
Patching is more efficient than loading, modifying, and re-saving a document because only the changed values are sent to the database, and it doesn't require reading the full document first.
:::

### Patching and the Native `json` Type

The `CAST` above is deliberate. On SQL Server 2025 RTM-CU5, a `JSON_MODIFY` that adds a key directly to a `json` value leaves it in a state where the next `JSON_MODIFY` through an object can fail with "Encountered corrupted JSON type", write to the wrong member, or leave the document unreadable. Until that second write the row reads back correctly, and `DBCC CHECKTABLE` doesn't report it, even with `EXTENDED_LOGICAL_CHECKS`. Working on text avoids this, and the next patch to a row that an earlier version left in that state puts it right. A write that has already gone to the wrong member can't be undone this way.

The cost is a full parse of the document for each patch operation, so the time for one update grows with its size. Measured per update on one server:

| Document size | Directly on `json` | Through text |
|---|---|---|
| 1 KB | 0.7 ms | 0.9 ms |
| 10 KB | 1.0 ms | 2.8 ms |
| 100 KB | 1.7 ms | 14.8 ms |
| 1 MB | 4.1 ms | 170 ms |

`nvarchar(max)` storage pays none of this.
