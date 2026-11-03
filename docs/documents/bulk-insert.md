# Bulk Insert

Polecat provides a high-performance bulk insert API for inserting large numbers of documents efficiently.

## Basic Usage

```cs
var users = Enumerable.Range(0, 1000)
    .Select(i => new User { FirstName = $"User{i}", LastName = "Bulk" })
    .ToList();

await store.Advanced.BulkInsertAsync(users);
```

## Bulk Insert Modes

### InsertsOnly (Default)

Inserts all documents. Throws on duplicate IDs:

```cs
await store.Advanced.BulkInsertAsync(users, BulkInsertMode.InsertsOnly);
```

### IgnoreDuplicates

Inserts new documents and silently skips duplicates:

```cs
await store.Advanced.BulkInsertAsync(users, BulkInsertMode.IgnoreDuplicates);
```

### OverwriteExisting

Inserts new documents and updates existing ones (upsert):

```cs
await store.Advanced.BulkInsertAsync(users, BulkInsertMode.OverwriteExisting);
```

## Batch Size

Control the number of documents per batch (default: 200):

```cs
await store.Advanced.BulkInsertAsync(users, BulkInsertMode.InsertsOnly, batchSize: 500);
```

## Multi-Tenant Bulk Insert

Specify a tenant ID for conjoined tenancy:

```cs
await store.Advanced.BulkInsertAsync(users, BulkInsertMode.InsertsOnly, 200, tenantId: "tenant-a");
```

## Automatic ID Assignment

Bulk insert automatically handles ID assignment:

- **Guid IDs**: Auto-generated for empty Guids
- **Numeric IDs**: Assigned via HiLo sequences
- **Strongly Typed IDs**: Inner values are auto-assigned

A string id its column can't hold is refused before the first batch is written, so a bulk insert doesn't stop
halfway through for it. See [what a string id can hold](/documents/identity#what-a-string-id-can-hold).

## Metadata Sync

During bulk insert, Polecat automatically syncs metadata interfaces:

- `ITenanted.TenantId` is set from the tenant parameter
- `ISoftDeleted` properties are initialized
- `ITracked` properties are synced if set on the session
