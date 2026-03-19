# Computed Indexes

Polecat supports computed indexes on document properties. These indexes use SQL Server's **persisted computed columns** backed by `JSON_VALUE` expressions, giving you the performance of a traditional column index without duplicating data outside of the JSON document.

## How It Works

When you define a computed index, Polecat:

1. Adds a **persisted computed column** to the document table using `JSON_VALUE(data, '$.path')`
2. Creates a standard **nonclustered index** on that computed column

For example, indexing `UserName` on a `User` document produces:

```sql
ALTER TABLE [myschema].[pc_doc_user]
    ADD [cc_username] AS CAST(JSON_VALUE(data, '$.userName') AS varchar(250)) PERSISTED;

CREATE NONCLUSTERED INDEX [ix_pc_doc_user_username]
    ON [myschema].[pc_doc_user] ([cc_username]);
```

## Simple Indexes

Use the fluent API via `StoreOptions.Schema.For<T>().Index()`:

```cs
var store = DocumentStore.For(opts =>
{
    opts.ConnectionString = "...";
    opts.Schema.For<User>().Index(x => x.UserName);
});
```

SQL Server will use this index when querying:

```cs
var user = await session.Query<User>()
    .FirstOrDefaultAsync(x => x.UserName == "somebody");
```

## Composite (Multi-Column) Indexes

Create a single index across multiple properties using an anonymous type:

```cs
opts.Schema.For<User>().Index(x => new { x.FirstName, x.LastName });
```

This produces one nonclustered index with both columns:

```sql
CREATE NONCLUSTERED INDEX [ix_pc_doc_user_firstname_lastname]
    ON [myschema].[pc_doc_user] ([cc_firstname], [cc_lastname]);
```

## Unique Indexes

```cs
opts.Schema.For<User>().UniqueIndex(x => x.Email);
```

Attempting to store two documents with the same email will throw a SQL Server unique constraint violation.

## Customizing an Index

The `Index()` and `UniqueIndex()` methods accept an optional `Action<DocumentIndex>` to customize the index:

```cs
opts.Schema.For<User>().Index(x => x.UserName, idx =>
{
    // Force the indexed value to lowercase for case-insensitive lookups
    idx.Casing = IndexCasing.Lower;

    // Override the index name
    idx.IndexName = "ix_user_name_ci";

    // Use a different SQL type (default: varchar(250))
    idx.SqlType = "varchar(500)";

    // Change sort order (default: Ascending)
    idx.SortOrder = SortOrder.Descending;

    // Scope uniqueness per tenant (for conjoined tenancy)
    idx.TenancyScope = TenancyScope.PerTenant;

    // Add a WHERE clause for a filtered (partial) index
    idx.Predicate = "tenant_id <> 'EXCLUDED'";
});
```

## Case Transformations

For case-insensitive lookups, you can apply `UPPER()` or `LOWER()` transformations to string-typed index columns. This wraps the `JSON_VALUE` expression so the persisted computed column stores the normalized value:

```cs
// Lowercase index — stores "john.doe@example.com" even if original is "John.Doe@Example.COM"
opts.Schema.For<User>().Index(x => x.Email, idx =>
{
    idx.Casing = IndexCasing.Lower;
});

// Uppercase index
opts.Schema.For<User>().Index(x => x.UserName, idx =>
{
    idx.Casing = IndexCasing.Upper;
});
```

The generated SQL for a lowercase index:

```sql
ALTER TABLE [myschema].[pc_doc_user]
    ADD [cc_email_lower] AS LOWER(CAST(JSON_VALUE(data, '$.email') AS varchar(250))) PERSISTED;

CREATE NONCLUSTERED INDEX [ix_pc_doc_user_email_lower]
    ON [myschema].[pc_doc_user] ([cc_email_lower]);
```

::: tip
Case transformations only apply to string-typed columns. Non-string columns (int, Guid, etc.) ignore the `Casing` setting.
:::

### Case-Insensitive Unique Indexes

Combine casing with unique indexes to enforce uniqueness regardless of case:

```cs
opts.Schema.For<User>().UniqueIndex(x => x.Email, idx =>
{
    idx.Casing = IndexCasing.Lower;
});
```

This rejects both `"test@example.com"` and `"Test@Example.COM"` as duplicates.

## Attribute-Based Indexes

Instead of (or in addition to) the fluent API, you can declare indexes directly on your document properties using attributes.

### [Index] Attribute

Marks a property for a computed index:

```cs
using Polecat.Attributes;

public class User
{
    public Guid Id { get; set; }

    [Index]
    public string UserName { get; set; } = "";

    [Index(Casing = IndexCasing.Lower)]
    public string Email { get; set; } = "";

    [Index(SqlType = "int")]
    public int Age { get; set; }
}
```

### [UniqueIndex] Attribute

Marks a property for a unique computed index:

```cs
public class User
{
    public Guid Id { get; set; }

    [UniqueIndex]
    public string Email { get; set; } = "";

    public string Name { get; set; } = "";
}
```

### Composite Unique Indexes with Attributes

Use the `IndexName` property to group multiple properties into a single composite unique index:

```cs
public class User
{
    public Guid Id { get; set; }

    [UniqueIndex(IndexName = "ux_fullname")]
    public string FirstName { get; set; } = "";

    [UniqueIndex(IndexName = "ux_fullname")]
    public string LastName { get; set; } = "";
}
```

This creates one unique index across both `FirstName` and `LastName`.

### Attribute Options

Both `[Index]` and `[UniqueIndex]` support these options:

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `IndexName` | `string?` | Auto-generated | Explicit index name |
| `Casing` | `IndexCasing` | `Default` | Case transformation (`Upper`, `Lower`, `Default`) |
| `SqlType` | `string?` | `varchar(250)` | SQL type for the computed column |
| `SortOrder` | `SortOrder` | `Ascending` | Sort order (Index only) |
| `TenancyScope` | `TenancyScope` | `Global` | Per-tenant scoping (UniqueIndex only) |

::: tip
Attribute-based indexes are discovered automatically when the document type is first used. They can be combined with fluent API indexes on the same document type.
:::

## Tenancy-Scoped Indexes

For multi-tenant applications using conjoined tenancy, you can scope unique indexes per tenant:

```cs
opts.Schema.For<User>().UniqueIndex(x => x.Email, idx =>
{
    idx.TenancyScope = TenancyScope.PerTenant;
});
```

This includes `tenant_id` in the index columns, allowing the same email across different tenants while enforcing uniqueness within each tenant.
