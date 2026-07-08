# How Documents are Stored

Polecat stores documents as JSON in SQL Server 2025 using the native `json` data type.

## Document Table Structure

Each document type gets its own table with the prefix `pc_doc_`. A plain, single-tenant document
type produces exactly these columns — no `tenant_id` (that is conjoined-tenancy only, see below):

```sql
CREATE TABLE dbo.pc_doc_user (
    id uniqueidentifier NOT NULL,
    data json NOT NULL,
    version bigint NOT NULL,
    last_modified datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    created_at datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    dotnet_type varchar(500) NULL,
    CONSTRAINT pkey_pc_doc_user_id PRIMARY KEY (id)
);
```

The `version` column is always present (a `bigint` that carries optimistic/numeric concurrency
revisions when configured); `last_modified`, `created_at`, and `dotnet_type` are the standard
metadata columns. The `tenant_id` column is **only** created for conjoined multi-tenancy — a
single-tenant store keeps every document under the default tenant implicitly, with no `tenant_id`
column and no per-query tenant filter.

## ID Column Types

The `id` column type varies based on the document's ID property:

| .NET Type | SQL Server Type |
| :--- | :--- |
| `Guid` | `uniqueidentifier` |
| `string` | `nvarchar(250)` |
| `int` | `int` |
| `long` | `bigint` |

## Optional Columns

Additional columns are added based on document configuration:

### Soft Deletes

```sql
is_deleted bit NOT NULL DEFAULT 0,
deleted_at datetimeoffset NULL
```

### Guid Versioning (IVersioned)

```sql
guid_version uniqueidentifier NULL
```

### Numeric Revisions (IRevisioned / ILongVersioned)

The `version` column is always `bigint` (Decision D2), carrying both `IRevisioned` (int, downcast on
read) and `ILongVersioned` (long) revisions. Every write sets it explicitly, so it has no default.

```sql
version bigint NOT NULL
```

### Conjoined Tenancy

```sql
tenant_id nvarchar(250) NOT NULL DEFAULT 'DEFAULT'
```

The primary key becomes composite: `PRIMARY KEY (tenant_id, id)`.

### Metadata Tracking

```sql
correlation_id nvarchar(250) NULL,
causation_id nvarchar(250) NULL
```

## JSON Storage

The `data` column uses SQL Server 2025's native `json` type. This provides:

- **Server-side validation** -- Invalid JSON is rejected at the database level
- **JSON_VALUE()** -- Extract scalar values for WHERE clauses
- **JSON_QUERY()** -- Extract objects and arrays
- **JSON_MODIFY()** -- Partial updates without full document rewrite
- **Efficient storage** -- Compact representation compared to `nvarchar(max)`

## Upsert Strategy

Polecat uses SQL Server's `MERGE` statement for upsert operations:

```sql
MERGE pc_doc_user AS target
USING (VALUES (@id, @data, @type, ...)) AS source (id, data, type, ...)
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET data = source.data, ...
WHEN NOT MATCHED THEN INSERT (id, data, type, ...) VALUES (...);
```
