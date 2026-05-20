# Database Storage

Polecat stores each document type in its own dedicated SQL Server table.

## Table Naming

Document tables follow the pattern `pc_doc_{typename}` where `{typename}` is the lowercase, simple name of the .NET type. For example:

- `User` → `pc_doc_user`
- `Order` → `pc_doc_order`
- `InvoiceLineItem` → `pc_doc_invoicelineitem`

## Table Structure

A typical document table includes:

```sql
CREATE TABLE dbo.pc_doc_user (
    id uniqueidentifier NOT NULL PRIMARY KEY,
    data json NOT NULL,
    type nvarchar(250) NULL,
    last_modified datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    created datetimeoffset NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    dotnet_type nvarchar(500) NULL
);
```

### Additional Columns

Depending on configuration, additional columns may be present:

| Column | Type | When Added |
| :--- | :--- | :--- |
| `tenant_id` | `nvarchar(250)` | Conjoined tenancy |
| `is_deleted` | `bit` | Soft deletes enabled |
| `deleted_at` | `datetimeoffset` | Soft deletes enabled |
| `guid_version` | `uniqueidentifier` | `IVersioned` interface |
| `version` | `bigint` | Revision counter (`IRevisioned` int / `ILongVersioned` long) |
| `correlation_id` | `nvarchar(250)` | Metadata tracking |
| `causation_id` | `nvarchar(250)` | Metadata tracking |

## JSON Storage

Document bodies are stored using SQL Server 2025's native `json` data type. This provides:

- Server-side JSON validation
- Efficient `JSON_VALUE()` extraction for WHERE clauses
- `JSON_MODIFY()` for [partial updates](/documents/partial-updates-patching)
- Compact storage format

## Auto-Create Behavior

By default (`AutoCreate.CreateOrUpdate`), Polecat will:

1. Create the table if it doesn't exist on first use
2. Add new columns if the configuration changes (e.g., enabling soft deletes)
3. Never drop existing columns

See [Schema Migrations](/schema/migrations) for more details.
