using System.Data.Common;
using Polecat.Metadata;
using Weasel.Core;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Storage;

/// <summary>
///     Weasel table definition for a document type.
///     Table name follows the pattern: pc_doc_{lowercase_type_name}
/// </summary>
internal class DocumentTable : Table
{
    public DocumentTable(DocumentMapping mapping)
        : base(new SqlServerObjectName(mapping.DatabaseSchemaName, mapping.TableName))
    {
        if (mapping.TenancyStyle == TenancyStyle.Conjoined)
        {
            AddColumn(JasperFx.StorageConstants.TenantIdColumn, "varchar(250)").AsPrimaryKey().NotNull();
        }

        // #296: strongly-typed ids persist their INNER primitive value (ValueTypeIdentification
        // unwraps to TInner on write and reads GetFieldValue<TInner> on read), so the id column
        // must be the inner type — uniqueidentifier/int/bigint — NOT varchar(250) derived from the
        // wrapper type. A varchar id column trips InvalidCastException in the shared writeable
        // selectors on any Lightweight/IdentityMap database read of a wrapper-id document.
        var idColumnType = mapping.InnerIdType == typeof(Guid) ? "uniqueidentifier"
            : mapping.InnerIdType == typeof(int) ? "int"
            : mapping.InnerIdType == typeof(long) ? "bigint"
            : "varchar(250)";

        AddColumn("id", idColumnType).AsPrimaryKey().NotNull();

        AddColumn("data", mapping.JsonColumnType).NotNull();

        // Always bigint (Decision D2): natively carries ILongVersioned (long) revisions, with
        // IRevisioned (int) values fitting and downcast on read. No default constraint — every
        // write sets version explicitly (INSERT => 1, UPDATE => version + 1), and a default would
        // both be dead weight and block the in-place int->bigint widening of pre-D2 tables
        // (handled in DocumentTableEnsurer.WidenVersionColumnIfNeededAsync).
        AddColumn("version", "bigint").NotNull();

        AddColumn("last_modified", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        AddColumn("created_at", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        AddColumn("dotnet_type", "varchar(500)").AllowNulls();

        // #241: opt-in document metadata columns (correlation_id / causation_id / last_modified_by /
        // headers), enabled via the .Metadata(...) DSL or metadata attributes (#243).
        foreach (var column in mapping.EnabledMetadataColumns)
        {
            var columnType = column.Name == "headers" ? mapping.JsonColumnType : "varchar(250)";
            AddColumn(column.Name, columnType).AllowNulls();
        }

        // Sub-class hierarchy discriminator
        if (mapping.IsHierarchy())
        {
            AddColumn("doc_type", "varchar(250)").NotNull().DefaultValueByString("base");
        }

        // Soft delete columns
        if (mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            AddColumn("is_deleted", "bit").NotNull().DefaultValue(0);
            AddColumn("deleted_at", "datetimeoffset").AllowNulls();
        }

        // Guid-based optimistic concurrency
        if (mapping.UseOptimisticConcurrency)
        {
            AddColumn("guid_version", "uniqueidentifier").NotNull().DefaultValueByExpression("NEWID()");
        }

        // #234: single-tenant (non-conjoined) tables do NOT carry a tenant_id column, matching
        // Marten — every document lives under the default tenant implicitly, so the whole runtime
        // tenant_id if/then (schema column, write binder, load filter, LINQ filter, metadata query)
        // stays off for single-tenant stores. Conjoined tenancy adds tenant_id to the primary key
        // above. Existing single-tenant tables keep their (now-unmodeled) tenant_id column via the
        // additive CreateDeltaAsync override (#267) — it is defaulted, so INSERTs that omit it still
        // succeed — so this is purely additive with no destructive migration.

        // Managed per-tenant partitioning (#335): the conjoined table is physically partitioned by
        // the store's shared tenant-ordinal strategy (one pc_tenant_partitions registry per
        // database). SQL Server requires the partition column in the clustered index, so
        // tenant_ordinal joins the primary key after (tenant_id, id) — reads still prefix-seek on
        // (tenant_id, id); every write resolves the ordinal server-side from the registry.
        if (mapping.TenantPartitioned)
        {
            if (mapping.Partitioning is not null)
            {
                throw new NotSupportedException(
                    $"Document '{mapping.DocumentType.Name}' cannot combine PartitionByRange with the " +
                    "store's managed tenant partitioning — a SQL Server table supports only one " +
                    "partition scheme. Opt the type out via " +
                    "Policies.ForDocument<T>(p => p.DisableTenantPartitioning = true) to keep the " +
                    "custom RANGE partitioning.");
            }

            AddColumn(DocumentMapping.TenantOrdinalColumn, "int").NotNull().AsPrimaryKey();
            this.PartitionByManagedTenants(mapping.StoreOptions.EventGraph.TenantPartitionManager);
        }
        // Declarative SQL Server RANGE partitioning (#211). The partition column must be part of the
        // table's unique (clustered) index, so a promoted member joins the primary key.
        else if (mapping.Partitioning is { } partitioning)
        {
            if (mapping.TenancyStyle == TenancyStyle.Conjoined)
            {
                throw new NotSupportedException(
                    "RANGE partitioning of document tables on a caller-chosen member is supported for " +
                    $"single-tenant tables only, but '{mapping.DocumentType.Name}' uses conjoined " +
                    "tenancy. Conjoined tables can instead be partitioned per tenant via " +
                    "StoreOptions.Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement() (#335).");
            }

            if (partitioning.RequiresDuplicatedColumn)
            {
                AddColumn(partitioning.ColumnName, partitioning.SqlDataType).AsPrimaryKey().NotNull();
            }

            var range = PartitionByRange(partitioning.ColumnName, partitioning.SqlDataType);
            foreach (var boundary in partitioning.Boundaries)
            {
                range.AddBoundary(boundary);
            }
        }
    }

    /// <summary>
    ///     #267: Polecat's <see cref="DocumentTable" /> models only the columns it manages. The
    ///     persisted computed columns and secondary indexes Polecat itself creates are applied as
    ///     idempotent raw DDL by <see cref="Internal.DocumentTableEnsurer" /> (not modeled here), and a
    ///     user may add their own (e.g. a persisted computed column + unique index via an EF migration).
    ///     Weasel's default <see cref="TableDelta" /> would treat every such object as an "extra" and
    ///     emit <c>DROP COLUMN</c> / <c>DROP INDEX</c> — destructive — while also leaving the table with
    ///     a permanently non-empty diff that makes every storage-ensure re-run DDL.
    ///     <para>
    ///     This override strips those unmodeled objects from the fetched (actual) table before the diff
    ///     is computed, so Polecat is purely additive: it creates what is missing from its own model and
    ///     never drops or churns columns/indexes it does not own. Columns and indexes Polecat <em>does</em>
    ///     model still reconcile normally (missing/different are detected as before).
    ///     </para>
    /// </summary>
    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader,
        CancellationToken ct = default)
    {
        var delta = (TableDelta)await base.CreateDeltaAsync(reader, ct).ConfigureAwait(false);

        // Null Actual => the table does not exist yet; nothing to strip (this is a Create).
        var actual = delta.Actual;
        if (actual == null)
        {
            return delta;
        }

        var modeledColumns = Columns.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var extra in actual.Columns.Where(c => !modeledColumns.Contains(c.Name)).ToList())
        {
            actual.RemoveColumn(extra.Name);
        }

        // DocumentTable does not model secondary indexes in Weasel (they are raw-DDL managed), so any
        // non-primary-key index on the live table is treated as user/Polecat-owned and left in place.
        var modeledIndexes = Indexes.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var extra in actual.Indexes.Where(i => !modeledIndexes.Contains(i.Name)).ToList())
        {
            actual.Indexes.Remove(extra);
        }

        // Foreign keys are likewise raw-DDL managed (DocumentTableEnsurer.EnsureForeignKeysAsync) and
        // not modeled here, so an unmodeled FK — Polecat's own or a user's — must not be dropped either.
        var modeledForeignKeys = ForeignKeys.Select(x => x.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var extra in actual.ForeignKeys.Where(f => !modeledForeignKeys.Contains(f.Name)).ToList())
        {
            actual.ForeignKeys.Remove(extra);
        }

        // Recompute the diff against the now-stripped actual table.
        return new TableDelta(this, actual);
    }
}
