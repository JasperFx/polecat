using System.Data.Common;
using Polecat.Metadata;
using Weasel.Core;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

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

        var idColumnType = mapping.IdType == typeof(Guid) ? "uniqueidentifier"
            : mapping.IdType == typeof(int) ? "int"
            : mapping.IdType == typeof(long) ? "bigint"
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

        if (mapping.TenancyStyle != TenancyStyle.Conjoined)
        {
            AddColumn(JasperFx.StorageConstants.TenantIdColumn, "varchar(250)")
                .NotNull()
                .DefaultValueByString(JasperFx.StorageConstants.DefaultTenantId);
        }

        // Declarative SQL Server RANGE partitioning (#211). The partition column must be part of the
        // table's unique (clustered) index, so a promoted member joins the primary key.
        if (mapping.Partitioning is { } partitioning)
        {
            if (mapping.TenancyStyle == TenancyStyle.Conjoined)
            {
                throw new NotSupportedException(
                    "RANGE partitioning of document tables is currently supported for single-tenant tables " +
                    $"only, but '{mapping.DocumentType.Name}' uses conjoined tenancy.");
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
