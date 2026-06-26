using Polecat.Metadata;
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
}
