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
            AddColumn("tenant_id", "varchar(250)").AsPrimaryKey().NotNull();
        }

        var idColumnType = mapping.IdType == typeof(Guid)
            ? "uniqueidentifier"
            : "varchar(250)";

        AddColumn("id", idColumnType).AsPrimaryKey().NotNull();

        AddColumn("data", "nvarchar(max)").NotNull();

        AddColumn("version", "int").NotNull().DefaultValue(1);

        AddColumn("last_modified", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        AddColumn("dotnet_type", "varchar(500)").AllowNulls();

        if (mapping.TenancyStyle != TenancyStyle.Conjoined)
        {
            AddColumn("tenant_id", "varchar(250)")
                .NotNull()
                .DefaultValueByString(Tenancy.DefaultTenantId);
        }
    }
}
