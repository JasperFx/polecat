using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for pc_streams — stores stream metadata.
/// </summary>
internal class StreamsTable : Table
{
    public const string TableName = "pc_streams";

    public StreamsTable(EventGraph events)
        : base(new SqlServerObjectName(events.DatabaseSchemaName, TableName))
    {
        if (events.TenancyStyle == TenancyStyle.Conjoined)
        {
            AddColumn("tenant_id", "varchar(250)").AsPrimaryKey().NotNull();
        }

        var idType = events.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? "uniqueidentifier"
            : "varchar(250)";

        AddColumn("id", idType).AsPrimaryKey().NotNull();

        AddColumn("type", "varchar(250)").AllowNulls();
        AddColumn("version", "bigint").NotNull().DefaultValue(0);

        AddColumn("timestamp", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        AddColumn("created", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        AddColumn("snapshot", "nvarchar(max)").AllowNulls();
        AddColumn("snapshot_version", "int").AllowNulls();

        if (events.TenancyStyle != TenancyStyle.Conjoined)
        {
            AddColumn("tenant_id", "varchar(250)")
                .NotNull()
                .DefaultValueByString(Tenancy.DefaultTenantId);
        }

        AddColumn("is_archived", "bit").NotNull().DefaultValue(0);
    }
}
