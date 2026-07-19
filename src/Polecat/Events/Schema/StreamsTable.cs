using Weasel.SqlServer;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;

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
            AddColumn(JasperFx.StorageConstants.TenantIdColumn, "varchar(250)").AsPrimaryKey().NotNull();
        }

        var idType = events.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? "uniqueidentifier"
            : "varchar(250)";

        AddColumn("id", idType).AsPrimaryKey().NotNull();

        // #335: partition pc_streams alongside pc_events under per-tenant partitioning (Marten
        // parity — mt_streams rides mt_events' tenant partitioning). SQL Server requires the
        // partition column in the clustered index, so tenant_ordinal joins the primary key AFTER
        // (tenant_id, id) — existing readers keep their (tenant_id, id) prefix seek. The ordinal is
        // stamped by the append path's stream-row SQL from the planner-resolved tenant cache.
        if (events.UseTenantPartitionedEvents)
        {
            AddColumn(events.TenantPartitionManager.Column, "int").NotNull().AsPrimaryKey();
            this.PartitionByManagedTenants(events.TenantPartitionManager);
        }

        AddColumn("type", "varchar(250)").AllowNulls();
        AddColumn("version", "bigint").NotNull().DefaultValue(0);

        AddColumn("timestamp", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        AddColumn("created", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        if (events.TenancyStyle != TenancyStyle.Conjoined)
        {
            AddColumn(JasperFx.StorageConstants.TenantIdColumn, "varchar(250)")
                .NotNull()
                .DefaultValueByString(JasperFx.StorageConstants.DefaultTenantId);
        }

        AddColumn("is_archived", "bit").NotNull().DefaultValue(0);
    }
}
