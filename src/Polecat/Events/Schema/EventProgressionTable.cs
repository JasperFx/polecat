using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for pc_event_progression — tracks async daemon progress.
/// </summary>
internal class EventProgressionTable : Table
{
    public const string TableName = "pc_event_progression";

    public EventProgressionTable(EventGraph eventGraph)
        : base(new SqlServerObjectName(eventGraph.DatabaseSchemaName, TableName))
    {
        AddColumn("name", "varchar(200)").AsPrimaryKey().NotNull();
        AddColumn("last_seq_id", "bigint").NotNull().DefaultValue(0);
        AddColumn("last_updated", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        if (eventGraph.EnableExtendedProgressionTracking)
        {
            AddColumn("heartbeat", "datetimeoffset").AllowNulls();
            AddColumn("agent_status", "varchar(20)").AllowNulls();
            AddColumn("pause_reason", "nvarchar(max)").AllowNulls();
            AddColumn("running_on_node", "int").AllowNulls();
            AddColumn("warning_behind_threshold", "bigint").AllowNulls();
            AddColumn("critical_behind_threshold", "bigint").AllowNulls();

            // #368 / jasperfx#565: the classified reason this shard is paused or stopped, so a consumer
            // polling the database (CritterWatch when the publishing node is DOWN, which is exactly when
            // it matters) sees the same reason an in-process ShardState observer does. The reason *text*
            // needs no new column — ShardFailure.Detail is precisely what pause_reason has always
            // carried. failure_category stores the enum NAME, never the ordinal, so reordering
            // ShardFailureCategory can never silently re-label persisted rows.
            AddColumn("failure_category", "varchar(50)").AllowNulls();
            AddColumn("failure_event_sequence", "bigint").AllowNulls();
            AddColumn("failure_event_type", "varchar(500)").AllowNulls();
            AddColumn("failure_event_tenant_id", "varchar(500)").AllowNulls();
        }
    }
}
