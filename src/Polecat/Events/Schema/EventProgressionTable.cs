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
        }
    }
}
