using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for pc_event_progression — tracks async daemon progress.
/// </summary>
internal class EventProgressionTable : Table
{
    public const string TableName = "pc_event_progression";

    public EventProgressionTable(string schemaName)
        : base(new SqlServerObjectName(schemaName, TableName))
    {
        AddColumn("name", "varchar(200)").AsPrimaryKey().NotNull();
        AddColumn("last_seq_id", "bigint").NotNull().DefaultValue(0);
        AddColumn("last_updated", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");
    }
}
