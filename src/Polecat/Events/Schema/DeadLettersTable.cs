using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for pc_dead_letters — stores projection/subscription
///     dead-letter events recorded when an <c>Apply</c> fails under
///     <c>SkipApplyErrors</c> (the JasperFx.Events 2.0 default). Columns mirror
///     <see cref="JasperFx.Events.Daemon.DeadLetterEvent" />; the
///     <c>(projection_name, shard_name)</c> pair is the per-shard grouping key the
///     dead-letter count read (jasperfx#356) aggregates on.
/// </summary>
internal class DeadLettersTable : Table
{
    public const string TableName = "pc_dead_letters";

    public DeadLettersTable(EventGraph eventGraph)
        : base(new SqlServerObjectName(eventGraph.DatabaseSchemaName, TableName))
    {
        AddColumn("id", "uniqueidentifier").AsPrimaryKey().NotNull();
        AddColumn("projection_name", "varchar(200)").NotNull();
        AddColumn("shard_name", "varchar(200)").NotNull();
        AddColumn("event_sequence", "bigint").NotNull();
        AddColumn("timestamp", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");
        AddColumn("exception_type", "nvarchar(500)").AllowNulls();
        AddColumn("exception_message", "nvarchar(max)").AllowNulls();
    }
}
