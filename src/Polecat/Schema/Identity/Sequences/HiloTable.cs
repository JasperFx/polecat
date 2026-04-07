using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Schema.Identity.Sequences;

/// <summary>
///     Weasel table definition for pc_hilo — stores HiLo sequence counters for numeric ID generation.
/// </summary>
internal class HiloTable : Table
{
    public const string TableName = "pc_hilo";

    public HiloTable(string schemaName)
        : base(new SqlServerObjectName(schemaName, TableName))
    {
        AddColumn("entity_name", "varchar(250)").AsPrimaryKey().NotNull();
        AddColumn("hi_value", "bigint").NotNull().DefaultValue(0);
    }
}
