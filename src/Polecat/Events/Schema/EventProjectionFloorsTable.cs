using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     #813 (jasperfx#809): pc_event_projection_floors — the persisted start floor of each projection,
///     the sequence below which it has no meaningful history.
/// </summary>
/// <remarks>
///     <para>
///         <b>A table of its own, not columns on pc_event_progression,</b> because a floor has to survive
///         rebuild teardown and a rebuild deletes the progression row. Keeping the two apart also keeps
///         "no progression row means a fresh shard" true, which the daemon and
///         <c>WaitForNonStaleProjectionDataAsync</c> both rely on.
///     </para>
///     <para>
///         Keyed by the projection's <c>ShardName.Name</c> WITHOUT its version, so a version bump inherits
///         the floor, plus the tenant. <c>tenant_id</c> is part of the primary key and so cannot be null:
///         the store-global floor (a null tenant through <c>IEventDatabase</c>) is stored as the empty
///         string, which is never a tenant id.
///     </para>
///     <para>
///         Deliberately left alone by every data-cleaning path (<c>Advanced.Clean</c>,
///         <c>ResetAllData</c>): a floor is operator configuration about where history starts, not data
///         derived from the events.
///     </para>
/// </remarks>
internal class EventProjectionFloorsTable : Table
{
    public const string TableName = "pc_event_projection_floors";

    /// <summary>What a null tenant id is stored as. Not a valid tenant id anywhere in Polecat.</summary>
    public const string StoreGlobalTenant = "";

    public EventProjectionFloorsTable(EventGraph eventGraph)
        : base(new SqlServerObjectName(eventGraph.DatabaseSchemaName, TableName))
    {
        AddColumn("name", "varchar(200)").AsPrimaryKey().NotNull();
        AddColumn("tenant_id", "varchar(250)").AsPrimaryKey().NotNull();
        AddColumn("floor_seq_id", "bigint").NotNull();
        AddColumn("last_updated", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");
    }
}
