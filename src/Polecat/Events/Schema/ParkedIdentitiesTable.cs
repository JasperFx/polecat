using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     marten#6003 / #1053: <c>pc_parked_identities</c>, one row for every identity of an async projection that failed
///     and was parked instead of skipping its event or pausing the shard. Written and deleted inside the projection
///     batch that moves the shard's progression, so a park and an unpark commit with the progression they explain.
/// </summary>
/// <remarks>
///     <para>
///         Keyed by the projection's name and version, the shard key, the slice's tenant and the identity's canonical
///         text (<c>ParkedIdentityText.Format</c>). The tenant is never null: it is the tenant of the slice that
///         failed, the default tenant in a single-tenant store.
///     </para>
///     <para>
///         The primary key is nonclustered, because with unicode keys (#935) the five columns can pass SQL Server's
///         900 byte limit for a clustered key. A nonclustered key may be 1,700 bytes. The table holds at most
///         <c>MaxParkedIdentitiesPerShard</c> rows per shard, so a heap is fine.
///     </para>
///     <para>
///         No copy of the event: it is still in <c>pc_events</c>.
///     </para>
/// </remarks>
internal class ParkedIdentitiesTable : Polecat.Storage.KeyedTable
{
    public const string TableName = "pc_parked_identities";

    /// <summary>The widest projection name, in characters.</summary>
    public const int ProjectionNameLength = 200;

    /// <summary>The widest shard key, in characters.</summary>
    public const int ShardKeyLength = 50;

    public ParkedIdentitiesTable(EventGraph eventGraph)
        : base(new SqlServerObjectName(eventGraph.DatabaseSchemaName, TableName))
    {
        AddColumn("projection_name", eventGraph.KeyText(ProjectionNameLength)).AsPrimaryKey().NotNull();
        AddColumn("projection_version", "int").AsPrimaryKey().NotNull();
        AddColumn("shard_key", eventGraph.KeyText(ShardKeyLength)).AsPrimaryKey().NotNull();
        AddColumn("tenant_id", eventGraph.KeyText()).AsPrimaryKey().NotNull();
        AddColumn("identity_text", eventGraph.KeyText()).AsPrimaryKey().NotNull();

        AddColumn("identity_type", "varchar(20)").NotNull();
        AddColumn("first_failing_seq", "bigint").NotNull();
        AddColumn("resume_after", "bigint").NotNull();

        // The enum's name, as pc_event_progression's failure_category is
        AddColumn("failure_category", "varchar(50)").AllowNulls();
        AddColumn("exception_type", "nvarchar(500)").AllowNulls();
        AddColumn("exception_message", "nvarchar(max)").AllowNulls();
        AddColumn("event_type", "nvarchar(500)").AllowNulls();
        AddColumn("event_id", "uniqueidentifier").AllowNulls();
        AddColumn("failing_stream", "nvarchar(500)").AllowNulls();
        AddColumn("parked_at", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");
        AddColumn("replay_attempts", "int").NotNull().DefaultValue(0);
        AddColumn("last_attempt_at", "datetimeoffset").AllowNulls();
        AddColumn("last_attempt_error", "nvarchar(max)").AllowNulls();
        AddColumn("requested_action", "varchar(20)").AllowNulls();
        AddColumn("requested_at", "datetimeoffset").AllowNulls();

        PrimaryKeyName = "pk_pc_parked_identities";
        PrimaryKeyIsClustered = false;
    }
}
