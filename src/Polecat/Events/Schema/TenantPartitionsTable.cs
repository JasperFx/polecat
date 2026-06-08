using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Registry mapping each tenant id to a compact integer partition id, used to name that tenant's
///     per-tenant event sequence (<c>pc_events_sequence_{partition_id}</c>). Only created when
///     <see cref="EventGraph.UseTenantPartitionedEvents" /> is enabled (#163 Phase 1). The per-tenant
///     sequence objects themselves are created on demand the first time a tenant appends events.
/// </summary>
internal class TenantPartitionsTable : Table
{
    public const string TableName = "pc_tenant_partitions";

    public TenantPartitionsTable(EventGraph eventGraph)
        : base(new SqlServerObjectName(eventGraph.DatabaseSchemaName, TableName))
    {
        AddColumn("tenant_id", "varchar(250)").AsPrimaryKey().NotNull();

        // Compact, stable per-tenant ordinal assigned on first append. IDENTITY keeps allocation
        // server-side and contention-free; the value names the tenant's pc_events_sequence_{id}.
        AddColumn("partition_id", "int").NotNull().AutoNumber();

        AddColumn("created", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        Indexes.Add(new IndexDefinition("ix_pc_tenant_partitions_partition_id")
        {
            IsUnique = true,
            Columns = ["partition_id"]
        });
    }
}
