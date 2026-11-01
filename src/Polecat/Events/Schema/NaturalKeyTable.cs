using JasperFx.Events;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for pc_natural_key_{aggregatetype} — stores natural key
///     to stream id mappings for domain-meaningful lookups.
/// </summary>
internal class NaturalKeyTable : Table
{
    public NaturalKeyTable(EventGraph events, NaturalKeyDefinition naturalKey)
        : base(new SqlServerObjectName(events.DatabaseSchemaName,
            EventGraph.NaturalKeyTableNameFor(naturalKey.AggregateType)))
    {
        var columnType = naturalKey.InnerType == typeof(int) ? "int"
            : naturalKey.InnerType == typeof(long) ? "bigint"
            : "nvarchar(200)";

        AddColumn("natural_key_value", columnType).AsPrimaryKey().NotNull();

        var isConjoined = events.TenancyStyle == TenancyStyle.Conjoined;

        var streamCol = events.StreamIdentity == StreamIdentity.AsGuid ? "stream_id" : "stream_key";
        var streamColType = events.StreamIdentity == StreamIdentity.AsGuid ? "uniqueidentifier" : "varchar(250)";

        AddColumn(streamCol, streamColType).NotNull();

        // Tenancy support - tenant_id is part of PK so same natural key can exist in different tenants
        if (isConjoined)
        {
            AddColumn(JasperFx.StorageConstants.TenantIdColumn, "varchar(250)").NotNull().AsPrimaryKey();
        }

        AddColumn("is_archived", "bit").NotNull().DefaultValue(0);

        // Foreign key to streams table, accounting for conjoined tenancy. UseArchivedStreamPartitioning
        // changes nothing here: it partitions pc_events only, so pc_streams' key is still (id). This used
        // to reference (id, is_archived) under that flag, a column list with no unique index behind it,
        // which SQL Server refuses with Msg 1776 and so made the combination impossible to provision.
        if (isConjoined)
        {
            // NOTE: Composite FK to pc_streams is intentionally omitted for conjoined tenancy.
            // It was first ruled out because Weasel.SqlServer's ForeignKey sorted its column
            // lists, which it no longer does. It stays out because adding one now is a migration
            // that fails on any row orphaned by a tombstone before #865, and because under
            // UseTenantPartitionedEvents the pc_streams key also has a tenant_ordinal this table
            // does not carry. Referential integrity is enforced by the event store's application
            // logic (stream is always inserted before events), and nothing cascades: see
            // EventGraph.NaturalKeyRowsCascadeWithStreams.
        }
        else
        {
            // FK to pc_streams with CASCADE delete
            ForeignKeys.Add(new ForeignKey($"fk_{Identifier.Name}_stream")
            {
                ColumnNames = [streamCol],
                LinkedNames = ["id"],
                LinkedTable = new SqlServerObjectName(events.DatabaseSchemaName, StreamsTable.TableName),
#pragma warning disable CS0618
                OnDelete = CascadeAction.Cascade
#pragma warning restore CS0618
            });
        }
    }
}
