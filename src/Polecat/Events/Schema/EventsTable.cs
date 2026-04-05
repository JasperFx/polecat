using JasperFx.Events;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for pc_events — stores individual events.
/// </summary>
internal class EventsTable : Table
{
    public const string TableName = "pc_events";

    public EventsTable(EventGraph events)
        : base(new SqlServerObjectName(events.DatabaseSchemaName, TableName))
    {
        // Global sequence — auto-incrementing primary key
        AddColumn("seq_id", "bigint").AsPrimaryKey().AutoNumber();

        // Event identity
        AddColumn("id", "uniqueidentifier").NotNull();

        // Stream reference
        var streamIdType = events.StreamIdentity == StreamIdentity.AsGuid
            ? "uniqueidentifier"
            : "varchar(250)";
        AddColumn("stream_id", streamIdType).NotNull();

        // Version within the stream
        AddColumn("version", "bigint").NotNull();

        // Event data — SQL Server 2025 native JSON type by default
        AddColumn("data", events.JsonColumnType).NotNull();

        // Event type name for deserialization
        AddColumn("type", "varchar(500)").NotNull();

        // Timestamp
        AddColumn("timestamp", "datetimeoffset")
            .NotNull()
            .DefaultValueByExpression("SYSDATETIMEOFFSET()");

        // Tenant id
        AddColumn("tenant_id", "varchar(250)")
            .NotNull()
            .DefaultValueByString(Tenancy.DefaultTenantId);

        // .NET type for deserialization
        AddColumn("dotnet_type", "varchar(500)").AllowNulls();

        // Optional metadata columns
        if (events.EventOptions.EnableCorrelationId)
        {
            AddColumn("correlation_id", "varchar(250)").AllowNulls();
        }

        if (events.EventOptions.EnableCausationId)
        {
            AddColumn("causation_id", "varchar(250)").AllowNulls();
        }

        if (events.EventOptions.EnableHeaders)
        {
            AddColumn("headers", events.JsonColumnType).AllowNulls();
        }

        // Archive flag
        var archiveColumn = AddColumn("is_archived", "bit").NotNull().DefaultValue(0);

        // Apply SQL Server RANGE partitioning on is_archived if enabled.
        // This separates active (is_archived=0) and archived (is_archived=1)
        // events into different partitions for improved query performance.
        if (events.UseArchivedStreamPartitioning)
        {
            // SQL Server requires the partition column to be in the clustered index (PK).
            archiveColumn.AsPrimaryKey();

            // Use Weasel.SqlServer 8.10.0's proper partitioning API which generates
            // CREATE PARTITION FUNCTION + CREATE PARTITION SCHEME + ON clause DDL.
            var partitioning = new RangePartitioning("is_archived", "bit");
            partitioning.AddBoundary(1);
            SqlServerPartitioning = partitioning;
        }

        // Unique constraint: one version per stream (with tenant for conjoined)
        // When using archived stream partitioning, SQL Server requires the partition
        // column (is_archived) to be included in all unique indexes.
        if (events.TenancyStyle == TenancyStyle.Conjoined)
        {
            var uniqueColumns = events.UseArchivedStreamPartitioning
                ? new[] { "tenant_id", "stream_id", "version", "is_archived" }
                : new[] { "tenant_id", "stream_id", "version" };

            Indexes.Add(new IndexDefinition("ix_pc_events_stream_and_version")
            {
                IsUnique = true,
                Columns = uniqueColumns
            });

            // NOTE: Composite FK to pc_streams is intentionally omitted for conjoined tenancy.
            // Weasel.SqlServer's ForeignKey sorts ColumnNames/LinkedNames alphabetically,
            // which breaks the column mapping when the PK order (tenant_id, id) doesn't match
            // alphabetical order (id, tenant_id). Referential integrity is enforced by the
            // event store's application logic (stream is always inserted before events).
        }
        else
        {
            var uniqueCols = events.UseArchivedStreamPartitioning
                ? new[] { "stream_id", "version", "is_archived" }
                : new[] { "stream_id", "version" };

            Indexes.Add(new IndexDefinition("ix_pc_events_stream_and_version")
            {
                IsUnique = true,
                Columns = uniqueCols
            });

            // Foreign key to streams table
            ForeignKeys.Add(new ForeignKey("fk_pc_events_stream_id")
            {
                ColumnNames = ["stream_id"],
                LinkedNames = ["id"],
                LinkedTable = new SqlServerObjectName(events.DatabaseSchemaName, StreamsTable.TableName),
#pragma warning disable CS0618 // CascadeAction obsolete in Weasel.SqlServer
                OnDelete = CascadeAction.Cascade
#pragma warning restore CS0618
            });
        }
    }
}
