using JasperFx.Events;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

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

        // Event data — SQL Server 2025 JSON type
        AddColumn("data", "nvarchar(max)").NotNull();

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
            AddColumn("headers", "nvarchar(max)").AllowNulls();
        }

        // Archive flag
        AddColumn("is_archived", "bit").NotNull().DefaultValue(0);

        // Unique constraint: one version per stream (with tenant for conjoined)
        if (events.TenancyStyle == TenancyStyle.Conjoined)
        {
            Indexes.Add(new IndexDefinition("ix_pc_events_stream_and_version")
            {
                IsUnique = true,
                Columns = ["tenant_id", "stream_id", "version"]
            });

            // NOTE: Composite FK to pc_streams is intentionally omitted for conjoined tenancy.
            // Weasel.SqlServer's ForeignKey sorts ColumnNames/LinkedNames alphabetically,
            // which breaks the column mapping when the PK order (tenant_id, id) doesn't match
            // alphabetical order (id, tenant_id). Referential integrity is enforced by the
            // event store's application logic (stream is always inserted before events).
        }
        else
        {
            Indexes.Add(new IndexDefinition("ix_pc_events_stream_and_version")
            {
                IsUnique = true,
                Columns = ["stream_id", "version"]
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
