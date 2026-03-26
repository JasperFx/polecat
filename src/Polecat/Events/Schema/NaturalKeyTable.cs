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
            $"pc_natural_key_{naturalKey.AggregateType.Name.ToLowerInvariant()}"))
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
            AddColumn("tenant_id", "varchar(250)").NotNull().AsPrimaryKey();
        }

        AddColumn("is_archived", "bit").NotNull().DefaultValue(0);

        // Foreign key to streams table, accounting for conjoined tenancy and archived stream partitioning
        if (events.UseArchivedStreamPartitioning)
        {
            if (isConjoined)
            {
                // FK must include tenant_id and is_archived to match pc_streams composite PK
                // NOTE: Composite FK with tenant_id is intentionally omitted for conjoined tenancy
                // due to Weasel.SqlServer's ForeignKey sorting ColumnNames/LinkedNames alphabetically,
                // which breaks the column mapping when PK order doesn't match alphabetical order.
            }
            else
            {
                // FK to pc_streams must include is_archived when streams table is partitioned
                ForeignKeys.Add(new ForeignKey($"fk_{Identifier.Name}_stream_is_archived")
                {
                    ColumnNames = [streamCol, "is_archived"],
                    LinkedNames = ["id", "is_archived"],
                    LinkedTable = new SqlServerObjectName(events.DatabaseSchemaName, StreamsTable.TableName),
#pragma warning disable CS0618
                    OnDelete = CascadeAction.Cascade
#pragma warning restore CS0618
                });
            }
        }
        else if (isConjoined)
        {
            // NOTE: Composite FK to pc_streams is intentionally omitted for conjoined tenancy.
            // Weasel.SqlServer's ForeignKey sorts ColumnNames/LinkedNames alphabetically,
            // which breaks the column mapping when the PK order (tenant_id, id) doesn't match
            // alphabetical order (id, tenant_id). Referential integrity is enforced by the
            // event store's application logic (stream is always inserted before events).
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
