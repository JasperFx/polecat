using JasperFx.Events.Tags;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for pc_event_tag_{suffix} — stores tag values for DCB support.
///     One table is created per registered tag type. Composite PK is (value, seq_id),
///     with tenant_id added to the PK for conjoined tenancy.
/// </summary>
/// <remarks>
///     <para>
///         The primary key answers "which events carry this tag". The opposite question, "which tag
///         rows belong to this event", is asked by every delete from <c>pc_events</c> (the foreign key's
///         cascade) and by every statement that starts from a stream's events (#841), so
///         <c>ix_pc_event_tag_{suffix}_seq_id</c> leads on <c>seq_id</c>. Without it each deleted event
///         scans the whole tag table.
///     </para>
///     <para>
///         #842: under <see cref="EventGraph.UseTenantPartitionedEvents" /> <c>seq_id</c> is numbered
///         per tenant, so the row also carries <c>tenant_ordinal</c>. That pair is the
///         <c>pc_events</c> primary key, which gives the foreign key a target and gives a join a
///         partition-eliminated seek rather than one seek per tenant. The table itself is deliberately
///         not partitioned: the cascade already takes a removed tenant's rows, and partitioning would
///         cost every tag type another partition function and another SPLIT per tenant.
///     </para>
/// </remarks>
internal class EventTagTable : Table
{
    public EventTagTable(EventGraph events, ITagTypeRegistration registration)
        : base(new SqlServerObjectName(events.DatabaseSchemaName, EventGraph.TagTableNameFor(registration)))
    {
        var sqlType = SqlServerTypeFor(registration.SimpleType);
        var isConjoined = events.TenancyStyle == TenancyStyle.Conjoined;

        AddColumn("value", sqlType).NotNull().AsPrimaryKey();

        // Add tenant_id to PK for conjoined tenancy to enable tenant-scoped tag queries
        if (isConjoined)
        {
            AddColumn(JasperFx.StorageConstants.TenantIdColumn, "varchar(250)").NotNull().AsPrimaryKey();
        }

        AddColumn("seq_id", "bigint").NotNull().AsPrimaryKey();

        var indexColumns = new List<string> { "seq_id" };

        if (events.UseTenantPartitionedEvents)
        {
            var ordinalColumn = events.TenantPartitionManager.Column;
            AddColumn(ordinalColumn, "int").NotNull();
            indexColumns.Add(ordinalColumn);

            // The names match on both sides, so Weasel's alphabetical sort of the two lists cannot pair a
            // column with the wrong partner the way it does for (tenant_id, id) -> (id, tenant_id).
            ForeignKeys.Add(new ForeignKey($"fk_pc_event_tag_{registration.TableSuffix}_seq_id")
            {
                ColumnNames = ["seq_id", ordinalColumn],
                LinkedNames = ["seq_id", ordinalColumn],
                LinkedTable = new SqlServerObjectName(events.DatabaseSchemaName, EventsTable.TableName),
#pragma warning disable CS0618
                OnDelete = CascadeAction.Cascade
#pragma warning restore CS0618
            });
        }
        else if (events.UseArchivedStreamPartitioning)
        {
            // When pc_events is partitioned by is_archived, its PK includes is_archived,
            // so an FK from pc_event_tag_*.seq_id alone won't match a unique key in
            // pc_events. Add is_archived here so the tag row's lifecycle matches its
            // owning event partition.
            //
            // NOTE: A composite FK (seq_id, is_archived) → pc_events(seq_id, is_archived)
            // is intentionally omitted. Weasel.SqlServer's ForeignKey sorts
            // ColumnNames/LinkedNames alphabetically, which produces a FK column-list
            // order that SQL Server rejects when it doesn't match the referenced PK
            // column order (the PK is created as (seq_id, is_archived) in code order,
            // not alphabetical). See the matching note on the streams FK in EventsTable.cs.
            // Referential integrity is enforced by the event store's application logic
            // (event row is inserted in the same command as its tag rows), which also means
            // nothing cascades: see EventGraph.TagRowsCascadeWithEvents.
            //
            // Nothing keeps this flag in step with pc_events.is_archived once the row is
            // written, so no read may trust it (#841). Only pc_events answers "is this live".
            AddColumn("is_archived", "bit").NotNull().DefaultValue(0).AsPrimaryKey();
        }
        else
        {
            ForeignKeys.Add(new ForeignKey($"fk_pc_event_tag_{registration.TableSuffix}_seq_id")
            {
                ColumnNames = ["seq_id"],
                LinkedNames = ["seq_id"],
                LinkedTable = new SqlServerObjectName(events.DatabaseSchemaName, EventsTable.TableName),
#pragma warning disable CS0618
                OnDelete = CascadeAction.Cascade
#pragma warning restore CS0618
            });
        }

        PrimaryKeyName = $"pk_pc_event_tag_{registration.TableSuffix}";

        Indexes.Add(new IndexDefinition($"ix_pc_event_tag_{registration.TableSuffix}_seq_id")
        {
            Columns = indexColumns.ToArray()
        });
    }

    internal static string SqlServerTypeFor(Type type)
    {
        if (type == typeof(Guid)) return "uniqueidentifier";
        if (type == typeof(string)) return "varchar(250)";
        if (type == typeof(int)) return "int";
        if (type == typeof(long)) return "bigint";
        if (type == typeof(short)) return "smallint";

        throw new ArgumentOutOfRangeException(nameof(type),
            $"Unsupported tag value type '{type.Name}' for SQL Server event tag table.");
    }
}
