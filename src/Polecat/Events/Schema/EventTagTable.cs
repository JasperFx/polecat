using JasperFx.Events.Tags;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for pc_event_tag_{suffix} — stores tag values for DCB support.
///     One table is created per registered tag type. Composite PK is (value, seq_id),
///     with tenant_id added to the PK for conjoined tenancy.
/// </summary>
internal class EventTagTable : Table
{
    public EventTagTable(EventGraph events, ITagTypeRegistration registration)
        : base(new SqlServerObjectName(events.DatabaseSchemaName, $"pc_event_tag_{registration.TableSuffix}"))
    {
        var sqlType = SqlServerTypeFor(registration.SimpleType);
        var isConjoined = events.TenancyStyle == TenancyStyle.Conjoined;

        AddColumn("value", sqlType).NotNull().AsPrimaryKey();

        // Add tenant_id to PK for conjoined tenancy to enable tenant-scoped tag queries
        if (isConjoined)
        {
            AddColumn("tenant_id", "varchar(250)").NotNull().AsPrimaryKey();
        }

        AddColumn("seq_id", "bigint").NotNull().AsPrimaryKey();

        if (events.UseArchivedStreamPartitioning)
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
            // (event row is inserted in the same command as its tag rows).
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
    }

    private static string SqlServerTypeFor(Type type)
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
