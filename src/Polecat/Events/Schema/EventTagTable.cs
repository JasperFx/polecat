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

        PrimaryKeyName = $"pk_pc_event_tag_{registration.TableSuffix}";

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
