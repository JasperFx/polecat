using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for <c>pc_dcb_tag_version</c> — one row per DCB tag boundary, carrying a
///     monotonic counter of the commits that have touched it.
/// </summary>
/// <remarks>
///     <para>
///         The counter is what makes a DCB boundary safe under concurrency. The check it replaced was a
///         <c>SELECT ... WHERE EXISTS</c> over the tag tables, which at READ COMMITTED is a non-locking
///         predicate read: concurrent savers each ran it before any of them committed, so none of them
///         could see the others, and their event INSERTs did not collide either because a boundary
///         routinely spans distinct streams. Every racer committed. See gh-515.
///     </para>
///     <para>
///         Against a single row the check becomes a write conflict instead of a read. The row is
///         captured at <c>FetchForWritingByTags</c> time and asserted-and-bumped at save time by an
///         <c>UPDATE ... WHERE version = @captured</c>, so concurrent savers queue on the row's own
///         exclusive lock rather than racing past each other.
///     </para>
///     <para>
///         One table across every registered tag type, discriminated by <c>tag_table</c> (the tag type's
///         <c>TableSuffix</c>) — hence the stringified <c>tag_value</c>, see
///         <see cref="Polecat.Events.Dcb.TagValueStringifier" />. <c>tenant_id</c> is in the key
///         unconditionally: under single tenancy every row simply carries the default tenant.
///     </para>
/// </remarks>
internal class DcbTagVersionTable : Table
{
    public const string TableName = "pc_dcb_tag_version";

    public DcbTagVersionTable(EventGraph events)
        : base(new SqlServerObjectName(events.DatabaseSchemaName, TableName))
    {
        // varchar(250) matches the width EventTagTable gives a string tag value, so a stringified
        // value that fits its own tag table fits here too.
        AddColumn("tag_table", "varchar(250)").NotNull().AsPrimaryKey();
        AddColumn("tag_value", "varchar(250)").NotNull().AsPrimaryKey();
        AddColumn(JasperFx.StorageConstants.TenantIdColumn, "varchar(250)").NotNull().AsPrimaryKey();
        AddColumn("version", "bigint").NotNull();

        PrimaryKeyName = "pk_pc_dcb_tag_version";
    }
}
