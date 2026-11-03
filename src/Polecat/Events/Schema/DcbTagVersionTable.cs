using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Events.Schema;

/// <summary>
///     Weasel table definition for <c>pc_dcb_tag_version</c> — one row per DCB tag boundary, carrying a
///     version that changes on every commit that touches it.
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
///     <para>
///         #890: <c>tag_value</c> is the application's value as text, and a tag can be personal data, so a
///         tombstone deletes the row once no event carries the tag. That is only safe because the version
///         never repeats: it comes from one sequence per schema, not from counting up the row. A counter
///         that restarted at 1 when the row came back could match a capture of the row that was deleted,
///         and a conflict would be lost. See <c>StreamTagVersionBump</c>.
///     </para>
/// </remarks>
internal class DcbTagVersionTable : Table
{
    public const string TableName = "pc_dcb_tag_version";
    public const string SequenceName = "pc_dcb_tag_version_sequence";

    /// <summary>
    ///     Above any value an older store can hold. Rows written before #890 keep their counters, and those
    ///     count the commits on one tag from 1, so a sequence starting at 1 could give a row a value it has
    ///     held before. A constant, rather than the table's highest value when the sequence is created,
    ///     because <c>CREATE SEQUENCE</c> takes only a constant and a script has to be able to run without
    ///     reading the data. It leaves the rest of the bigint range, about 9,000 times this much.
    /// </summary>
    public const long SequenceStart = 1_000_000_000_000_000;

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
