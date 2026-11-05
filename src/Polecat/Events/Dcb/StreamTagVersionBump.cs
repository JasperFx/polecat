using JasperFx.Events.Tags;
using Polecat.Internal;
using Weasel.SqlServer;

namespace Polecat.Events.Dcb;

/// <summary>
///     Moves the <c>pc_dcb_tag_version</c> row of every tag on one stream's events, for an operation that
///     changes what a boundary over those tags reads without appending anything: archiving, un-archiving
///     (#841) and tombstoning (#890). The boundary check only ever compares that value, so left alone, a
///     boundary would save straight through the change. The save that reads the value again sees the
///     current events, so the cost is a retry and never a wrong answer.
/// </summary>
/// <remarks>
///     <para>
///         All of it finds the tags through the stream's events, so it has to run ahead of anything
///         deleting them. The command must bind <c>@id</c> and <c>@tenant_id</c>.
///     </para>
///     <para>
///         One value is drawn per command rather than one per row. Nothing needs them to differ, because a
///         capture is only ever compared with its own row, and <c>NEXT VALUE FOR</c> isn't allowed in a
///         <c>MERGE</c> anyway.
///     </para>
/// </remarks>
internal static class StreamTagVersionBump
{
    private const string Version = "@dcb_version";

    /// <summary>
    ///     The bump an archive makes: a tag with no row gets one. The value is still in the stream's events
    ///     and they are staying, so this adds no copy of anything that wasn't already there.
    /// </summary>
    /// <remarks>
    ///     <c>HOLDLOCK</c> because this is a set-based create-or-bump, where <c>DcbTagVersionPhase</c> can
    ///     insert one row and catch the duplicate. It takes the range locks that phase avoids, which it can
    ///     afford for the same reason it is a separate statement rather than part of the phase: archiving
    ///     is not common, and appending is.
    /// </remarks>
    public static void WriteSql(ICommandBuilder builder, EventGraph events)
    {
        if (events.TagTypes.Count == 0) return;

        builder.Append($"\nDECLARE {Version} bigint = NEXT VALUE FOR {events.DcbTagVersionSequenceName};\n");

        foreach (var registration in events.TagTypes)
        {
            var tagValue = TagValueStringifier.ToSql(registration.SimpleType, "t.value");

            // CONVERT because a dictionary parameter binds as nvarchar, and comparing that to the
            // varchar key of pc_dcb_tag_version would convert the column and range-lock a scan of it.
            builder.Append($"""

                MERGE {events.DcbTagVersionTableName} WITH (HOLDLOCK) AS target
                USING (
                    SELECT DISTINCT '{SqlEscaping.LiteralBody(registration.TableSuffix)}' AS tag_table,
                        {tagValue} AS tag_value, CONVERT(varchar(250), @tenant_id) AS tenant_id
                    FROM {events.TagTableName(registration)} t
                    INNER JOIN {events.EventsTableName} e ON {events.TagToEventJoin("t", "e")}
                    WHERE e.stream_id = @id AND e.tenant_id = @tenant_id
                ) AS source
                ON target.tag_table = source.tag_table AND target.tag_value = source.tag_value
                    AND target.tenant_id = source.tenant_id
                WHEN MATCHED THEN UPDATE SET version = {Version}
                WHEN NOT MATCHED THEN INSERT (tag_table, tag_value, tenant_id, version)
                    VALUES (source.tag_table, source.tag_value, source.tenant_id, {Version});

                """);
        }
    }

    /// <summary>
    ///     What a tombstone does: move the row of every tag on the stream, then delete the ones no other
    ///     stream carries, so the value doesn't outlive the events it came from. It never creates a row,
    ///     which would write the value down again in the middle of erasing it.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>The update comes first and is a statement of its own, and that is what makes the delete
    ///         safe against a concurrent append.</b> A save takes the row before it inserts any event (see
    ///         <c>DcbTagVersionPhase</c>). So once the update holds the row, whoever had it has committed
    ///         and their events can be seen, and whoever wants it is waiting behind this transaction and
    ///         hasn't inserted anything. The check for another stream is therefore complete, and a save
    ///         let through afterwards finds no row. If it had a captured version it loses, and if it was
    ///         only bumping it creates a row with a new value. In one statement this doesn't hold: the
    ///         check could read the other streams before the row's lock was granted, and under READ
    ///         COMMITTED SNAPSHOT it would read them as of the statement's start whatever it waited for.
    ///     </para>
    ///     <para>
    ///         <c>version = @dcb_version</c> limits the delete to rows the update reached, because only
    ///         those are locked. A row some other save created after the update isn't, and its events
    ///         may not be in yet.
    ///     </para>
    ///     <para>
    ///         A tag with no row is left with none. A boundary that read such a tag captured 0, and has no
    ///         row to lose against, so the save checks the events it read instead. See
    ///         <see cref="DcbTagVersionCapture.ReadFor" />.
    ///     </para>
    ///     <para>
    ///         The other stream's events count whether they are archived or not. They are still stored,
    ///         tag rows included, and un-archiving them would bring the tag back.
    ///     </para>
    /// </remarks>
    public static void WriteErasureSql(ICommandBuilder builder, EventGraph events)
    {
        if (events.TagTypes.Count == 0) return;

        builder.Append($"\nDECLARE {Version} bigint = NEXT VALUE FOR {events.DcbTagVersionSequenceName};\n");

        // In the order DcbTagVersionPhase takes rows, so far as a set-based statement can.
        var registrations = events.TagTypes.OrderBy(x => x.TableSuffix, StringComparer.Ordinal).ToArray();

        // #925: one row at a time, in the order the phase sorts them. A set-based UPDATE takes its row locks
        // in whatever order the plan produces them, and a tagged append taking the same rows in
        // (tag_table, tag_value) order then crossed it: each held a row the other wanted. BIN2 is the
        // ordinal order DcbTagVersionPhase sorts by, which the column's own collation is not.
        builder.Append("DECLARE @erase_next varchar(250); DECLARE @erase_prev varchar(250);\n");

        var index = 0;
        foreach (var registration in registrations)
        {
            var tags = $"@erase_tags_{index++}";
            var tableSuffix = SqlEscaping.LiteralBody(registration.TableSuffix);

            builder.Append($"""

                DECLARE {tags} TABLE (tag_value varchar(250) COLLATE Latin1_General_BIN2 NOT NULL PRIMARY KEY);
                INSERT INTO {tags} (tag_value)
                SELECT DISTINCT CONVERT(varchar(250), {TagValueStringifier.ToSql(registration.SimpleType, "t.value")})
                FROM {events.EventsTableName} e
                INNER JOIN {events.TagTableName(registration)} t ON {events.TagToEventJoin("t", "e")}
                WHERE e.stream_id = @id AND e.tenant_id = @tenant_id;

                SET @erase_prev = NULL;
                WHILE 1 = 1
                BEGIN
                    SET @erase_next = NULL;
                    SELECT TOP (1) @erase_next = tag_value FROM {tags}
                        WHERE @erase_prev IS NULL OR tag_value > @erase_prev ORDER BY tag_value;
                    IF @erase_next IS NULL BREAK;
                    UPDATE {events.DcbTagVersionTableName} SET version = {Version}
                        WHERE tag_table = '{tableSuffix}' AND tag_value = @erase_next AND tenant_id = @tenant_id;
                    SET @erase_prev = @erase_next;
                END;

                """);
        }

        foreach (var registration in registrations)
        {
            builder.Append($"""

                DELETE v
                {RowsOfTheStreamsTags(events, registration)}
                WHERE v.version = {Version}
                    AND NOT EXISTS (
                        SELECT 1 FROM {events.TagTableName(registration)} c
                        INNER JOIN {events.EventsTableName} o ON {events.TagToEventJoin("c", "o")}
                        WHERE c.value = s.value AND o.tenant_id = @tenant_id AND o.stream_id <> @id
                    );

                """);
        }
    }

    // A loop from the stream's events to exact keys, by hint, because the table is small until it isn't.
    // Left to itself the optimizer reads every row of the tag type, taking update locks as it goes, and two
    // tombstones with nothing in common then deadlock on each other's rows. Both forms of the value come out
    // of the derived table: stringified to reach pc_dcb_tag_version, and as stored to seek the tag table.
    private static string RowsOfTheStreamsTags(EventGraph events, ITagTypeRegistration registration)
        => $"""
            FROM (
                    SELECT DISTINCT t.value, {TagValueStringifier.ToSql(registration.SimpleType, "t.value")} AS tag_value
                    FROM {events.EventsTableName} e
                    INNER JOIN {events.TagTableName(registration)} t ON {events.TagToEventJoin("t", "e")}
                    WHERE e.stream_id = @id AND e.tenant_id = @tenant_id
                ) s
                INNER LOOP JOIN {events.DcbTagVersionTableName} v WITH (FORCESEEK)
                    ON v.tag_table = '{SqlEscaping.LiteralBody(registration.TableSuffix)}'
                        AND v.tag_value = s.tag_value AND v.tenant_id = @tenant_id
            """;
}
