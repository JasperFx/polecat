using JasperFx.Events.Tags;
using Polecat.Internal;
using Polecat.Linq.SqlGeneration;
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
            var tagValue = TagValueSql.ToSql(registration.SimpleType, "t.value");

            // CONVERT because a dictionary parameter binds as nvarchar, and comparing that to the
            // varchar key of pc_dcb_tag_version would convert the column and range-lock a scan of it.
            // #973: under a scope that keeps per-event-type versions, the archive moves those too, or a
            // boundary checked by event type would save straight through it.
            var byEventType = events.RecordsDcbEventTypeVersions
                ? $"""

                    UNION
                    SELECT DISTINCT {EventTypeKeySql(registration, "e")} AS tag_table,
                        {tagValue} AS tag_value, CONVERT({events.KeyText()}, @tenant_id) AS tenant_id
                    FROM {events.TagTableName(registration)} t
                    INNER JOIN {events.EventsTableName} e ON {events.TagToEventJoin("t", "e")}
                    WHERE e.stream_id = @id AND e.tenant_id = @tenant_id AND {FitsKeySql(registration, "e")}
                """
                : string.Empty;

            builder.Append($"""

                MERGE {events.DcbTagVersionTableName} WITH (HOLDLOCK) AS target
                USING (
                    SELECT DISTINCT '{SqlEscaping.LiteralBody(registration.TableSuffix)}' AS tag_table,
                        {tagValue} AS tag_value, CONVERT({events.KeyText()}, @tenant_id) AS tenant_id
                    FROM {events.TagTableName(registration)} t
                    INNER JOIN {events.EventsTableName} e ON {events.TagToEventJoin("t", "e")}
                    WHERE e.stream_id = @id AND e.tenant_id = @tenant_id{byEventType}
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
    ///     What <c>AssignTagWhere</c> does after it tags the events (polecat#1070, marten#6078): move the
    ///     assigned tag value's row, and its per-event-type rows under a scope that keeps them, creating any
    ///     that are missing. Nothing is written when no event of the tenant matched.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Tagging existing events changes what a boundary over that tag reads without appending anything,
    ///         and a boundary only compares the rows it captured. Without this, a boundary that read the tag
    ///         before the assignment saved straight through it, as if the newly tagged events weren't there.
    ///         Creating a missing row is what catches a boundary that captured 0: it read no row, and finds one
    ///         with a sequence value at save. The value is the one the assignment just wrote into the tag table,
    ///         so the row adds no copy of anything that wasn't already there.
    ///     </para>
    ///     <para>
    ///         <c>MERGE ... HOLDLOCK</c> for the reason <see cref="WriteSql" /> gives: assigning tags is not
    ///         common, and appending is.
    ///     </para>
    /// </remarks>
    /// <param name="builder">The assignment's command, after its <c>INSERT</c>.</param>
    /// <param name="events">The event store's configuration.</param>
    /// <param name="registration">The assigned tag's type.</param>
    /// <param name="tagValue">The value in <see cref="TagValueStringifier" />'s form.</param>
    /// <param name="tenantId">The session's tenant, which owns the rows.</param>
    /// <param name="where">The assignment's filter over <c>pc_events e</c>.</param>
    /// <param name="conjoined">Limit the matching events to <paramref name="tenantId" />'s, as the assignment is.</param>
    public static void WriteAssignedSql(ICommandBuilder builder, EventGraph events,
        ITagTypeRegistration registration, string tagValue, string tenantId, ISqlFragment where, bool conjoined)
    {
        builder.Append($"\nDECLARE {Version} bigint = NEXT VALUE FOR {events.DcbTagVersionSequenceName};\n");
        builder.Append($"DECLARE @assign_value {events.KeyText()} = ");
        builder.AppendParameter(tagValue, events.KeyDbType);
        builder.Append($"; DECLARE @assign_tenant {events.KeyText()} = ");
        builder.AppendParameter(tenantId, events.KeyDbType);
        builder.Append(";\n");

        void matching(bool byEventType)
        {
            builder.Append($"FROM {events.EventsTableName} e WHERE (");
            where.Apply(builder);
            builder.Append(")");
            if (conjoined) builder.Append(" AND e.tenant_id = @assign_tenant");
            if (byEventType) builder.Append($" AND {FitsKeySql(registration, "e")}");
        }

        builder.Append($"""
            MERGE {events.DcbTagVersionTableName} WITH (HOLDLOCK) AS target
            USING (
                SELECT DISTINCT CONVERT(varchar(250), '{SqlEscaping.LiteralBody(registration.TableSuffix)}') AS tag_table
            """);
        builder.Append("\n    ");
        matching(false);

        // #973: under a scope that keeps per-event-type versions, the assignment moves those too, or a
        // boundary checked by event type would save straight through it.
        if (events.RecordsDcbEventTypeVersions)
        {
            builder.Append($"\n    UNION\n    SELECT DISTINCT {EventTypeKeySql(registration, "e")} AS tag_table\n    ");
            matching(true);
        }

        builder.Append("""

            ) AS source
            ON target.tag_table = source.tag_table AND target.tag_value = @assign_value
                AND target.tenant_id = @assign_tenant
            """);
        builder.Append($"""

            WHEN MATCHED THEN UPDATE SET version = {Version}
            WHEN NOT MATCHED THEN INSERT (tag_table, tag_value, tenant_id, version)
                VALUES (source.tag_table, @assign_value, @assign_tenant, {Version});

            """);
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

        // #925: one row at a time, in the order DcbTagVersionPhase sorts them. A set-based UPDATE takes its
        // row locks in whatever order the plan produces them, and a tagged append taking the same rows in
        // (tag_table, tag_value) order then crossed it: each held a row the other wanted. BIN2 is the ordinal
        // order the phase sorts by, which the columns' own collation is not.
        //
        // #973: the per-event-type rows are in the same list, keyed "{suffix}:{type}", so they are taken in the
        // same single order as the tag value rows, as the phase takes them. They are erased whatever the
        // configured DcbConflictScope: the tag value is the application's data either way, and a store that
        // ran under another scope earlier may still hold them.
        builder.Append($"""

            DECLARE @erase_rows TABLE (
                tag_table varchar(250) COLLATE Latin1_General_BIN2 NOT NULL,
                tag_value {events.KeyText()} COLLATE Latin1_General_BIN2 NOT NULL,
                PRIMARY KEY (tag_table, tag_value));
            DECLARE @erase_table varchar(250); DECLARE @erase_value {events.KeyText()};
            DECLARE @erase_prev_table varchar(250); DECLARE @erase_prev_value {events.KeyText()};

            """);

        foreach (var registration in events.TagTypes)
        {
            var tagValue = $"CONVERT({events.KeyText()}, {TagValueSql.ToSql(registration.SimpleType, "t.value")})";

            builder.Append($"""

                INSERT INTO @erase_rows (tag_table, tag_value)
                SELECT DISTINCT '{SqlEscaping.LiteralBody(registration.TableSuffix)}', {tagValue}
                FROM {events.EventsTableName} e
                INNER JOIN {events.TagTableName(registration)} t ON {events.TagToEventJoin("t", "e")}
                WHERE e.stream_id = @id AND e.tenant_id = @tenant_id
                UNION
                SELECT DISTINCT {EventTypeKeySql(registration, "e")}, {tagValue}
                FROM {events.EventsTableName} e
                INNER JOIN {events.TagTableName(registration)} t ON {events.TagToEventJoin("t", "e")}
                WHERE e.stream_id = @id AND e.tenant_id = @tenant_id AND {FitsKeySql(registration, "e")};

                """);
        }

        builder.Append($"""

            WHILE 1 = 1
            BEGIN
                SET @erase_table = NULL;
                SELECT TOP (1) @erase_table = tag_table, @erase_value = tag_value FROM @erase_rows
                    WHERE @erase_prev_table IS NULL OR tag_table > @erase_prev_table
                        OR (tag_table = @erase_prev_table AND tag_value > @erase_prev_value)
                    ORDER BY tag_table, tag_value;
                IF @erase_table IS NULL BREAK;
                UPDATE {events.DcbTagVersionTableName} SET version = {Version}
                    WHERE tag_table = @erase_table AND tag_value = @erase_value AND tenant_id = @tenant_id;
                SET @erase_prev_table = @erase_table;
                SET @erase_prev_value = @erase_value;
            END;

            """);

        foreach (var registration in events.TagTypes)
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

                DELETE v
                {RowsOfTheStreamsTagsByEventType(events, registration)}
                WHERE v.version = {Version}
                    AND NOT EXISTS (
                        SELECT 1 FROM {events.TagTableName(registration)} c
                        INNER JOIN {events.EventsTableName} o ON {events.TagToEventJoin("c", "o")}
                        WHERE c.value = s.value AND o.type = s.type AND o.tenant_id = @tenant_id
                            AND o.stream_id <> @id
                    );

                """);
        }
    }

    /// <summary>
    ///     The SQL form of <see cref="DcbTagVersionKeys.ForEventType" />, from an events alias.
    /// </summary>
    private static string EventTypeKeySql(ITagTypeRegistration registration, string events)
        => $"CONVERT(varchar(250), '{SqlEscaping.LiteralBody(registration.TableSuffix)}{DcbTagVersionKeys.Separator}' + {events}.type)";

    /// <summary>
    ///     A key too long for the column has no row: <see cref="DcbTagVersionKeys.ForEventType" /> refuses to
    ///     make one rather than truncate it, so neither may the SQL.
    /// </summary>
    private static string FitsKeySql(ITagTypeRegistration registration, string events)
        => $"LEN({events}.type) <= {DcbTagVersionKeys.MaxLength - registration.TableSuffix.Length - 1}";

    // A loop from the stream's events to exact keys, by hint, because the table is small until it isn't.
    // Left to itself the optimizer reads every row of the tag type, taking update locks as it goes, and two
    // tombstones with nothing in common then deadlock on each other's rows. Both forms of the value come out
    // of the derived table: stringified to reach pc_dcb_tag_version, and as stored to seek the tag table.
    private static string RowsOfTheStreamsTags(EventGraph events, ITagTypeRegistration registration)
        => $"""
            FROM (
                    SELECT DISTINCT t.value, {TagValueSql.ToSql(registration.SimpleType, "t.value")} AS tag_value
                    FROM {events.EventsTableName} e
                    INNER JOIN {events.TagTableName(registration)} t ON {events.TagToEventJoin("t", "e")}
                    WHERE e.stream_id = @id AND e.tenant_id = @tenant_id
                ) s
                INNER LOOP JOIN {events.DcbTagVersionTableName} v WITH (FORCESEEK)
                    ON v.tag_table = '{SqlEscaping.LiteralBody(registration.TableSuffix)}'
                        AND v.tag_value = s.tag_value AND v.tenant_id = @tenant_id
            """;

    // The per-event-type form of RowsOfTheStreamsTags (#973), carrying the event type so the delete can ask
    // whether another stream still has an event of that type with the tag.
    private static string RowsOfTheStreamsTagsByEventType(EventGraph events, ITagTypeRegistration registration)
        => $"""
            FROM (
                    SELECT DISTINCT t.value, e.type, {TagValueSql.ToSql(registration.SimpleType, "t.value")} AS tag_value,
                        {EventTypeKeySql(registration, "e")} AS tag_table
                    FROM {events.EventsTableName} e
                    INNER JOIN {events.TagTableName(registration)} t ON {events.TagToEventJoin("t", "e")}
                    WHERE e.stream_id = @id AND e.tenant_id = @tenant_id AND {FitsKeySql(registration, "e")}
                ) s
                INNER LOOP JOIN {events.DcbTagVersionTableName} v WITH (FORCESEEK)
                    ON v.tag_table = s.tag_table AND v.tag_value = s.tag_value AND v.tenant_id = @tenant_id
            """;
}
