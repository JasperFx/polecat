using Polecat.Internal;
using Weasel.SqlServer;

namespace Polecat.Events.Dcb;

/// <summary>
///     Moves the <c>pc_dcb_tag_version</c> row of every tag on one stream's events, for an operation that
///     changes what a boundary over those tags reads without appending anything: archiving, un-archiving
///     (#841) and tombstoning (#890). The boundary check only ever compares that counter, so left alone, a
///     boundary would save straight through the change. The save that reads the counter again sees the
///     current events, so the cost is a retry and never a wrong answer.
/// </summary>
/// <remarks>
///     <para>
///         <c>HOLDLOCK</c> because this is a set-based create-or-bump, where
///         <c>DcbTagVersionPhase</c> can insert one row and catch the duplicate. It takes the range
///         locks that phase avoids, which it can afford for the same reason it is a separate statement
///         rather than part of the phase: none of the operations using it are common, and appending is.
///     </para>
///     <para>
///         It finds the tags through the stream's events, so it has to run ahead of anything deleting
///         them. The command must bind <c>@id</c> and <c>@tenant_id</c>.
///     </para>
/// </remarks>
internal static class StreamTagVersionBump
{
    public static void WriteSql(ICommandBuilder builder, EventGraph events)
    {
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
                WHEN MATCHED THEN UPDATE SET version = target.version + 1
                WHEN NOT MATCHED THEN INSERT (tag_table, tag_value, tenant_id, version)
                    VALUES (source.tag_table, source.tag_value, source.tenant_id, 1);

                """);
        }
    }
}
