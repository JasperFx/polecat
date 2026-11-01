using System.Data;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Events.Schema;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Events.Dcb;

/// <summary>
///     The body of a DCB tag query. <see cref="EventOperations.WriteTagQuerySql" /> and
///     <see cref="EventOperations.WriteEventsExistSql" /> are the two statements that carry it, and
///     between them they serve the direct and the batched flavour of every tag read.
/// </summary>
/// <remarks>
///     <para>
///         #841: one <c>SELECT</c> per (tag type, event type) pair, combined with <c>UNION</c>. Each one
///         reads a single tag table, so an event carrying only one of several queried tag types
///         matches, which an inner join across the tag tables cannot do. They select the event's key
///         alone, which is what brings an event tagged with two queried values back once:
///         <c>DISTINCT</c> over the event columns is not available, because the <c>json</c> type has no
///         equality. And it gives each pair a seek on the tag table's primary key, where OR-ing
///         predicates across outer-joined tag tables has to scan <c>pc_events</c>.
///     </para>
///     <para>
///         <b>Liveness is read from <c>pc_events</c></b>, never from the tag row. The tag tables'
///         <c>is_archived</c> column exists only under archived partitioning and nothing updates it, so
///         the existence check pays one clustered seek per matching tag row to ask the row that knows.
///         An archived event counts as gone here for the same reason it does on every other event
///         query, and the boundary check agrees: see <c>SetStreamArchivedOperation</c>.
///     </para>
///     <para>
///         #834: every condition bound its value, and its event type when it had one, so a boundary
///         naming a couple of thousand tags was refused outright. Over
///         <see cref="JsonValueList.Threshold" /> parameters the values travel as one JSON array per
///         (tag type, event type) pair, the same decision #710 made for a LINQ <c>IN</c> list, and it
///         means the same thing: a condition is an OR, so a run of them over one tag type and one
///         event type is an <c>IN</c>.
///     </para>
///     <para>
///         A string tag unpacks as <c>varchar(8000)</c> rather than the column's <c>varchar(250)</c>.
///         The two compare without a conversion on the column, so the seek is kept, and a query value
///         longer than the column cannot be truncated into a match on a shorter stored tag.
///     </para>
/// </remarks>
internal static class TagConditionSql
{
    private const int VarCharSize = 8000;

    /// <summary>
    ///     The key of every event the query names, with no filtering on the events themselves beyond
    ///     what a condition's event type asks for. Each event appears once.
    /// </summary>
    public static void AppendEventKeys(ICommandBuilder builder, EventGraph events, EventTagQuery query,
        string? tenantId)
    {
        // DISTINCT as well as UNION: one branch holds every value of its tag type, so an event tagged with
        // two of them is two rows there, and with a single branch there is no UNION to collapse them.
        var keys = KeysOf(events);
        AppendBranches(builder, events, query, tenantId, alias => $"DISTINCT {keys(alias)}", " UNION ",
            liveEventsOnly: false);
    }

    /// <summary>A row per match, stopping at the first when read by an <c>EXISTS</c>.</summary>
    public static void AppendLiveMatches(ICommandBuilder builder, EventGraph events, EventTagQuery query,
        string? tenantId)
    {
        AppendBranches(builder, events, query, tenantId, _ => "1", " UNION ALL ", liveEventsOnly: true);
    }

    /// <summary>
    ///     Exactly the columns <see cref="EventGraph.TagToEventJoin" /> reads, which are all a
    ///     <c>UNION</c> over tag rows may compare and all the join back to <c>pc_events</c> needs.
    /// </summary>
    private static Func<string, string> KeysOf(EventGraph events)
    {
        var conjoined = events.TenancyStyle == TenancyStyle.Conjoined;
        var ordinal = events.UseTenantPartitionedEvents ? events.TenantPartitionManager.Column : null;

        return alias =>
        {
            var keys = $"{alias}.seq_id";
            if (conjoined) keys += $", {alias}.tenant_id";
            if (ordinal != null) keys += $", {alias}.{ordinal}";
            return keys;
        };
    }

    private static void AppendBranches(ICommandBuilder builder, EventGraph events, EventTagQuery query,
        string? tenantId, Func<string, string> columns, string combinedWith, bool liveEventsOnly)
    {
        var conditions = query.Conditions;
        var distinctTagTypes = conditions.Select(c => c.TagType).Distinct().ToList();
        var branches = conditions.GroupBy(c => (c.TagType, c.EventType)).ToArray();

        var filtersTenant = events.TenancyStyle == TenancyStyle.Conjoined && tenantId != null;
        var parameters = conditions.Count
                         + branches.Count(x => x.Key.EventType != null)
                         + (filtersTenant ? branches.Length : 0);

        var asJsonArrays = JsonValueList.ShouldBindAsJsonArray(builder, parameters);

        for (var i = 0; i < branches.Length; i++)
        {
            if (i > 0) builder.Append(combinedWith);

            var (tagType, eventType) = branches[i].Key;
            var registration = events.FindTagType(tagType)
                               ?? throw new InvalidOperationException(
                                   $"Tag type '{tagType.Name}' is not registered.");

            var alias = $"t{distinctTagTypes.IndexOf(tagType)}";

            builder.Append($"SELECT {columns(alias)} FROM {events.TagTableName(registration)} {alias}");

            if (liveEventsOnly || eventType != null)
            {
                builder.Append($" INNER JOIN {events.EventsTableName} e ON {events.TagToEventJoin(alias, "e")}");
            }

            builder.Append(" WHERE ");

            var values = branches[i].Select(c => registration.ExtractValue(c.TagValue)).ToArray();
            if (asJsonArrays)
            {
                JsonValueList.AppendInClause(builder, $"{alias}.value", values, ColumnTypeFor(registration),
                    value => value);
            }
            else
            {
                appendValues(builder, alias, values);
            }

            if (filtersTenant)
            {
                builder.Append($" AND {alias}.tenant_id = ");
                AppendVarChar(builder, tenantId!);
            }

            if (eventType != null)
            {
                builder.Append(" AND e.type = ");
                AppendVarChar(builder, events.EventMappingFor(eventType).EventTypeName);
            }

            // A literal, so SQL Server can eliminate the archived partition at compile time where
            // is_archived participates in the partition scheme.
            if (liveEventsOnly) builder.Append(" AND e.is_archived = 0");
        }
    }

    private static void appendValues(ICommandBuilder builder, string alias, object[] values)
    {
        builder.Append("(");

        for (var i = 0; i < values.Length; i++)
        {
            if (i > 0) builder.Append(" OR ");

            builder.Append($"{alias}.value = ");
            if (values[i] is string text)
            {
                AppendVarChar(builder, text);
            }
            else
            {
                builder.AppendParameter(values[i], null);
            }
        }

        builder.Append(")");
    }

    /// <summary>
    ///     A string bound against one of the event store's varchar columns. The fixed size keeps one
    ///     plan per statement rather than one per distinct value length, as
    ///     <see cref="Polecat.Internal.SqlParameterCollectionExtensions.AddVarChar" /> does for a
    ///     <see cref="SqlCommand" />.
    /// </summary>
    public static void AppendVarChar(ICommandBuilder builder, string value)
        => builder.AppendParameter(value, SqlDbType.VarChar).Size = VarCharSize;

    private static string ColumnTypeFor(ITagTypeRegistration registration)
        => registration.SimpleType == typeof(string)
            ? $"varchar({VarCharSize})"
            : EventTagTable.SqlServerTypeFor(registration.SimpleType);
}
