using System.Data;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Events.Schema;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Events.Dcb;

/// <summary>
///     The OR'd predicate of a DCB tag query. <see cref="EventOperations.WriteTagQuerySql" /> and
///     <see cref="EventOperations.WriteEventsExistSql" /> are the two statements that carry it, and
///     between them they serve the direct and the batched flavour of every tag read.
/// </summary>
/// <remarks>
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

    public static void Append(ICommandBuilder builder, EventGraph events,
        IReadOnlyList<EventTagQueryCondition> conditions, List<Type> distinctTagTypes)
    {
        var parameters = conditions.Sum(c => c.EventType is null ? 1 : 2);

        if (JsonValueList.ShouldBindAsJsonArray(builder, parameters))
        {
            appendAsJsonArrays(builder, events, conditions, distinctTagTypes);
            return;
        }

        for (var i = 0; i < conditions.Count; i++)
        {
            if (i > 0) builder.Append(" OR ");

            var condition = conditions[i];
            var tagIndex = distinctTagTypes.IndexOf(condition.TagType);
            var registration = events.FindTagType(condition.TagType)!;
            var value = registration.ExtractValue(condition.TagValue);

            builder.Append($"(t{tagIndex}.value = ");
            if (value is string text)
            {
                AppendVarChar(builder, text);
            }
            else
            {
                builder.AppendParameter(value, null);
            }

            appendEventTypeFilter(builder, events, condition.EventType);
            builder.Append(")");
        }
    }

    /// <summary>
    ///     A string bound against one of the event store's varchar columns. The fixed size keeps one
    ///     plan per statement rather than one per distinct value length, as
    ///     <see cref="Polecat.Internal.SqlParameterCollectionExtensions.AddVarChar" /> does for a
    ///     <see cref="SqlCommand" />.
    /// </summary>
    public static void AppendVarChar(ICommandBuilder builder, string value)
        => builder.AppendParameter(value, SqlDbType.VarChar).Size = VarCharSize;

    private static void appendAsJsonArrays(ICommandBuilder builder, EventGraph events,
        IReadOnlyList<EventTagQueryCondition> conditions, List<Type> distinctTagTypes)
    {
        var first = true;
        foreach (var group in conditions.GroupBy(c => (c.TagType, c.EventType)))
        {
            if (!first) builder.Append(" OR ");
            first = false;

            var (tagType, eventType) = group.Key;
            var registration = events.FindTagType(tagType)!;
            var values = group.Select(c => registration.ExtractValue(c.TagValue)).ToArray();

            builder.Append("(");
            JsonValueList.AppendInClause(builder, $"t{distinctTagTypes.IndexOf(tagType)}.value", values,
                ColumnTypeFor(registration), value => value);
            appendEventTypeFilter(builder, events, eventType);
            builder.Append(")");
        }
    }

    private static void appendEventTypeFilter(ICommandBuilder builder, EventGraph events, Type? eventType)
    {
        if (eventType is null) return;

        builder.Append(" AND e.type = ");
        AppendVarChar(builder, events.EventMappingFor(eventType).EventTypeName);
    }

    private static string ColumnTypeFor(ITagTypeRegistration registration)
        => registration.SimpleType == typeof(string)
            ? $"varchar({VarCharSize})"
            : EventTagTable.SqlServerTypeFor(registration.SimpleType);
}
