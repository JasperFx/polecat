using System.Data.Common;
using JasperFx.Events.Tags;
using Polecat.Events;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

internal class EventsExistBatchItem : IBatchQueryItem
{
    private readonly EventGraph _eventGraph;
    private readonly EventTagQuery _query;
    private readonly TaskCompletionSource<bool> _tcs = new();

    public EventsExistBatchItem(EventGraph eventGraph, EventTagQuery query)
    {
        _eventGraph = eventGraph;
        _query = query;
    }

    public Task<bool> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder)
    {
        var conditions = _query.Conditions;
        if (conditions.Count == 0)
            throw new ArgumentException("EventTagQuery must have at least one condition.");

        var distinctTagTypes = conditions.Select(c => c.TagType).Distinct().ToList();
        var schema = _eventGraph.DatabaseSchemaName;

        builder.Append("SELECT CASE WHEN EXISTS (SELECT 1 FROM ");

        var first = true;
        for (var i = 0; i < distinctTagTypes.Count; i++)
        {
            var tagType = distinctTagTypes[i];
            var registration = _eventGraph.FindTagType(tagType)
                               ?? throw new InvalidOperationException(
                                   $"Tag type '{tagType.Name}' is not registered.");

            var alias = $"t{i}";
            if (first)
            {
                builder.Append($"[{schema}].[pc_event_tag_{registration.TableSuffix}] {alias}");
                first = false;
            }
            else
            {
                builder.Append($" INNER JOIN [{schema}].[pc_event_tag_{registration.TableSuffix}] {alias} ON t0.seq_id = {alias}.seq_id");
            }
        }

        var hasEventTypeFilter = conditions.Any(c => c.EventType != null);
        if (hasEventTypeFilter)
        {
            builder.Append($" INNER JOIN [{schema}].[pc_events] e ON t0.seq_id = e.seq_id");
        }

        builder.Append(" WHERE (");
        for (var i = 0; i < conditions.Count; i++)
        {
            if (i > 0) builder.Append(" OR ");

            var condition = conditions[i];
            var tagIndex = distinctTagTypes.IndexOf(condition.TagType);
            var registration = _eventGraph.FindTagType(condition.TagType)!;
            var value = registration.ExtractValue(condition.TagValue);

            builder.Append($"(t{tagIndex}.value = ");
            builder.AppendParameter(value);

            if (condition.EventType != null)
            {
                builder.Append(" AND e.type = ");
                var eventTypeName = _eventGraph.EventMappingFor(condition.EventType).EventTypeName;
                builder.AppendParameter(eventTypeName);
            }

            builder.Append(")");
        }

        builder.Append(")) THEN 1 ELSE 0 END");
    }

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        var exists = false;
        if (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            exists = reader.GetInt32(0) == 1;
        }

        _tcs.SetResult(exists);
    }
}
