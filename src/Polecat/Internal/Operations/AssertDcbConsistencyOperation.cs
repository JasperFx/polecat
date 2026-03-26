using System.Data.Common;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events;
using Polecat.Events.Dcb;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

internal class AssertDcbConsistencyOperation : IStorageOperation
{
    private readonly EventGraph _events;
    private readonly EventTagQuery _query;
    private readonly long _lastSeenSequence;
    private readonly string? _tenantId;

    public AssertDcbConsistencyOperation(EventGraph events, EventTagQuery query, long lastSeenSequence,
        string? tenantId = null)
    {
        _events = events;
        _query = query;
        _lastSeenSequence = lastSeenSequence;
        _tenantId = tenantId;
    }

    public Type DocumentType => typeof(IEvent);

    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var conditions = _query.Conditions;
        var distinctTagTypes = conditions.Select(c => c.TagType).Distinct().ToList();
        var schema = _events.DatabaseSchemaName;

        // Build EXISTS query
        builder.Append("SELECT CASE WHEN EXISTS (SELECT 1 FROM ");

        var first = true;
        for (var i = 0; i < distinctTagTypes.Count; i++)
        {
            var tagType = distinctTagTypes[i];
            var registration = _events.FindTagType(tagType)
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

        // Join to pc_events only if we need event type filtering
        var hasEventTypeFilter = conditions.Any(c => c.EventType != null);
        if (hasEventTypeFilter)
        {
            builder.Append($" INNER JOIN [{schema}].[pc_events] e ON t0.seq_id = e.seq_id");
        }

        builder.Append(" WHERE t0.seq_id > ");
        builder.AppendParameter(_lastSeenSequence);

        // Build OR conditions
        builder.Append(" AND (");
        for (var i = 0; i < conditions.Count; i++)
        {
            if (i > 0) builder.Append(" OR ");

            var condition = conditions[i];
            var tagIndex = distinctTagTypes.IndexOf(condition.TagType);
            var alias = $"t{tagIndex}";

            builder.Append("(");
            builder.Append(alias);
            builder.Append(".value = ");

            var registration = _events.FindTagType(condition.TagType)!;
            var value = registration.ExtractValue(condition.TagValue);
            builder.AppendParameter(value);

            if (condition.EventType != null)
            {
                builder.Append(" AND e.type = ");
                var eventTypeName = _events.EventMappingFor(condition.EventType).EventTypeName;
                builder.AppendParameter(eventTypeName);
            }

            builder.Append(")");
        }

        builder.Append(")");

        // Filter by tenant_id for conjoined tenancy
        if (_events.TenancyStyle == TenancyStyle.Conjoined && _tenantId != null)
        {
            builder.Append(" AND t0.tenant_id = ");
            builder.AppendParameter(_tenantId);
        }

        builder.Append(") THEN 1 ELSE 0 END");
    }

    public async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        if (await reader.ReadAsync(token) && reader.GetInt32(0) == 1)
        {
            exceptions.Add(new DcbConcurrencyException(_query, _lastSeenSequence));
        }
    }
}
