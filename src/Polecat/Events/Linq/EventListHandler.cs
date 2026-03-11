using System.Data.Common;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Serialization;

namespace Polecat.Events.Linq;

/// <summary>
///     Reads IEvent objects from a multi-column result set on pc_events.
///     Column layout: seq_id, id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type, is_archived
/// </summary>
internal class EventListHandler
{
    private readonly ISerializer _serializer;
    private readonly EventGraph _events;

    public EventListHandler(ISerializer serializer, EventGraph events)
    {
        _serializer = serializer;
        _events = events;
    }

    public async Task<IReadOnlyList<IEvent>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var results = new List<IEvent>();
        var sqlReader = (SqlDataReader)reader;

        while (await sqlReader.ReadAsync(token))
        {
            var seqId = sqlReader.GetInt64(0);
            var eventId = sqlReader.GetGuid(1);
            var streamId = sqlReader.IsDBNull(2) ? (object?)null : sqlReader.GetValue(2);
            var eventVersion = sqlReader.GetInt64(3);
            var json = sqlReader.GetString(4);
            var typeName = sqlReader.GetString(5);
            var eventTimestamp = sqlReader.GetDateTimeOffset(6);
            var tenantId = sqlReader.GetString(7);
            var dotNetTypeName = sqlReader.IsDBNull(8) ? null : sqlReader.GetString(8);
            var isArchived = sqlReader.GetBoolean(9);

            var resolvedType = _events.ResolveEventType(dotNetTypeName);
            if (resolvedType == null) continue;

            var data = _serializer.FromJson(resolvedType, json);
            var mapping = _events.EventMappingFor(resolvedType);
            var @event = mapping.Wrap(data);

            @event.Id = eventId;
            @event.Sequence = seqId;
            @event.Version = eventVersion;
            @event.Timestamp = eventTimestamp;
            @event.TenantId = tenantId;
            @event.EventTypeName = typeName;
            @event.DotNetTypeName = dotNetTypeName!;
            @event.IsArchived = isArchived;

            if (_events.StreamIdentity == StreamIdentity.AsGuid && streamId is Guid g)
            {
                @event.StreamId = g;
            }
            else if (streamId != null)
            {
                @event.StreamKey = streamId.ToString();
            }

            results.Add(@event);
        }

        return results;
    }
}
