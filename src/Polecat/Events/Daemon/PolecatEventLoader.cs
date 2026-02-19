using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;

namespace Polecat.Events.Daemon;

/// <summary>
///     SQL Server implementation of IEventLoader.
///     Loads event batches by seq_id range from pc_events.
/// </summary>
internal class PolecatEventLoader : IEventLoader
{
    private readonly EventGraph _events;
    private readonly StoreOptions _options;
    private readonly string _connectionString;

    public PolecatEventLoader(EventGraph events, StoreOptions options, string connectionString)
    {
        _events = events;
        _options = options;
        _connectionString = connectionString;
    }

    public async Task<EventPage> LoadAsync(EventRequest request, CancellationToken token)
    {
        var page = new EventPage(request.Floor);

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(token);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT TOP(@batchSize) seq_id, id, stream_id, version, data, type, timestamp,
                tenant_id, dotnet_type, is_archived
            FROM {_events.EventsTableName}
            WHERE seq_id > @floor AND seq_id <= @ceiling AND is_archived = 0
            ORDER BY seq_id;
            """;

        cmd.Parameters.AddWithValue("@batchSize", request.BatchSize);
        cmd.Parameters.AddWithValue("@floor", request.Floor);
        cmd.Parameters.AddWithValue("@ceiling", request.HighWater);

        var skippedEvents = 0;

        await using var reader = await cmd.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            var seqId = reader.GetInt64(0);
            var eventId = reader.GetGuid(1);
            var rawStreamId = reader.GetValue(2);
            var eventVersion = reader.GetInt64(3);
            var json = reader.GetString(4);
            var typeName = reader.GetString(5);
            var eventTimestamp = reader.GetDateTimeOffset(6);
            var tenantId = reader.GetString(7);
            var dotNetTypeName = reader.IsDBNull(8) ? null : reader.GetString(8);
            var isArchived = reader.GetBoolean(9);

            var resolvedType = _events.ResolveEventType(dotNetTypeName);
            if (resolvedType == null)
            {
                if (request.ErrorOptions.SkipUnknownEvents)
                {
                    skippedEvents++;
                    continue;
                }

                throw new InvalidOperationException(
                    $"Unable to resolve event type for dotnet_type '{dotNetTypeName}' at seq_id {seqId}.");
            }

            object data;
            try
            {
                data = _options.Serializer.FromJson(resolvedType, json);
            }
            catch (Exception ex)
            {
                if (request.ErrorOptions.SkipSerializationErrors)
                {
                    skippedEvents++;
                    continue;
                }

                throw new InvalidOperationException(
                    $"Failed to deserialize event at seq_id {seqId} of type '{dotNetTypeName}'.", ex);
            }

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

            if (_events.StreamIdentity == StreamIdentity.AsGuid && rawStreamId is Guid g)
            {
                @event.StreamId = g;
            }
            else
            {
                @event.StreamKey = rawStreamId.ToString();
            }

            page.Add(@event);
        }

        page.CalculateCeiling(request.BatchSize, request.HighWater, skippedEvents);
        return page;
    }
}
