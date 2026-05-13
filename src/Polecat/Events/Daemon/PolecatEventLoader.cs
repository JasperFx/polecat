using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polly;

namespace Polecat.Events.Daemon;

/// <summary>
///     SQL Server implementation of IEventLoader.
///     Loads event batches by seq_id range from pc_events.
///     All SQL execution is wrapped with Polly resilience.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: hydrates IEvent batches via EventGraph.Wrap (routed through ISerializer.FromJson). Event types are preserved by EventGraph registration on the caller side per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson and Event<T>.MakeGenericType are annotated RDC. AOT consumers register concrete event types ahead of time.")]
internal class PolecatEventLoader : IEventLoader
{
    private readonly EventGraph _events;
    private readonly StoreOptions _options;
    private readonly string _connectionString;
    private readonly ResiliencePipeline _resilience;
    private readonly HashSet<string>? _allowedDotNetTypes;

    public PolecatEventLoader(EventGraph events, StoreOptions options, string connectionString,
        EventFilterable? filtering = null)
    {
        _events = events;
        _options = options;
        _connectionString = connectionString;
        _resilience = options.ResiliencePipeline;

        // Build an allow list of dotnet_type names from the included event types
        if (filtering?.IncludedEventTypes is { Count: > 0 } includedTypes)
        {
            _allowedDotNetTypes = new HashSet<string>(StringComparer.Ordinal);
            foreach (var type in includedTypes)
            {
                var mapping = events.EventMappingFor(type);
                _allowedDotNetTypes.Add(mapping.DotNetTypeName);
            }
        }
    }

    public async Task<EventPage> LoadAsync(EventRequest request, CancellationToken token)
    {
        return await _resilience.ExecuteAsync(async (state, ct) =>
        {
            var page = new EventPage(state.Floor);

            await using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync(ct);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                SELECT TOP(@batchSize) seq_id, id, stream_id, version, data, type, timestamp,
                    tenant_id, dotnet_type, is_archived
                FROM {_events.EventsTableName}
                WHERE seq_id > @floor AND seq_id <= @ceiling AND is_archived = 0
                ORDER BY seq_id;
                """;

            cmd.Parameters.AddWithValue("@batchSize", state.BatchSize);
            cmd.Parameters.AddWithValue("@floor", state.Floor);
            cmd.Parameters.AddWithValue("@ceiling", state.HighWater);

            var skippedEvents = 0;

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
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

                // Apply event type allow-list filter (skip events not in the subscription's filter)
                if (_allowedDotNetTypes != null && dotNetTypeName != null &&
                    !_allowedDotNetTypes.Contains(dotNetTypeName))
                {
                    continue;
                }

                var resolvedType = _events.ResolveEventType(dotNetTypeName);
                if (resolvedType == null)
                {
                    if (state.ErrorOptions.SkipUnknownEvents)
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
                    if (state.ErrorOptions.SkipSerializationErrors)
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

            page.CalculateCeiling(state.BatchSize, state.HighWater, skippedEvents);
            return page;
        }, request, token);
    }
}
