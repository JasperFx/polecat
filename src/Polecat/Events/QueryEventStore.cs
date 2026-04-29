using System.Collections.Concurrent;
using System.Reflection;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Events.Linq;
using Polecat.Internal;
using Polecat.Linq;
using Polecat.Storage;

namespace Polecat.Events;

/// <summary>
///     Read-only event store implementation. Fetches events and stream state from the database.
///     All SQL execution routes through session's Polly-wrapped centralized methods.
/// </summary>
internal class QueryEventStore : IQueryEventStore
{
    private readonly QuerySession _session;
    private readonly StoreOptions _options;
    protected readonly EventGraph _events;

    // Cache types that have no Id property to avoid repeated reflection
    private static readonly ConcurrentDictionary<Type, bool> _hasIdCache = new();

    public QueryEventStore(QuerySession session, EventGraph events, StoreOptions options)
    {
        _session = session;
        _events = events;
        _options = options;
    }

    public IPolecatQueryable<T> QueryRawEventDataOnly<T>() where T : class
    {
        _events.AddEventType(typeof(T));
        var eventTypeName = _events.EventMappingFor(typeof(T)).EventTypeName;
        var provider = new EventLinqQueryProvider(_session, _events, eventTypeName, typeof(T), _options);
        return new PolecatLinqQueryable<T>(provider);
    }

    public IPolecatQueryable<IEvent> QueryAllRawEvents()
    {
        var provider = new EventLinqQueryProvider(_session, _events);
        return new PolecatLinqQueryable<IEvent>(provider);
    }

    public async Task<IReadOnlyList<IEvent>> FetchStreamAsync(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0, CancellationToken token = default)
    {
        return await FetchStreamInternalAsync(streamId, version, timestamp, fromVersion, token);
    }

    public async Task<IReadOnlyList<IEvent>> FetchStreamAsync(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, long fromVersion = 0, CancellationToken token = default)
    {
        return await FetchStreamInternalAsync(streamKey, version, timestamp, fromVersion, token);
    }

    private async Task<IReadOnlyList<IEvent>> FetchStreamInternalAsync(object streamId, long version,
        DateTimeOffset? timestamp, long fromVersion, CancellationToken token)
    {
        await using var cmd = new SqlCommand();

        var eventOptions = _events.EventOptions;

        // Build SELECT columns dynamically for optional metadata
        var selectColumns = "seq_id, id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type, is_archived";
        if (eventOptions.EnableCorrelationId) selectColumns += ", correlation_id";
        if (eventOptions.EnableCausationId) selectColumns += ", causation_id";
        if (eventOptions.EnableHeaders) selectColumns += ", headers";

        var sql = $"""
            SELECT {selectColumns}
            FROM {_events.EventsTableName}
            WHERE stream_id = @stream_id AND tenant_id = @tenant_id AND is_archived = 0
            """;

        cmd.Parameters.AddWithValue("@stream_id", streamId);
        cmd.Parameters.AddWithValue("@tenant_id", _session.TenantId);

        if (version > 0)
        {
            sql += " AND version <= @version";
            cmd.Parameters.AddWithValue("@version", version);
        }

        if (timestamp.HasValue)
        {
            sql += " AND timestamp <= @timestamp";
            cmd.Parameters.AddWithValue("@timestamp", timestamp.Value);
        }

        if (fromVersion > 0)
        {
            sql += " AND version >= @from_version";
            cmd.Parameters.AddWithValue("@from_version", fromVersion);
        }

        sql += " ORDER BY version;";
        cmd.CommandText = sql;

        var results = new List<IEvent>();
        await using var dbReader = await _session.ExecuteReaderAsync(cmd, token);
        var reader = (SqlDataReader)dbReader;

        while (await reader.ReadAsync(token))
        {
            var seqId = reader.GetInt64(0);
            var eventId = reader.GetGuid(1);
            // stream_id at index 2
            var eventVersion = reader.GetInt64(3);
            var json = reader.GetString(4);
            var typeName = reader.GetString(5);
            var eventTimestamp = reader.GetDateTimeOffset(6);
            var tenantId = reader.GetString(7);
            var dotNetTypeName = reader.IsDBNull(8) ? null : reader.GetString(8);
            var isArchived = reader.GetBoolean(9);

            var resolvedType = _events.ResolveEventType(dotNetTypeName);
            if (resolvedType == null) continue; // Skip events we can't resolve

            var data = _session.Serializer.FromJson(resolvedType, json);
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

            // Read optional metadata columns (indices 10+ based on what's enabled)
            var metaIndex = 10;
            if (eventOptions.EnableCorrelationId)
            {
                @event.CorrelationId = reader.IsDBNull(metaIndex) ? null : reader.GetString(metaIndex);
                metaIndex++;
            }

            if (eventOptions.EnableCausationId)
            {
                @event.CausationId = reader.IsDBNull(metaIndex) ? null : reader.GetString(metaIndex);
                metaIndex++;
            }

            if (eventOptions.EnableHeaders)
            {
                if (!reader.IsDBNull(metaIndex))
                {
                    var headersJson = reader.GetString(metaIndex);
                    @event.Headers = _session.Serializer.FromJson<Dictionary<string, object>>(headersJson);
                }
            }

            if (_events.StreamIdentity == StreamIdentity.AsGuid)
            {
                @event.StreamId = streamId is Guid g ? g : Guid.Empty;
            }
            else
            {
                @event.StreamKey = streamId.ToString();
            }

            results.Add(@event);
        }

        return results;
    }

    public async Task<StreamState?> FetchStreamStateAsync(Guid streamId, CancellationToken token = default)
    {
        return await FetchStreamStateInternalAsync(streamId, token);
    }

    public async Task<StreamState?> FetchStreamStateAsync(string streamKey, CancellationToken token = default)
    {
        return await FetchStreamStateInternalAsync(streamKey, token);
    }

    private async Task<StreamState?> FetchStreamStateInternalAsync(object streamId, CancellationToken token)
    {
        await using var cmd = new SqlCommand();
        cmd.CommandText = $"""
            SELECT id, type, version, timestamp, created, tenant_id, is_archived
            FROM {_events.StreamsTableName}
            WHERE id = @id AND tenant_id = @tenant_id;
            """;
        cmd.Parameters.AddWithValue("@id", streamId);
        cmd.Parameters.AddWithValue("@tenant_id", _session.TenantId);

        await using var dbReader = await _session.ExecuteReaderAsync(cmd, token);
        var reader = (SqlDataReader)dbReader;
        if (await reader.ReadAsync(token))
        {
            var version = reader.GetInt64(2);
            var lastTimestamp = reader.GetDateTimeOffset(3);
            var created = reader.GetDateTimeOffset(4);
            var isArchived = reader.GetBoolean(6);

            var state = new StreamState
            {
                Version = version,
                LastTimestamp = lastTimestamp,
                Created = created,
                IsArchived = isArchived
            };

            if (_events.StreamIdentity == StreamIdentity.AsGuid)
            {
                state.Id = reader.GetGuid(0);
            }
            else
            {
                state.Key = reader.GetString(0);
            }

            return state;
        }

        return null;
    }

    public async Task<T?> AggregateStreamAsync<T>(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, T? state = null, long fromVersion = 0,
        CancellationToken token = default) where T : class, new()
    {
        return await AggregateStreamInternalAsync<T>(streamId, version, timestamp, state, fromVersion, token);
    }

    public async Task<T?> AggregateStreamAsync<T>(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, T? state = null, long fromVersion = 0,
        CancellationToken token = default) where T : class, new()
    {
        return await AggregateStreamInternalAsync<T>(streamKey, version, timestamp, state, fromVersion, token);
    }

    private async Task<T?> AggregateStreamInternalAsync<T>(object streamId, long version,
        DateTimeOffset? timestamp, T? state, long fromVersion,
        CancellationToken token) where T : class, new()
    {
        IReadOnlyList<IEvent> events;
        if (streamId is Guid guid)
            events = await FetchStreamAsync(guid, version, timestamp, fromVersion, token);
        else
            events = await FetchStreamAsync((string)streamId, version, timestamp, fromVersion, token);

        if (events.Count == 0) return state;

        var aggregator = _options.Projections.AggregatorFor<T>();
        var aggregate = await aggregator.BuildAsync(events, _session, state, token);
        if (aggregate == null) return null;

        TrySetIdentity(aggregate, streamId);
        return aggregate;
    }

    public async Task<T?> AggregateStreamToLastKnownAsync<T>(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, CancellationToken token = default) where T : class, new()
    {
        return await AggregateStreamToLastKnownInternalAsync<T>(streamId, version, timestamp, token);
    }

    public async Task<T?> AggregateStreamToLastKnownAsync<T>(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, CancellationToken token = default) where T : class, new()
    {
        return await AggregateStreamToLastKnownInternalAsync<T>(streamKey, version, timestamp, token);
    }

    private async Task<T?> AggregateStreamToLastKnownInternalAsync<T>(object streamId, long version,
        DateTimeOffset? timestamp, CancellationToken token) where T : class, new()
    {
        IReadOnlyList<IEvent> events;
        if (streamId is Guid guid)
            events = await FetchStreamAsync(guid, version, timestamp, 0, token);
        else
            events = await FetchStreamAsync((string)streamId, version, timestamp, 0, token);

        if (events.Count == 0) return null;

        var aggregator = _options.Projections.AggregatorFor<T>();
        var eventList = events.ToList();

        T? aggregate = null;
        while (aggregate == null && eventList.Count > 0)
        {
            aggregate = await aggregator.BuildAsync(eventList, _session, default, token);
            eventList = eventList.SkipLast(1).ToList();
        }

        if (aggregate != null)
        {
            TrySetIdentity(aggregate, streamId);
        }

        return aggregate;
    }

    public async ValueTask<T?> FetchLatest<T>(Guid id, CancellationToken cancellation = default)
        where T : class, new()
    {
        if (_session.TryGetAggregateFromIdentityMap<T, Guid>(id, out var cached))
        {
            return cached;
        }

        return await AggregateStreamAsync<T>(id, token: cancellation);
    }

    public async ValueTask<T?> FetchLatest<T>(string key, CancellationToken cancellation = default)
        where T : class, new()
    {
        if (_session.TryGetAggregateFromIdentityMap<T, string>(key, out var cached))
        {
            return cached;
        }

        return await AggregateStreamAsync<T>(key, token: cancellation);
    }

    internal static void TrySetIdentity<T>(T aggregate, object streamId) where T : class
    {
        var hasId = _hasIdCache.GetOrAdd(typeof(T), static t =>
            DocumentMapping.FindIdProperty(t) != null);

        if (!hasId) return;

        var idProp = DocumentMapping.FindIdProperty(typeof(T))!;
        if (idProp.PropertyType.IsInstanceOfType(streamId))
        {
            idProp.SetValue(aggregate, streamId);
        }
    }
}
