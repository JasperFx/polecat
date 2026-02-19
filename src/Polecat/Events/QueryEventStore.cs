using System.Collections.Concurrent;
using System.Reflection;
using JasperFx.Events;
using Polecat.Internal;
using Polecat.Storage;

namespace Polecat.Events;

/// <summary>
///     Read-only event store implementation. Fetches events and stream state from the database.
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
        var conn = await _session.GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();
        if (_session.ActiveTransaction != null) cmd.Transaction = _session.ActiveTransaction;

        var sql = $"""
            SELECT seq_id, id, stream_id, version, data, type, timestamp, tenant_id, dotnet_type, is_archived
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
        await using var reader = await cmd.ExecuteReaderAsync(token);

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
        var conn = await _session.GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();
        if (_session.ActiveTransaction != null) cmd.Transaction = _session.ActiveTransaction;

        cmd.CommandText = $"""
            SELECT id, type, version, timestamp, created, tenant_id, is_archived
            FROM {_events.StreamsTableName}
            WHERE id = @id AND tenant_id = @tenant_id;
            """;
        cmd.Parameters.AddWithValue("@id", streamId);
        cmd.Parameters.AddWithValue("@tenant_id", _session.TenantId);

        await using var reader = await cmd.ExecuteReaderAsync(token);
        if (await reader.ReadAsync(token))
        {
            var version = reader.GetInt64(2);
            var lastTimestamp = reader.GetDateTimeOffset(3);
            var created = reader.GetDateTimeOffset(4);
            var isArchived = reader.GetBoolean(6);

            if (_events.StreamIdentity == StreamIdentity.AsGuid)
            {
                return new StreamState(reader.GetGuid(0), version, null, lastTimestamp, created)
                {
                    IsArchived = isArchived
                };
            }
            else
            {
                return new StreamState(reader.GetString(0), version, null, lastTimestamp, created)
                {
                    IsArchived = isArchived
                };
            }
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
        var effectiveFromVersion = fromVersion;

        // Try loading a cached snapshot from pc_streams if no explicit state or fromVersion was provided
        if (state == null && fromVersion == 0 && timestamp == null)
        {
            var (snapshot, snapshotVersion) = await TryLoadSnapshotAsync<T>(streamId, token);
            if (snapshot != null && snapshotVersion > 0)
            {
                // If a max version was requested and it's less than snapshot version,
                // we can't use the snapshot — fall back to full event replay
                if (version > 0 && version < snapshotVersion)
                {
                    // Don't use snapshot, let full replay happen below
                }
                else if (version > 0 && version == snapshotVersion)
                {
                    // Snapshot exactly matches requested version
                    TrySetIdentity(snapshot, streamId);
                    return snapshot;
                }
                else
                {
                    // Snapshot is valid; replay only events after snapshot
                    state = snapshot;
                    effectiveFromVersion = snapshotVersion + 1;
                }
            }
        }

        IReadOnlyList<IEvent> events;
        if (streamId is Guid guid)
            events = await FetchStreamAsync(guid, version, timestamp, effectiveFromVersion, token);
        else
            events = await FetchStreamAsync((string)streamId, version, timestamp, effectiveFromVersion, token);

        if (events.Count == 0) return state;

        var aggregator = _options.Projections.AggregatorFor<T>();
        var aggregate = await aggregator.BuildAsync(events, _session, state, token);
        if (aggregate == null) return null;

        TrySetIdentity(aggregate, streamId);
        return aggregate;
    }

    private async Task<(T? snapshot, long version)> TryLoadSnapshotAsync<T>(object streamId,
        CancellationToken token) where T : class
    {
        var conn = await _session.GetConnectionAsync(token);
        await using var cmd = conn.CreateCommand();
        if (_session.ActiveTransaction != null) cmd.Transaction = _session.ActiveTransaction;

        cmd.CommandText = $"""
            SELECT snapshot, snapshot_version
            FROM {_events.StreamsTableName}
            WHERE id = @id AND tenant_id = @tenant_id AND snapshot IS NOT NULL;
            """;
        cmd.Parameters.AddWithValue("@id", streamId);
        cmd.Parameters.AddWithValue("@tenant_id", _session.TenantId);

        await using var reader = await cmd.ExecuteReaderAsync(token);
        if (await reader.ReadAsync(token))
        {
            var json = reader.GetString(0);
            var snapshotVersion = reader.GetInt32(1);
            var snapshot = _session.Serializer.FromJson<T>(json);
            return (snapshot, snapshotVersion);
        }

        return (null, 0);
    }

    public async ValueTask<T?> FetchLatest<T>(Guid id, CancellationToken cancellation = default)
        where T : class, new()
    {
        return await AggregateStreamAsync<T>(id, token: cancellation);
    }

    public async ValueTask<T?> FetchLatest<T>(string key, CancellationToken cancellation = default)
        where T : class, new()
    {
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
