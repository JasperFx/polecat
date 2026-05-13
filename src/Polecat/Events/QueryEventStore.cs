using System.Collections.Concurrent;
using System.Reflection;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Events.Internal;
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
        // #57 pc_events half: column projection + per-row hydration live in
        // PcEventsRowReader, shared with the IEventStore explorer's
        // ReadStreamAsync path. This method only composes WHERE / ORDER BY.
        await using var cmd = new SqlCommand();

        var sql = $"""
            SELECT {PcEventsRowReader.ComposeSelectColumns(_events.EventOptions)}
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

        var ctx = new EventHydrationContext(
            _events,
            _session.Serializer,
            streamId,
            defaultTenantId: _session.TenantId);

        // Per-batch hoists: compute the optional-metadata column ordinals
        // once, declare a single-slot type→mapping cache, pick the
        // StreamIdentity specialization once. Per-row reads have zero
        // option-flag branches and ~1 EventMappingFor lookup per distinct
        // event-type-in-stream.
        var slots = MetadataSlots.Compute(_events.EventOptions);
        var cache = new EventTypeCache();

        var results = new List<IEvent>();
        await using var reader = await _session.ExecuteReaderAsync(cmd, token);

        if (_events.StreamIdentity == StreamIdentity.AsGuid)
        {
            while (await reader.ReadAsync(token))
            {
                var @event = PcEventsRowReader.ReadEventAsGuid(reader, ctx, slots, ref cache);
                if (@event != null) results.Add(@event);
            }
        }
        else
        {
            while (await reader.ReadAsync(token))
            {
                var @event = PcEventsRowReader.ReadEventAsString(reader, ctx, slots, ref cache);
                if (@event != null) results.Add(@event);
            }
        }

        return results;
    }

    public async Task<IEvent<T>?> LoadAsync<T>(Guid id, CancellationToken token = default) where T : class
    {
        var @event = await LoadInternalAsync(id, token);
        return @event as IEvent<T>;
    }

    public Task<IEvent?> LoadAsync(Guid id, CancellationToken token = default)
        => LoadInternalAsync(id, token);

    private async Task<IEvent?> LoadInternalAsync(Guid id, CancellationToken token)
    {
        // Mirrors FetchStreamInternalAsync but filters by the event UUID
        // rather than stream id, and reads the row's stream_id column to
        // assemble the context (since the caller doesn't know the stream
        // up-front for a load-by-event-id lookup).
        await using var cmd = new SqlCommand();
        cmd.CommandText = $"""
            SELECT {PcEventsRowReader.ComposeSelectColumns(_events.EventOptions)}
            FROM {_events.EventsTableName}
            WHERE id = @id AND tenant_id = @tenant_id AND is_archived = 0;
            """;
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant_id", _session.TenantId);

        await using var reader = await _session.ExecuteReaderAsync(cmd, token);
        if (!await reader.ReadAsync(token)) return null;

        // Pull stream_id off the row so PcEventsRowReader's stream-id
        // assignment (driven by ctx.StreamId) gets the right value. Ordinal 2
        // matches PcEventsRowReader.ComposeSelectColumns(...).
        object streamId = _events.StreamIdentity == StreamIdentity.AsGuid
            ? reader.GetGuid(2)
            : reader.GetString(2);

        var ctx = new EventHydrationContext(
            _events,
            _session.Serializer,
            streamId,
            defaultTenantId: _session.TenantId);

        var slots = MetadataSlots.Compute(_events.EventOptions);
        var cache = new EventTypeCache();

        return _events.StreamIdentity == StreamIdentity.AsGuid
            ? PcEventsRowReader.ReadEventAsGuid(reader, ctx, slots, ref cache)
            : PcEventsRowReader.ReadEventAsString(reader, ctx, slots, ref cache);
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
        // #57: column projection + row read live in PcStreamsRowReader so this
        // method, GetRecentStreamsAsync, and GetStreamMetadataAsync all read
        // pc_streams with the same shape. Note the canonical column order
        // (created before timestamp) differs from the historical order this
        // method used (timestamp before created) — the typed reader normalizes.
        await using var cmd = new SqlCommand();
        cmd.CommandText = $"""
            SELECT {PcStreamsRowReader.SelectColumns}
            FROM {_events.StreamsTableName}
            WHERE id = @id AND tenant_id = @tenant_id;
            """;
        cmd.Parameters.AddWithValue("@id", streamId);
        cmd.Parameters.AddWithValue("@tenant_id", _session.TenantId);

        await using var reader = await _session.ExecuteReaderAsync(cmd, token);
        if (await reader.ReadAsync(token))
        {
            return PcStreamsRowReader.ReadStreamState(reader, _events.StreamIdentity);
        }

        return null;
    }

    public async Task<T?> AggregateStreamAsync<T>(Guid streamId, long version = 0,
        DateTimeOffset? timestamp = null, T? state = null, long fromVersion = 0,
        CancellationToken token = default) where T : class
    {
        return await AggregateStreamInternalAsync<T>(streamId, version, timestamp, state, fromVersion, token);
    }

    public async Task<T?> AggregateStreamAsync<T>(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, T? state = null, long fromVersion = 0,
        CancellationToken token = default) where T : class
    {
        return await AggregateStreamInternalAsync<T>(streamKey, version, timestamp, state, fromVersion, token);
    }

    private async Task<T?> AggregateStreamInternalAsync<T>(object streamId, long version,
        DateTimeOffset? timestamp, T? state, long fromVersion,
        CancellationToken token) where T : class
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
        DateTimeOffset? timestamp = null, CancellationToken token = default) where T : class
    {
        return await AggregateStreamToLastKnownInternalAsync<T>(streamId, version, timestamp, token);
    }

    public async Task<T?> AggregateStreamToLastKnownAsync<T>(string streamKey, long version = 0,
        DateTimeOffset? timestamp = null, CancellationToken token = default) where T : class
    {
        return await AggregateStreamToLastKnownInternalAsync<T>(streamKey, version, timestamp, token);
    }

    private async Task<T?> AggregateStreamToLastKnownInternalAsync<T>(object streamId, long version,
        DateTimeOffset? timestamp, CancellationToken token) where T : class
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
        where T : class
    {
        if (_session.TryGetAggregateFromIdentityMap<T, Guid>(id, out var cached))
        {
            return cached;
        }

        return await AggregateStreamAsync<T>(id, token: cancellation);
    }

    public async ValueTask<T?> FetchLatest<T>(string key, CancellationToken cancellation = default)
        where T : class
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
