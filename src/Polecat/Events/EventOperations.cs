using System.Data.Common;
using System.Linq.Expressions;
using System.Text;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Events.Dcb;
using Polecat.Events.Fetching;
using Polecat.Events.Operations;
using Polecat.Internal;
using Polecat.Internal.Operations;
using Polecat.Serialization;
using Weasel.SqlServer;

namespace Polecat.Events;

/// <summary>
///     Per-session event operations. Wraps raw events and queues StreamActions
///     in the session's WorkTracker for execution on SaveChangesAsync.
/// </summary>
internal class EventOperations : QueryEventStore, IEventOperations
{
    private readonly DocumentSessionBase _sessionBase;
    private readonly WorkTracker _workTracker;
    private readonly string _tenantId;

    public EventOperations(DocumentSessionBase session, EventGraph events, StoreOptions options, WorkTracker workTracker, string tenantId)
        : base(session, events, options)
    {
        _sessionBase = session;
        _workTracker = workTracker;
        _tenantId = tenantId;
    }

    public StreamAction Append(Guid stream, params object[] events)
    {
        if (stream == Guid.Empty)
            throw new ArgumentOutOfRangeException(nameof(stream), "Stream id cannot be empty.");

        var wrapped = WrapEvents(events);
        foreach (var e in wrapped) e.StreamId = stream;

        if (_workTracker.TryFindStream(stream, out var existing))
        {
            existing!.AddEvents(wrapped);
            return existing;
        }

        var action = StreamAction.Append(stream, wrapped);
        action.TenantId = _tenantId;
        _workTracker.AddStream(action);
        return action;
    }

    public StreamAction Append(string stream, params object[] events)
    {
        if (string.IsNullOrEmpty(stream))
            throw new ArgumentOutOfRangeException(nameof(stream), "Stream key cannot be null or empty.");

        var wrapped = WrapEvents(events);
        foreach (var e in wrapped) e.StreamKey = stream;

        if (_workTracker.TryFindStream(stream, out var existing))
        {
            existing!.AddEvents(wrapped);
            return existing;
        }

        var action = StreamAction.Append(stream, wrapped);
        action.TenantId = _tenantId;
        _workTracker.AddStream(action);
        return action;
    }

    public StreamAction Append(Guid stream, long expectedVersion, params object[] events)
    {
        var action = Append(stream, events);
        action.ExpectedVersionOnServer = expectedVersion - action.Events.Count;
        if (action.ExpectedVersionOnServer < 0)
            throw new ArgumentOutOfRangeException(nameof(expectedVersion),
                "Expected version cannot be less than the number of events being appended.");
        return action;
    }

    public StreamAction Append(string stream, long expectedVersion, params object[] events)
    {
        var action = Append(stream, events);
        action.ExpectedVersionOnServer = expectedVersion - action.Events.Count;
        if (action.ExpectedVersionOnServer < 0)
            throw new ArgumentOutOfRangeException(nameof(expectedVersion),
                "Expected version cannot be less than the number of events being appended.");
        return action;
    }

    public StreamAction StartStream(Guid id, params object[] events)
    {
        if (id == Guid.Empty)
            throw new ArgumentOutOfRangeException(nameof(id), "Stream id cannot be empty.");

        var wrapped = WrapEvents(events);
        foreach (var e in wrapped) e.StreamId = id;

        var action = new StreamAction(id, StreamActionType.Start);
        action.AddEvents(wrapped);
        action.TenantId = _tenantId;
        _workTracker.AddStream(action);
        return action;
    }

    public StreamAction StartStream(string streamKey, params object[] events)
    {
        if (string.IsNullOrEmpty(streamKey))
            throw new ArgumentOutOfRangeException(nameof(streamKey), "Stream key cannot be null or empty.");

        var wrapped = WrapEvents(events);
        foreach (var e in wrapped) e.StreamKey = streamKey;

        var action = new StreamAction(streamKey, StreamActionType.Start);
        action.AddEvents(wrapped);
        action.TenantId = _tenantId;
        _workTracker.AddStream(action);
        return action;
    }

    public StreamAction StartStream<TAggregate>(Guid id, params object[] events) where TAggregate : class
    {
        var action = StartStream(id, events);
        action.AggregateType = typeof(TAggregate);
        return action;
    }

    public StreamAction StartStream<TAggregate>(string streamKey, params object[] events) where TAggregate : class
    {
        var action = StartStream(streamKey, events);
        action.AggregateType = typeof(TAggregate);
        return action;
    }

    public StreamAction StartStream(params object[] events)
    {
        return StartStream(Guid.NewGuid(), events);
    }

    public StreamAction StartStream<TAggregate>(params object[] events) where TAggregate : class
    {
        return StartStream<TAggregate>(Guid.NewGuid(), events);
    }

    public async Task<IEventStream<T>> FetchForWriting<T>(Guid id, CancellationToken cancellation = default)
        where T : class, new()
    {
        return await FetchForWritingInternal<T>(id, false, null, cancellation);
    }

    public async Task<IEventStream<T>> FetchForWriting<T>(string key, CancellationToken cancellation = default)
        where T : class, new()
    {
        return await FetchForWritingInternal<T>(key, false, null, cancellation);
    }

    public async Task<IEventStream<T>> FetchForWriting<T>(Guid id, long expectedVersion, CancellationToken cancellation = default)
        where T : class, new()
    {
        return await FetchForWritingInternal<T>(id, false, expectedVersion, cancellation);
    }

    public async Task<IEventStream<T>> FetchForWriting<T>(string key, long expectedVersion, CancellationToken cancellation = default)
        where T : class, new()
    {
        return await FetchForWritingInternal<T>(key, false, expectedVersion, cancellation);
    }

    public async Task<IEventStream<T>> FetchForExclusiveWriting<T>(Guid id, CancellationToken cancellation = default)
        where T : class, new()
    {
        return await FetchForWritingInternal<T>(id, true, null, cancellation);
    }

    public async Task<IEventStream<T>> FetchForExclusiveWriting<T>(string key, CancellationToken cancellation = default)
        where T : class, new()
    {
        return await FetchForWritingInternal<T>(key, true, null, cancellation);
    }

    public async Task WriteToAggregate<T>(Guid id, Action<IEventStream<T>> writing, CancellationToken cancellation = default)
        where T : class, new()
    {
        var stream = await FetchForWriting<T>(id, cancellation);
        writing(stream);
        await _sessionBase.SaveChangesAsync(cancellation);
    }

    public async Task WriteToAggregate<T>(string key, Action<IEventStream<T>> writing, CancellationToken cancellation = default)
        where T : class, new()
    {
        var stream = await FetchForWriting<T>(key, cancellation);
        writing(stream);
        await _sessionBase.SaveChangesAsync(cancellation);
    }

    public async Task WriteToAggregate<T>(Guid id, Func<IEventStream<T>, Task> writing, CancellationToken cancellation = default)
        where T : class, new()
    {
        var stream = await FetchForWriting<T>(id, cancellation);
        await writing(stream);
        await _sessionBase.SaveChangesAsync(cancellation);
    }

    public async Task WriteToAggregate<T>(string key, Func<IEventStream<T>, Task> writing, CancellationToken cancellation = default)
        where T : class, new()
    {
        var stream = await FetchForWriting<T>(key, cancellation);
        await writing(stream);
        await _sessionBase.SaveChangesAsync(cancellation);
    }

    public void ArchiveStream(Guid streamId)
    {
        _workTracker.Add(new ArchiveStreamOperation(_events, streamId, _tenantId));
    }

    public void ArchiveStream(string streamKey)
    {
        _workTracker.Add(new ArchiveStreamOperation(_events, streamKey, _tenantId));
    }

    public void UnArchiveStream(Guid streamId)
    {
        _workTracker.Add(new UnArchiveStreamOperation(_events, streamId, _tenantId));
    }

    public void UnArchiveStream(string streamKey)
    {
        _workTracker.Add(new UnArchiveStreamOperation(_events, streamKey, _tenantId));
    }

    public void TombstoneStream(Guid streamId)
    {
        _workTracker.Add(new TombstoneStreamOperation(_events, streamId, _tenantId));
    }

    public void TombstoneStream(string streamKey)
    {
        _workTracker.Add(new TombstoneStreamOperation(_events, streamKey, _tenantId));
    }

    private async Task<IEventStream<T>> FetchForWritingInternal<T>(object streamId, bool forExclusive,
        long? expectedVersion, CancellationToken cancellation) where T : class, new()
    {
        if (forExclusive)
        {
            await _sessionBase.BeginTransactionAsync(cancellation);
        }

        // Query stream version
        long version = 0;
        bool streamExists = false;
        var lockHint = forExclusive ? " WITH (UPDLOCK, HOLDLOCK)" : "";

        {
            await using var cmd = new SqlCommand();
            cmd.CommandText = $"""
                SELECT version FROM {_events.StreamsTableName}{lockHint}
                WHERE id = @id AND tenant_id = @tenant_id;
                """;
            cmd.Parameters.AddWithValue("@id", streamId);
            cmd.Parameters.AddWithValue("@tenant_id", _tenantId);

            var result = await _sessionBase.ExecuteScalarAsync(cmd, cancellation);
            if (result != null && result != DBNull.Value)
            {
                version = (long)result;
                streamExists = true;
            }
        }

        // Check expected version
        if (expectedVersion.HasValue && version != expectedVersion.Value)
        {
            throw new EventStreamUnexpectedMaxEventIdException(streamId, typeof(T),
                expectedVersion.Value, version);
        }

        // Build aggregate if stream exists
        T? aggregate = null;
        if (streamExists && version > 0)
        {
            if (streamId is Guid guidId)
            {
                var events = await FetchStreamAsync(guidId, token: cancellation);
                if (events.Count > 0)
                {
                    var aggregator = _sessionBase.Options.Projections.AggregatorFor<T>();
                    aggregate = await aggregator.BuildAsync(events, _sessionBase, null, cancellation);
                    if (aggregate != null) QueryEventStore.TrySetIdentity(aggregate, guidId);
                }
            }
            else
            {
                var key = (string)streamId;
                var events = await FetchStreamAsync(key, token: cancellation);
                if (events.Count > 0)
                {
                    var aggregator = _sessionBase.Options.Projections.AggregatorFor<T>();
                    aggregate = await aggregator.BuildAsync(events, _sessionBase, null, cancellation);
                    if (aggregate != null) QueryEventStore.TrySetIdentity(aggregate, key);
                }
            }
        }

        // Create the StreamAction
        StreamAction action;
        if (!streamExists)
        {
            if (streamId is Guid guidId)
            {
                action = new StreamAction(guidId, StreamActionType.Start);
            }
            else
            {
                action = new StreamAction((string)streamId, StreamActionType.Start);
            }

            action.ExpectedVersionOnServer = 0;
        }
        else
        {
            if (streamId is Guid guidId)
            {
                action = new StreamAction(guidId, StreamActionType.Append);
            }
            else
            {
                action = new StreamAction((string)streamId, StreamActionType.Append);
            }

            action.ExpectedVersionOnServer = version;
        }

        action.TenantId = _tenantId;
        action.AggregateType = typeof(T);
        _workTracker.AddStream(action);

        // Return the appropriate EventStream variant
        if (streamId is Guid gId)
        {
            return new EventStream<T>(_sessionBase, _events, gId, aggregate, cancellation, action);
        }
        else
        {
            return new EventStream<T>(_sessionBase, _events, (string)streamId, aggregate, cancellation, action);
        }
    }

    public async Task<IEventStream<T>> FetchForWriting<T, TId>(TId id, CancellationToken cancellation = default)
        where T : class, new() where TId : notnull
    {
        var naturalKey = FindNaturalKeyDefinition<T>();
        return await NaturalKeyFetchPlanner.FetchForWritingByNaturalKey<T, TId>(
            _sessionBase, _events, _workTracker, naturalKey, id, _tenantId, cancellation);
    }

    public async Task<IEventStream<T>> FetchForExclusiveWriting<T, TId>(TId id, CancellationToken cancellation = default)
        where T : class, new() where TId : notnull
    {
        // FetchForWritingByNaturalKey already uses UPDLOCK, HOLDLOCK for exclusive locking
        var naturalKey = FindNaturalKeyDefinition<T>();
        return await NaturalKeyFetchPlanner.FetchForWritingByNaturalKey<T, TId>(
            _sessionBase, _events, _workTracker, naturalKey, id, _tenantId, cancellation);
    }

    public async ValueTask<T?> FetchLatest<T, TId>(TId id, CancellationToken cancellation = default)
        where T : class, new() where TId : notnull
    {
        var naturalKey = FindNaturalKeyDefinition<T>();
        var unwrapped = naturalKey.Unwrap(id);
        if (unwrapped == null) return default;

        var isGuidStream = _events.StreamIdentity == StreamIdentity.AsGuid;
        var schema = _events.DatabaseSchemaName;
        var tableName = $"pc_natural_key_{naturalKey.AggregateType.Name.ToLowerInvariant()}";
        var streamColumn = isGuidStream ? "stream_id" : "stream_key";

        // Look up stream id from natural key table (read-only, no locking)
        await using var cmd = new SqlCommand();
        cmd.CommandText = $"""
            SELECT nk.{streamColumn}
            FROM [{schema}].[{tableName}] nk
            WHERE nk.natural_key_value = @naturalKey AND nk.is_archived = 0;
            """;
        cmd.Parameters.AddWithValue("@naturalKey", unwrapped);

        var result = await _sessionBase.ExecuteScalarAsync(cmd, cancellation);
        if (result == null || result == DBNull.Value) return default;

        // Delegate to existing FetchLatest with the resolved stream id
        if (isGuidStream)
        {
            return await FetchLatest<T>((Guid)result, cancellation);
        }
        else
        {
            return await FetchLatest<T>((string)result, cancellation);
        }
    }

    private NaturalKeyDefinition FindNaturalKeyDefinition<T>()
    {
        var definition = _sessionBase.Options.Projections.FindNaturalKeyDefinition(typeof(T));
        if (definition != null) return definition;

        throw new InvalidOperationException(
            $"No natural key definition found for aggregate type '{typeof(T).Name}'. " +
            "Configure a natural key via NaturalKey() in a SingleStreamProjection or Snapshot registration.");
    }

    public IEvent BuildEvent(object data) => _events.BuildEvent(data);

    public async Task<bool> EventsExistAsync(EventTagQuery query, CancellationToken cancellation = default)
    {
        var conditions = query.Conditions;
        if (conditions.Count == 0)
            throw new ArgumentException("EventTagQuery must have at least one condition.");

        var distinctTagTypes = conditions.Select(c => c.TagType).Distinct().ToList();
        var schema = _events.DatabaseSchemaName;

        var sb = new StringBuilder();
        sb.Append("SELECT CASE WHEN EXISTS (SELECT 1 FROM ");

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
                sb.Append($"[{schema}].[pc_event_tag_{registration.TableSuffix}] {alias}");
                first = false;
            }
            else
            {
                sb.Append($" INNER JOIN [{schema}].[pc_event_tag_{registration.TableSuffix}] {alias} ON t0.seq_id = {alias}.seq_id");
            }
        }

        // Join to pc_events only if we need event type filtering
        var hasEventTypeFilter = conditions.Any(c => c.EventType != null);
        if (hasEventTypeFilter)
        {
            sb.Append($" INNER JOIN [{schema}].[pc_events] e ON t0.seq_id = e.seq_id");
        }

        sb.Append(" WHERE (");
        await using var cmd = new SqlCommand();
        var paramIndex = 0;

        for (var i = 0; i < conditions.Count; i++)
        {
            if (i > 0) sb.Append(" OR ");

            var condition = conditions[i];
            var tagIndex = distinctTagTypes.IndexOf(condition.TagType);
            var registration = _events.FindTagType(condition.TagType)!;
            var value = registration.ExtractValue(condition.TagValue);

            sb.Append($"(t{tagIndex}.value = @p{paramIndex}");
            cmd.Parameters.AddWithValue($"@p{paramIndex}", value);
            paramIndex++;

            if (condition.EventType != null)
            {
                sb.Append($" AND e.type = @p{paramIndex}");
                var eventTypeName = _events.EventMappingFor(condition.EventType).EventTypeName;
                cmd.Parameters.AddWithValue($"@p{paramIndex}", eventTypeName);
                paramIndex++;
            }

            sb.Append(')');
        }

        sb.Append(")) THEN 1 ELSE 0 END");
        cmd.CommandText = sb.ToString();

        var result = await _sessionBase.ExecuteScalarAsync(cmd, cancellation);
        return result is int intVal && intVal == 1;
    }

    public async Task<IReadOnlyList<IEvent>> QueryByTagsAsync(EventTagQuery query,
        CancellationToken cancellation = default)
    {
        var conditions = query.Conditions;
        if (conditions.Count == 0)
            throw new ArgumentException("EventTagQuery must have at least one condition.");

        var distinctTagTypes = conditions.Select(c => c.TagType).Distinct().ToList();
        var schema = _events.DatabaseSchemaName;
        var eventOptions = _events.EventOptions;

        // Build SELECT columns matching the event reader format
        var selectColumns = "e.seq_id, e.id, e.stream_id, e.version, e.data, e.type, e.timestamp, e.tenant_id, e.dotnet_type, e.is_archived";
        if (eventOptions.EnableCorrelationId) selectColumns += ", e.correlation_id";
        if (eventOptions.EnableCausationId) selectColumns += ", e.causation_id";
        if (eventOptions.EnableHeaders) selectColumns += ", e.headers";

        var sb = new StringBuilder();
        sb.Append($"SELECT {selectColumns} FROM [{schema}].[pc_events] e");

        // INNER JOINs to tag tables
        for (var i = 0; i < distinctTagTypes.Count; i++)
        {
            var tagType = distinctTagTypes[i];
            var registration = _events.FindTagType(tagType)
                               ?? throw new InvalidOperationException(
                                   $"Tag type '{tagType.Name}' is not registered.");

            sb.Append($" INNER JOIN [{schema}].[pc_event_tag_{registration.TableSuffix}] t{i} ON e.seq_id = t{i}.seq_id");
        }

        // WHERE clause
        sb.Append(" WHERE (");
        var paramIndex = 0;
        await using var cmd = new SqlCommand();

        for (var i = 0; i < conditions.Count; i++)
        {
            if (i > 0) sb.Append(" OR ");

            var condition = conditions[i];
            var tagIndex = distinctTagTypes.IndexOf(condition.TagType);
            var registration = _events.FindTagType(condition.TagType)!;
            var value = registration.ExtractValue(condition.TagValue);

            sb.Append($"(t{tagIndex}.value = @p{paramIndex}");
            cmd.Parameters.AddWithValue($"@p{paramIndex}", value);
            paramIndex++;

            if (condition.EventType != null)
            {
                sb.Append($" AND e.type = @p{paramIndex}");
                var eventTypeName = _events.EventMappingFor(condition.EventType).EventTypeName;
                cmd.Parameters.AddWithValue($"@p{paramIndex}", eventTypeName);
                paramIndex++;
            }

            sb.Append(')');
        }

        sb.Append(") ORDER BY e.seq_id");
        cmd.CommandText = sb.ToString();

        var results = new List<IEvent>();
        await using var dbReader = await _sessionBase.ExecuteReaderAsync(cmd, cancellation);
        var reader = (SqlDataReader)dbReader;

        while (await reader.ReadAsync(cancellation))
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
            if (resolvedType == null) continue;

            var data = _sessionBase.Serializer.FromJson(resolvedType, json);
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
            if (eventOptions.EnableHeaders && !reader.IsDBNull(metaIndex))
            {
                var headersJson = reader.GetString(metaIndex);
                @event.Headers = _sessionBase.Serializer.FromJson<Dictionary<string, object>>(headersJson);
            }

            results.Add(@event);
        }

        return results;
    }

    public async Task<T?> AggregateByTagsAsync<T>(EventTagQuery query,
        CancellationToken cancellation = default) where T : class
    {
        var events = await QueryByTagsAsync(query, cancellation);
        if (events.Count == 0) return default;

        var aggregator = _sessionBase.Options.Projections.AggregatorFor<T>();
        return await aggregator.BuildAsync(events, _sessionBase, default, cancellation);
    }

    public async Task<IEventBoundary<T>> FetchForWritingByTags<T>(EventTagQuery query,
        CancellationToken cancellation = default) where T : class
    {
        var events = await QueryByTagsAsync(query, cancellation);
        var lastSeenSequence = events.Count > 0 ? events.Max(e => e.Sequence) : 0;

        T? aggregate = default;
        if (events.Count > 0)
        {
            var aggregator = _sessionBase.Options.Projections.AggregatorFor<T>();
            aggregate = await aggregator.BuildAsync(events, _sessionBase, default, cancellation);
        }

        // Register DCB assertion operation
        _workTracker.Add(new AssertDcbConsistencyOperation(_events, query, lastSeenSequence));

        return new EventBoundary<T>(_sessionBase, _events, aggregate, events, lastSeenSequence);
    }

    public void AssignTagWhere(Expression<Func<IEvent, bool>> expression, object tag)
    {
        if (expression == null) throw new ArgumentNullException(nameof(expression));
        if (tag == null) throw new ArgumentNullException(nameof(tag));

        var tagType = tag.GetType();
        var registration = _events.FindTagType(tagType)
                           ?? throw new InvalidOperationException(
                               $"Tag type '{tagType.Name}' is not registered. Call RegisterTagType<{tagType.Name}>() first.");

        var value = registration.ExtractValue(tag);
        var schema = _events.DatabaseSchemaName;

        var parser = new EventWhereClauseParser();
        var whereFragment = parser.Parse(expression.Body);

        var op = new AssignTagWhereOperation(schema, registration, value, whereFragment);
        _workTracker.Add(op);
    }

    private IEvent[] WrapEvents(object[] events)
    {
        var wrapped = new IEvent[events.Length];
        for (var i = 0; i < events.Length; i++)
        {
            wrapped[i] = _events.BuildEvent(events[i]);
        }

        return wrapped;
    }

    /// <summary>
    /// Build tag query SELECT columns and FROM/JOIN/WHERE SQL into a command builder.
    /// Shared between direct queries and batch queries.
    /// </summary>
    internal static void WriteTagQuerySql(ICommandBuilder builder, EventGraph eventGraph, EventTagQuery query)
    {
        var conditions = query.Conditions;
        var distinctTagTypes = conditions.Select(c => c.TagType).Distinct().ToList();
        var schema = eventGraph.DatabaseSchemaName;
        var eventOptions = eventGraph.EventOptions;

        var selectColumns = "e.seq_id, e.id, e.stream_id, e.version, e.data, e.type, e.timestamp, e.tenant_id, e.dotnet_type, e.is_archived";
        if (eventOptions.EnableCorrelationId) selectColumns += ", e.correlation_id";
        if (eventOptions.EnableCausationId) selectColumns += ", e.causation_id";
        if (eventOptions.EnableHeaders) selectColumns += ", e.headers";

        builder.Append($"SELECT {selectColumns} FROM [{schema}].[pc_events] e");

        for (var i = 0; i < distinctTagTypes.Count; i++)
        {
            var tagType = distinctTagTypes[i];
            var registration = eventGraph.FindTagType(tagType)
                               ?? throw new InvalidOperationException(
                                   $"Tag type '{tagType.Name}' is not registered.");

            builder.Append($" INNER JOIN [{schema}].[pc_event_tag_{registration.TableSuffix}] t{i} ON e.seq_id = t{i}.seq_id");
        }

        builder.Append(" WHERE (");
        for (var i = 0; i < conditions.Count; i++)
        {
            if (i > 0) builder.Append(" OR ");

            var condition = conditions[i];
            var tagIndex = distinctTagTypes.IndexOf(condition.TagType);
            var registration = eventGraph.FindTagType(condition.TagType)!;
            var value = registration.ExtractValue(condition.TagValue);

            builder.Append($"(t{tagIndex}.value = ");
            builder.AppendParameter(value);

            if (condition.EventType != null)
            {
                builder.Append(" AND e.type = ");
                var eventTypeName = eventGraph.EventMappingFor(condition.EventType).EventTypeName;
                builder.AppendParameter(eventTypeName);
            }

            builder.Append(")");
        }

        builder.Append(") ORDER BY e.seq_id");
    }

    /// <summary>
    /// Read a single event from the current reader row. Column layout must match WriteTagQuerySql.
    /// </summary>
    internal static IEvent? ReadEventFromReader(DbDataReader reader, ISerializer serializer, EventGraph eventGraph)
    {
        var eventOptions = eventGraph.EventOptions;
        var sqlReader = (SqlDataReader)reader;

        var seqId = reader.GetInt64(0);
        var eventId = reader.GetGuid(1);
        var eventVersion = reader.GetInt64(3);
        var json = reader.GetString(4);
        var typeName = reader.GetString(5);
        var eventTimestamp = sqlReader.GetDateTimeOffset(6);
        var tenantId = reader.GetString(7);
        var dotNetTypeName = reader.IsDBNull(8) ? null : reader.GetString(8);
        var isArchived = reader.GetBoolean(9);

        var resolvedType = eventGraph.ResolveEventType(dotNetTypeName);
        if (resolvedType == null) return null;

        var data = serializer.FromJson(resolvedType, json);
        var mapping = eventGraph.EventMappingFor(resolvedType);
        var @event = mapping.Wrap(data);

        @event.Id = eventId;
        @event.Sequence = seqId;
        @event.Version = eventVersion;
        @event.Timestamp = eventTimestamp;
        @event.TenantId = tenantId;
        @event.EventTypeName = typeName;
        @event.DotNetTypeName = dotNetTypeName!;
        @event.IsArchived = isArchived;

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
        if (eventOptions.EnableHeaders && !reader.IsDBNull(metaIndex))
        {
            var headersJson = reader.GetString(metaIndex);
            @event.Headers = serializer.FromJson<Dictionary<string, object>>(headersJson);
        }

        return @event;
    }
}
