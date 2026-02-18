using JasperFx.Events;
using Polecat.Internal;
using Polecat.Internal.Operations;

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

    private async Task<IEventStream<T>> FetchForWritingInternal<T>(object streamId, bool forExclusive,
        long? expectedVersion, CancellationToken cancellation) where T : class, new()
    {
        if (forExclusive)
        {
            await _sessionBase.BeginTransactionAsync(cancellation);
        }

        var conn = await _sessionBase.GetConnectionAsync(cancellation);

        // Query stream version
        long version = 0;
        bool streamExists = false;
        var lockHint = forExclusive ? " WITH (UPDLOCK, HOLDLOCK)" : "";

        await using (var cmd = conn.CreateCommand())
        {
            if (_sessionBase.ActiveTransaction != null)
            {
                cmd.Transaction = _sessionBase.ActiveTransaction;
            }

            cmd.CommandText = $"""
                SELECT version FROM {_events.StreamsTableName}{lockHint}
                WHERE id = @id AND tenant_id = @tenant_id;
                """;
            cmd.Parameters.AddWithValue("@id", streamId);
            cmd.Parameters.AddWithValue("@tenant_id", _tenantId);

            var result = await cmd.ExecuteScalarAsync(cancellation);
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

    private IEvent[] WrapEvents(object[] events)
    {
        var wrapped = new IEvent[events.Length];
        for (var i = 0; i < events.Length; i++)
        {
            wrapped[i] = _events.BuildEvent(events[i]);
        }

        return wrapped;
    }
}
