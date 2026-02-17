using JasperFx.Events;
using Polecat.Internal;

namespace Polecat.Events;

/// <summary>
///     Per-session event operations. Wraps raw events and queues StreamActions
///     in the session's WorkTracker for execution on SaveChangesAsync.
/// </summary>
internal class EventOperations : QueryEventStore, IEventOperations
{
    private readonly WorkTracker _workTracker;
    private readonly string _tenantId;

    public EventOperations(QuerySession session, EventGraph events, StoreOptions options, WorkTracker workTracker, string tenantId)
        : base(session, events, options)
    {
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
