using JasperFx.Events;
using Polecat.Internal;

namespace Polecat.Events;

/// <summary>
///     A writable handle to an event stream with its current aggregate state.
///     Returned by FetchForWriting / FetchForExclusiveWriting.
/// </summary>
public interface IEventStream<out T> where T : class
{
    /// <summary>
    ///     The current aggregate state, or null if the stream does not exist yet.
    /// </summary>
    T? Aggregate { get; }

    /// <summary>
    ///     The version of the stream when it was fetched (used for optimistic concurrency).
    /// </summary>
    long? StartingVersion { get; }

    /// <summary>
    ///     StartingVersion + count of appended events, or null if StartingVersion is null.
    /// </summary>
    long? CurrentVersion { get; }

    /// <summary>
    ///     The Guid identity of the stream (Guid.Empty if using string keys).
    /// </summary>
    Guid Id { get; }

    /// <summary>
    ///     The string key of the stream (null if using Guid identity).
    /// </summary>
    string? Key { get; }

    /// <summary>
    ///     The events that have been appended to this stream handle (not yet saved).
    /// </summary>
    IReadOnlyList<IEvent> Events { get; }

    /// <summary>
    ///     Append a single event to the stream.
    /// </summary>
    void AppendOne(object @event);

    /// <summary>
    ///     Append multiple events to the stream.
    /// </summary>
    void AppendMany(params object[] events);

    /// <summary>
    ///     Append multiple events to the stream.
    /// </summary>
    void AppendMany(IEnumerable<object> events);

    /// <summary>
    ///     Try to advance the expected starting version for optimistic concurrency checks to the current version
    ///     so that you can reuse a stream object for multiple units of work.
    /// </summary>
    void TryFastForwardVersion();
}

internal class EventStream<T> : IEventStream<T> where T : class
{
    private StreamAction _stream;
    private readonly Func<object, IEvent> _wrapper;
    private readonly DocumentSessionBase _session;

    public EventStream(DocumentSessionBase session, EventGraph events, Guid streamId, T? aggregate,
        CancellationToken cancellation, StreamAction stream)
    {
        _session = session;
        _wrapper = o =>
        {
            var e = events.BuildEvent(o);
            e.StreamId = streamId;
            return e;
        };

        _stream = stream;
        _stream.AggregateType = typeof(T);

        Aggregate = aggregate;
    }

    public EventStream(DocumentSessionBase session, EventGraph events, string streamKey, T? aggregate,
        CancellationToken cancellation, StreamAction stream)
    {
        _session = session;
        _wrapper = o =>
        {
            var e = events.BuildEvent(o);
            e.StreamKey = streamKey;
            return e;
        };

        _stream = stream;
        _stream.AggregateType = typeof(T);

        Aggregate = aggregate;
    }

    public Guid Id => _stream.Id;
    public string? Key => _stream.Key;

    public T? Aggregate { get; }
    public long? StartingVersion => _stream.ExpectedVersionOnServer;

    public long? CurrentVersion => _stream.ExpectedVersionOnServer == null
        ? null
        : _stream.ExpectedVersionOnServer.Value + _stream.Events.Count;

    public IReadOnlyList<IEvent> Events => _stream.Events;

    public void AppendOne(object @event)
    {
        _stream.AddEvent(_wrapper(@event));
    }

    public void AppendMany(params object[] events)
    {
        _stream.AddEvents(events.Select(e => _wrapper(e)).ToArray());
    }

    public void AppendMany(IEnumerable<object> events)
    {
        _stream.AddEvents(events.Select(e => _wrapper(e)).ToArray());
    }

    public void TryFastForwardVersion()
    {
        if (_session.WorkTracker.Streams.Contains(_stream))
        {
            return;
        }

        _stream = _stream.FastForward();
        _session.WorkTracker.AddStream(_stream);
    }
}
