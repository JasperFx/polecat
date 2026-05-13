using JasperFx.Events;
using Polecat.Internal;

namespace Polecat.Events;

// NOTE: The public IEventStream<T> contract moved to JasperFx.Events.IEventStream<T>
// as part of the Polecat 4 dedupe pillar consumption (see migration guide).
// Code that previously imported Polecat.Events.IEventStream<T> now resolves the
// unqualified `IEventStream<T>` to the JasperFx.Events version via
// `using JasperFx.Events;`.

internal class EventStream<T> : JasperFx.Events.IEventStream<T> where T : class
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

        Cancellation = cancellation;
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

        Cancellation = cancellation;
        Aggregate = aggregate;
    }

    public Guid Id => _stream.Id;
    public string? Key => _stream.Key;

    public T? Aggregate { get; }
    public long? StartingVersion => _stream.ExpectedVersionOnServer;

    public long? CurrentVersion => _stream.ExpectedVersionOnServer == null
        ? null
        : _stream.ExpectedVersionOnServer.Value + _stream.Events.Count;

    public CancellationToken Cancellation { get; }

    public bool AlwaysEnforceConsistency
    {
        get => _stream.AlwaysEnforceConsistency;
        set => _stream.AlwaysEnforceConsistency = value;
    }

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
