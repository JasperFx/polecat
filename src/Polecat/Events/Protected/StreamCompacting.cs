using JasperFx.Events;
using JasperFx.Events.Aggregation;
using Polecat.Internal;

namespace Polecat.Events.Protected;

/// <summary>
///     Callback interface for executing event archiving before stream compaction.
/// </summary>
public interface IEventsArchiver
{
    Task MaybeArchiveAsync<T>(IDocumentOperations operations, StreamCompactingRequest<T> request,
        IReadOnlyList<IEvent> events, CancellationToken cancellation) where T : class;
}

/// <summary>
///     Configuration and execution of stream compaction. Replaces all events (except the last)
///     with a single Compacted&lt;T&gt; snapshot event, then deletes the originals.
/// </summary>
public class StreamCompactingRequest<T> where T : class
{
    public StreamCompactingRequest(string? streamKey)
    {
        StreamKey = streamKey;
    }

    public StreamCompactingRequest(Guid? streamId)
    {
        StreamId = streamId;
    }

    /// <summary>
    ///     The identity of the stream if using string identified streams.
    /// </summary>
    public string? StreamKey { get; private set; }

    /// <summary>
    ///     The identity of the stream if using Guid identified streams.
    /// </summary>
    public Guid? StreamId { get; private set; }

    /// <summary>
    ///     If specified, the version at which the stream is going to be compacted.
    ///     Default 0 means the latest.
    /// </summary>
    public long Version { get; set; }

    /// <summary>
    ///     If specified, this operation will compact the events below the timestamp.
    /// </summary>
    public DateTimeOffset? Timestamp { get; set; }

    /// <summary>
    ///     Optional mechanism to carry out an archiving step for the events before the
    ///     compacting operation is completed and these events are permanently deleted.
    /// </summary>
    public IEventsArchiver? Archiver { get; set; }

    /// <summary>
    ///     CancellationToken for just this operation. Default is None.
    /// </summary>
    public CancellationToken CancellationToken { get; set; } = CancellationToken.None;

    /// <summary>
    ///     The event sequence of the last event being compacted.
    /// </summary>
    public long Sequence { get; private set; }

    internal async Task ExecuteAsync(DocumentSessionBase session)
    {
        // 1. Find the aggregator
        var aggregator = FindAggregator(session);

        // 2. Fetch events
        IReadOnlyList<IEvent> events;
        if (session.Options.Events.StreamIdentity == StreamIdentity.AsGuid)
        {
            events = await session.Events.FetchStreamAsync(StreamId!.Value, Version, Timestamp,
                token: CancellationToken).ConfigureAwait(false);
        }
        else
        {
            events = await session.Events.FetchStreamAsync(StreamKey!, Version, Timestamp,
                token: CancellationToken).ConfigureAwait(false);
        }

        if (events.Count == 0) return;
        if (events is [{ Data: Compacted<T> }]) return;

        // Sequences of all events except the last (the last will be replaced with the compacted snapshot)
        var sequences = events.Select(x => x.Sequence).Take(events.Count - 1).ToArray();

        Version = events[events.Count - 1].Version;
        Sequence = events[events.Count - 1].Sequence;

        // 3. Aggregate to build the snapshot
        var aggregate = await aggregator.BuildAsync(events, session, default, CancellationToken)
            .ConfigureAwait(false);

        // 4. Optional archiving
        if (Archiver != null)
        {
            await Archiver.MaybeArchiveAsync(session, this, events, CancellationToken)
                .ConfigureAwait(false);
        }

        // 5. Replace the last event with the Compacted<T> snapshot
        var compacted = new Compacted<T>(aggregate!, 
            StreamId ?? Guid.Empty, StreamKey ?? string.Empty);

        var serializedData = session.Serializer.ToJson(compacted);
        var mapping = session.Options.EventGraph.EventMappingFor(typeof(Compacted<T>));

        var replaceOp = new ReplaceEventOperation(
            session.Options.EventGraph, Sequence, serializedData,
            mapping.EventTypeName, mapping.DotNetTypeName);

        session.WorkTracker.Add(replaceOp);

        // 6. Delete the old events
        if (sequences.Length > 0)
        {
            session.WorkTracker.Add(new DeleteEventsOperation(session.Options.EventGraph, sequences));
        }
    }

    private static IAggregator<T, IQuerySession> FindAggregator(DocumentSessionBase session)
    {
        return session.Options.Projections.AggregatorFor<T>();
    }
}
