using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Protected;
using Polecat.Internal;

namespace Polecat.Events.Protected;

/// <summary>
///     Polecat-side execution of <see cref="StreamCompactingRequest{T}"/> (the
///     data shape now lives in <see cref="JasperFx.Events.Protected"/> per the
///     dedupe pillar — see jasperfx#214 and the consuming-product note on
///     <see cref="StreamCompactingRequest{T}"/>). Each product owns its own
///     <c>ExecuteAsync</c>; only the request shape and the
///     <see cref="IEventsArchiver{TOperations}"/> hook are shared.
/// </summary>
internal static class StreamCompactingExecution
{
    /// <summary>
    ///     Drive the compaction against a Polecat <see cref="DocumentSessionBase"/>:
    ///     fetch the events to be replaced, build a <see cref="Compacted{T}"/>
    ///     snapshot via the aggregator, optionally invoke the archiver (if the
    ///     request's <see cref="StreamCompactingRequest{T}.Archiver"/> closes the
    ///     generic over Polecat's <see cref="IDocumentOperations"/>), then write the
    ///     replacement event and delete the originals through the session's work
    ///     tracker.
    /// </summary>
    internal static async Task ExecuteAsync<T>(this StreamCompactingRequest<T> request, DocumentSessionBase session)
        where T : class
    {
        // 1. Find the aggregator
        var aggregator = FindAggregator<T>(session);

        // 2. Fetch events
        IReadOnlyList<IEvent> events;
        if (session.Options.Events.StreamIdentity == StreamIdentity.AsGuid)
        {
            events = await session.Events.FetchStreamAsync(request.StreamId!.Value, request.Version, request.Timestamp,
                token: request.CancellationToken).ConfigureAwait(false);
        }
        else
        {
            events = await session.Events.FetchStreamAsync(request.StreamKey!, request.Version, request.Timestamp,
                token: request.CancellationToken).ConfigureAwait(false);
        }

        if (events.Count == 0) return;
        if (events is [{ Data: Compacted<T> }]) return;

        // Sequences of all events except the last (the last will be replaced with the compacted snapshot)
        var sequences = events.Select(x => x.Sequence).Take(events.Count - 1).ToArray();

        request.Version = events[events.Count - 1].Version;
        request.Sequence = events[events.Count - 1].Sequence;

        // 3. Aggregate to build the snapshot
        var aggregate = await aggregator.BuildAsync(events, session, default, request.CancellationToken)
            .ConfigureAwait(false);

        // 4. Optional archiving. The lifted IEventsArchiver marker is non-generic
        //    so the data class doesn't have to flow a TOperations parameter; the
        //    product downcasts to the closed-generic at execution time. Polecat's
        //    callbacks close on IDocumentOperations.
        if (request.Archiver is IEventsArchiver<IDocumentOperations> archiver)
        {
            await archiver.MaybeArchiveAsync(session, request, events, request.CancellationToken)
                .ConfigureAwait(false);
        }

        // 5. Replace the last event with the Compacted<T> snapshot
        var compacted = new Compacted<T>(aggregate!,
            request.StreamId ?? Guid.Empty, request.StreamKey ?? string.Empty);

        var serializedData = session.Serializer.ToJson(compacted);
        var mapping = session.Options.EventGraph.EventMappingFor(typeof(Compacted<T>));

        var replaceOp = new ReplaceEventOperation(
            session.Options.EventGraph, request.Sequence, serializedData,
            mapping.EventTypeName, mapping.DotNetTypeName);

        session.WorkTracker.Add(replaceOp);

        // 6. Delete the old events
        if (sequences.Length > 0)
        {
            session.WorkTracker.Add(new DeleteEventsOperation(session.Options.EventGraph, sequences));
        }
    }

    private static IAggregator<T, IQuerySession> FindAggregator<T>(DocumentSessionBase session) where T : class
    {
        return session.Options.Projections.AggregatorFor<T>();
    }
}
