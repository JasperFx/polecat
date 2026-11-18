using JasperFx.Events;
using JasperFx.Events.Protected;

namespace Polecat.Events.Protected;

/// <summary>
///     Polecat's half of <see cref="StreamCompactingExecution{TSession}" /> (jasperfx#1048, polecat#806).
///     The shared base decides what compaction does: read the stream (or the part of it up to the
///     request's cutoff), fold it, archive, replace the last event with a <see cref="Compacted{T}" />,
///     delete the rest, and record the watermark. This class says how, against one tenant-scoped
///     <see cref="EventOperations" />: the tenant of every operation queued here is the one those
///     operations were opened for, which is not always the session's (#905).
/// </summary>
/// <remarks>
///     Nothing here commits. Every operation joins the session's unit of work, so a compaction is
///     applied by the caller's <c>SaveChangesAsync</c>, as before.
/// </remarks>
internal sealed class PolecatStreamCompacting : StreamCompactingExecution<EventOperations>
{
    public static readonly PolecatStreamCompacting Instance = new();

    private PolecatStreamCompacting()
    {
    }

    protected override IEventStoreOperations EventsFor(EventOperations session) => session;

    protected override async ValueTask<T?> BuildAggregateAsync<T>(EventOperations session,
        IReadOnlyList<IEvent> events, CancellationToken token) where T : class
    {
        var aggregator = session.Session.Options.Projections.AggregatorFor<T>();
        var aggregate = await aggregator.BuildAsync(events, session.Session, default, token).ConfigureAwait(false);

        // Called ahead of anything being queued, and only with the events about to be folded: the last one becomes
        // the Compacted<T>, and the rest are deleted. A stream that folds to nothing is left alone anyway.
        if (aggregate != null && events.Count > 0)
        {
            await ensureNoAsyncShardIsBehindAsync<T>(session, events[^1], token).ConfigureAwait(false);
        }

        return aggregate;
    }

    /// <summary>
    ///     #1086 (marten#6050): refuses to compact while an async projection or subscription has not processed the
    ///     events being compacted. Compaction deletes them and rewrites the last one into a
    ///     <see cref="Compacted{T}" />, so a multi-stream projection, a composite, another view of the stream or a
    ///     subscription (Wolverine's event forwarding included) that had not reached them yet would never see them.
    ///     A single-stream projection of <typeparamref name="T" /> itself is exempt: it fast-forwards over the
    ///     snapshot. See <see cref="AsyncShardProgressGuard" /> for when a shard counts as behind.
    /// </summary>
    private static async Task ensureNoAsyncShardIsBehindAsync<T>(EventOperations session, IEvent last,
        CancellationToken token) where T : class
    {
        var behind = await AsyncShardProgressGuard.FindShardsBehindAsync(session, last.Sequence, typeof(T), token)
            .ConfigureAwait(false);

        if (behind.Count > 0)
        {
            throw new InvalidOperationException(
                $"Cannot compact stream '{(object?)last.StreamKey ?? last.StreamId}' through version {last.Version} " +
                $"(sequence {last.Sequence}) yet: these async projections or subscriptions have not processed its " +
                $"events: {string.Join(", ", behind.Select(x => $"{x.Name} (at {x.Sequence})"))}. Compacting " +
                "deletes them, so those would never see them. Nothing was compacted; try again once they caught up.");
        }
    }

    /// <remarks>
    ///     BOTH closures are accepted, and that is not belt-and-braces (#423).
    ///     <c>IEventsArchiver&lt;T&gt;</c> is invariant, so an archiver over <see cref="IDocumentSession" />
    ///     is not one over <see cref="IDocumentOperations" />. Polecat closes <c>IEventStore&lt;,&gt;</c>
    ///     over <see cref="IDocumentSession" />, which is what a JasperFx-generic caller hands in, while
    ///     hand-written Polecat archivers have used <see cref="IDocumentOperations" />. Anything that
    ///     matches neither is refused by the base class rather than skipped.
    /// </remarks>
    protected override async Task<bool> TryArchiveAsync<T>(EventOperations session,
        StreamCompactingRequest<T> request, IReadOnlyList<IEvent> events) where T : class
    {
        return await TryArchiveThroughAsync<IDocumentSession, T>(session.Session, request, events)
                   .ConfigureAwait(false)
               || await TryArchiveThroughAsync<IDocumentOperations, T>(session.Session, request, events)
                   .ConfigureAwait(false);
    }

    protected override void DeleteEvents(EventOperations session, long[] sequences)
    {
        session.Session.WorkTracker.Add(new DeleteEventsOperation(session.Graph, sequences, session.TenantId));
    }

    /// <summary>
    ///     jasperfx#740: the compaction watermark on <c>pc_streams</c>, which
    ///     <c>StreamState.CompactedVersion</c> reads back so (Version - CompactedVersion) measures
    ///     un-compacted growth.
    /// </summary>
    protected override void RecordCompactionWatermark(EventOperations session, Guid? streamId, string? streamKey,
        long version)
    {
        session.Session.WorkTracker.Add(new SetCompactedVersionOperation(session.Graph,
            (object?)streamId ?? streamKey!, version, session.TenantId));
    }
}
