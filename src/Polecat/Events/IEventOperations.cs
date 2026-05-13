using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Events.Protected;

namespace Polecat.Events;

/// <summary>
///     Polecat's combined session-level event-store API: read + write + aggregate-handler
///     workflow.
/// </summary>
/// <remarks>
/// Polecat 4 dedupe pillar: the database-agnostic surface (Append, StartStream,
/// FetchForWriting, WriteToAggregate, AppendOptimistic/Exclusive, FetchLatest,
/// ProjectLatest, ArchiveStream, tag queries, natural-key fetches, OverwriteEvent,
/// CompletelyReplaceEvent) now lives in <see cref="JasperFx.Events.IEventStoreOperations"/>
/// and its parent <see cref="JasperFx.Events.IEventOperations"/>. This interface
/// inherits the canonical contracts and adds the Polecat-specific extras:
/// <list type="bullet">
///   <item><c>UnArchiveStream</c> + <c>TombstoneStream</c> — Polecat-specific
///   archive-lifecycle operations not yet present in the canonical surface.</item>
///   <item><c>FetchForWritingByTags&lt;T&gt;</c> — DCB workflow returning
///   <see cref="IEventBoundary{T}"/>. Lifts to JFx.Events once Polecat reaches DCB
///   parity (JasperFx/polecat#80).</item>
///   <item><c>CompactStreamAsync&lt;T&gt;</c> — execution depends on Polecat's
///   <see cref="StreamCompactingRequest{T}"/>.</item>
/// </list>
/// Note: Marten's 3-tier split (IQueryEventStore + IEventOperations + IEventStoreOperations)
/// is the canonical shape per the dedupe pillar. Polecat retains its 2-tier shape for
/// now; the structural split is a follow-up issue (see Polecat 4 migration guide).
/// </remarks>
public interface IEventOperations : JasperFx.Events.IEventStoreOperations, IQueryEventStore
{
    /// <summary>
    ///     Remove the archived flag from a stream and all its events by Guid id.
    /// </summary>
    void UnArchiveStream(Guid streamId);

    /// <summary>
    ///     Remove the archived flag from a stream and all its events by string key.
    /// </summary>
    void UnArchiveStream(string streamKey);

    /// <summary>
    ///     Permanently delete a stream and all its events (hard DELETE) by Guid id.
    /// </summary>
    void TombstoneStream(Guid streamId);

    /// <summary>
    ///     Permanently delete a stream and all its events (hard DELETE) by string key.
    /// </summary>
    void TombstoneStream(string streamKey);

    /// <summary>
    ///     Fetch events by tags and return a writable boundary with DCB consistency checking.
    /// </summary>
    Task<IEventBoundary<T>> FetchForWritingByTags<T>(EventTagQuery query, CancellationToken cancellation = default) where T : class;

    /// <summary>
    ///     Compact a stream by replacing its events with a single Compacted&lt;T&gt; snapshot event.
    /// </summary>
    Task CompactStreamAsync<T>(Guid streamId, Action<StreamCompactingRequest<T>>? configure = null) where T : class;

    /// <summary>
    ///     Compact a stream by replacing its events with a single Compacted&lt;T&gt; snapshot event.
    /// </summary>
    Task CompactStreamAsync<T>(string streamKey, Action<StreamCompactingRequest<T>>? configure = null) where T : class;
}
