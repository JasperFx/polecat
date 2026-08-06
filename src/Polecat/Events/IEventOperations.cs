using JasperFx.Events;
using JasperFx.Events.Protected;
using JasperFx.Events.Tags;
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
/// </list>
/// <c>CompactStreamAsync&lt;T&gt;</c> was one of those extras until jasperfx#635 (2.41.0) lifted
/// it onto <see cref="JasperFx.Events.IEventStoreOperations"/> alongside the
/// <see cref="StreamCompactingRequest{T}"/> data shape it already shared. Execution still depends
/// on Polecat's product-specific <see cref="StreamCompactingExecution.ExecuteAsync{T}"/> extension;
/// only the declaration is now canonical.
/// Note: <c>FetchForWritingByTags&lt;T&gt;(EventTagQuery)</c> is now inherited
/// from <see cref="JasperFx.Events.IEventStoreOperations"/> per the dedupe
/// pillar (lifted in JasperFx/jasperfx#270 once Polecat reached DCB parity via
/// JasperFx/polecat#80). Polecat's <c>EventOperations</c> implements the
/// inherited declaration directly.
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

    // CompactStreamAsync<T> used to be declared here. It was lifted onto
    // JasperFx.Events.IEventStoreOperations in jasperfx#635 (2.41.0) as default-implemented members
    // that throw, so a store without compacting stays source-compatible. This interface already
    // inherits that one, so keeping the local copy would make the signature reachable through two
    // paths and every CALL SITE ambiguous (CS0121) -- the same break Marten took in marten#5194.
    // EventOperations still implements the overloads; only the declaration moved.

    // FetchLatest is declared on two parent interfaces — JasperFx.Events.IEventStoreOperations
    // (where Marten put it canonically) and Polecat.Events.IQueryEventStore (kept on the
    // read-side as a Polecat convenience). Both declarations have identical signatures
    // and resolve to the same impl on EventOperations, but the C# compiler can't pick
    // between them through Polecat.IEventOperations, so we re-declare with `new` to
    // collapse the diamond at this level and keep the caller-facing surface unambiguous.

    /// <inheritdoc cref="IQueryEventStore.FetchLatest{T}(Guid, CancellationToken)" />
    new ValueTask<T?> FetchLatest<T>(Guid id, CancellationToken cancellation = default) where T : class;

    /// <inheritdoc cref="IQueryEventStore.FetchLatest{T}(string, CancellationToken)" />
    new ValueTask<T?> FetchLatest<T>(string key, CancellationToken cancellation = default) where T : class;
}
