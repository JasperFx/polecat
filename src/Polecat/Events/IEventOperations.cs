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
/// inherits the canonical contracts.
/// <para>
/// Polecat 6.0 dropped the <c>new</c> re-declarations of <c>UnArchiveStream(Guid|string)</c>
/// (lifted for JasperFx/marten#5476), <c>TombstoneStream(Guid|string)</c> (jasperfx#1014) and
/// <c>CompactStreamAsync&lt;T&gt;(Guid|string, ...)</c> (jasperfx#635). They existed only so that
/// assemblies compiled against Polecat 5.x, whose member references name this interface, kept
/// binding (#774, #776). A major version breaks that anyway, so these members now come only from
/// <see cref="JasperFx.Events.IEventStoreOperations"/>; source that calls them through
/// <c>session.Events</c> compiles unchanged, but a 5.x-compiled assembly must be recompiled.
/// <see cref="StreamCompactingRequest{T}"/> is the shared data shape; execution still depends on
/// Polecat's product-specific <see cref="Polecat.Events.Protected.PolecatStreamCompacting"/> extension.
/// </para>
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
    // FetchLatest is declared on two parent interfaces — JasperFx.Events.IEventStoreOperations
    // (where Marten put it canonically) and Polecat.Events.IQueryEventStore (kept on the
    // read-side as a Polecat convenience). Both declarations have identical signatures
    // and resolve to the same impl on EventOperations, but the C# compiler can't pick
    // between them through Polecat.IEventOperations, so we re-declare with `new` to
    // collapse the diamond at this level and keep the caller-facing surface unambiguous.
    // Unlike the binary-compatibility shims 6.0 removed, this one is load-bearing at the SOURCE
    // level: without it every session.Events.FetchLatest<T>(...) call is CS0121.

    /// <inheritdoc cref="IQueryEventStore.FetchLatest{T}(Guid, CancellationToken)" />
    new ValueTask<T?> FetchLatest<T>(Guid id, CancellationToken cancellation = default) where T : class;

    /// <inheritdoc cref="IQueryEventStore.FetchLatest{T}(string, CancellationToken)" />
    new ValueTask<T?> FetchLatest<T>(string key, CancellationToken cancellation = default) where T : class;
}
