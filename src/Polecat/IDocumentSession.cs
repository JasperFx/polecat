using JasperFx.Events;
using JasperFx.Events.Documents;
using Polecat.Events;

namespace Polecat;

/// <summary>
///     Full document session with mutation operations and SaveChanges.
///     This is the primary unit of work for Polecat.
/// </summary>
/// <remarks>
///     #443 / jasperfx#647: also Polecat's implementation of <see cref="IDocumentSessionOperations" />
///     — the committable tier of the shared document contract, which is the one a consumer holding a
///     unit of work wants and a projection must not be handed.
/// </remarks>
public interface IDocumentSession : IDocumentOperations, IStorageOperations, ITransactionParticipantRegistrar,
    IDocumentSessionOperations
{
    /// <summary>
    ///     Read-only view of pending operations.
    /// </summary>
    IWorkTracker PendingChanges { get; }

    /// <summary>
    ///     Event store operations (append, start stream, fetch).
    /// </summary>
    new Polecat.Events.IEventOperations Events { get; }

    /// <summary>
    ///     #475 / jasperfx#669: the shared contract's write-tier <c>Events</c> accessor — the append
    ///     half, reachable from a session a consumer opened through
    ///     <see cref="IDocumentSessionFactory" /> without naming a Polecat type.
    /// </summary>
    /// <remarks>
    ///     The same non-covariance trap as <see cref="IQuerySession" />'s read-tier implementation, and
    ///     implementing one tier does <em>not</em> implement the other:
    ///     <see cref="Polecat.Events.IEventOperations" /> derives from
    ///     <see cref="JasperFx.Events.IEventStoreOperations" /> yet cannot satisfy the contract member
    ///     implicitly, and the contract's default throws rather than failing to compile.
    /// </remarks>
    IEventStoreOperations IDocumentSessionOperations.Events => Events;

    /// <summary>
    ///     #477 / jasperfx#673: the <see cref="StreamAction" />s this session has queued but not yet
    ///     committed, read through the shared contract.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Polecat already had the collection one hop away, and the payload type was already the
    ///         shared <see cref="StreamAction" /> — only the accessor's NAME diverged between the
    ///         products (Marten's <c>PendingChanges.Streams()</c>, this <c>PendingChanges.Streams</c>,
    ///         Fisher's <c>Events.PendingStreams</c>). So this closes a naming gap rather than a
    ///         capability gap, and a Polecat caller keeps reading
    ///         <see cref="PendingChanges" />.<see cref="IWorkTracker.Streams" /> directly.
    ///     </para>
    ///     <para>
    ///         It exists for code that did <em>not</em> do the appending — a listener, or a pre-commit
    ///         hook deciding something from what the session is about to write. Code at the call site
    ///         already holds the <see cref="StreamAction" />, because <c>StartStream</c> and
    ///         <c>Append</c> return it.
    ///     </para>
    ///     <para>
    ///         The contract's default <b>throws</b> rather than answering with an empty list, and here
    ///         that choice matters more than it did for <see cref="Events" />: empty is
    ///         indistinguishable from a session with nothing pending, so a store left on a silent
    ///         default would discard whatever a consumer derives from these actions with a clean
    ///         build and green tests.
    ///     </para>
    /// </remarks>
    IReadOnlyList<StreamAction> IDocumentSessionOperations.PendingStreams => PendingChanges.Streams;

    /// <summary>
    ///     Flush all pending operations to the database in a single transaction.
    /// </summary>
    new Task SaveChangesAsync(CancellationToken token = default);

    /// <summary>
    ///     Remove a specific document from the session's pending operations
    ///     and identity map (if applicable).
    /// </summary>
    void Eject<T>(T document) where T : notnull;

    /// <summary>
    ///     Remove all pending operations and identity map entries for the given document type.
    /// </summary>
    void EjectAllOfType(Type type);

    /// <summary>
    ///     Clear all pending document operations and stream actions.
    ///     Does not clear the identity map.
    /// </summary>
    void EjectAllPendingChanges();
}
