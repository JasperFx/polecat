using JasperFx.Events;
using Polecat.Events;

namespace Polecat;

/// <summary>
///     Full document session with mutation operations and SaveChanges.
///     This is the primary unit of work for Polecat.
/// </summary>
public interface IDocumentSession : IDocumentOperations, IStorageOperations
{
    /// <summary>
    ///     Read-only view of pending operations.
    /// </summary>
    IWorkTracker PendingChanges { get; }

    /// <summary>
    ///     Event store operations (append, start stream, fetch).
    /// </summary>
    new IEventOperations Events { get; }

    /// <summary>
    ///     Flush all pending operations to the database in a single transaction.
    /// </summary>
    Task SaveChangesAsync(CancellationToken token = default);

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
