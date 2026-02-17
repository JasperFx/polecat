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
}
