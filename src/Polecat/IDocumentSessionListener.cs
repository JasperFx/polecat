using Polecat.Services;

namespace Polecat;

/// <summary>
///     Lifecycle hooks for document session save operations.
/// </summary>
public interface IDocumentSessionListener
{
    /// <summary>
    ///     Called before the transaction begins in SaveChangesAsync.
    /// </summary>
    Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token);

    /// <summary>
    ///     Called after the transaction has been committed successfully. The <paramref name="commit" />
    ///     is a snapshot of what was written (inserted/updated/deleted documents and events) in this unit
    ///     of work, taken before the session's pending changes were reset.
    /// </summary>
    Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token);
}
