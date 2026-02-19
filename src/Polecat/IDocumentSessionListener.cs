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
    ///     Called after the transaction has been committed successfully.
    /// </summary>
    Task AfterCommitAsync(IDocumentSession session, CancellationToken token);
}
