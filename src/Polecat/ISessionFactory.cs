namespace Polecat;

/// <summary>
///     Factory for creating document sessions. Registered as a singleton in DI.
///     Override to customize default session creation behavior.
/// </summary>
public interface ISessionFactory
{
    /// <summary>
    ///     Creates a read/write document session.
    ///     Default implementation returns a lightweight session.
    /// </summary>
    IDocumentSession OpenSession();

    /// <summary>
    ///     Creates a read-only query session.
    /// </summary>
    IQuerySession QuerySession();
}
