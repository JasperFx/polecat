namespace Polecat;

/// <summary>
///     The main entry point for Polecat. Creates sessions for document and event operations.
///     Typically registered as a singleton in DI.
/// </summary>
public interface IDocumentStore : IDisposable, IAsyncDisposable
{
    /// <summary>
    ///     The configuration options for this store.
    /// </summary>
    StoreOptions Options { get; }

    /// <summary>
    ///     Advanced operations including HiLo sequence management.
    /// </summary>
    AdvancedOperations Advanced { get; }

    /// <summary>
    ///     Open a lightweight session (no identity map).
    /// </summary>
    IDocumentSession LightweightSession();

    /// <summary>
    ///     Open a lightweight session with custom options.
    /// </summary>
    IDocumentSession LightweightSession(SessionOptions options);

    /// <summary>
    ///     Open a session with identity map tracking.
    /// </summary>
    IDocumentSession IdentitySession();

    /// <summary>
    ///     Open a session with identity map tracking and custom options.
    /// </summary>
    IDocumentSession IdentitySession(SessionOptions options);

    /// <summary>
    ///     Open a read-only query session.
    /// </summary>
    IQuerySession QuerySession();

    /// <summary>
    ///     Open a read-only query session with custom options.
    /// </summary>
    IQuerySession QuerySession(SessionOptions options);

    /// <summary>
    ///     Open a session with the specified session options.
    ///     Session type is determined by the Tracking property.
    /// </summary>
    IDocumentSession OpenSession(SessionOptions options);
}
