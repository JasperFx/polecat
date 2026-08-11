using JasperFx.Events;
using JasperFx.Events.Documents;

namespace Polecat;

/// <summary>
///     The main entry point for Polecat. Creates sessions for document and event operations.
///     Typically registered as a singleton in DI.
/// </summary>
/// <remarks>
///     #443 / jasperfx#647: also Polecat's implementation of
///     <see cref="IDocumentSessionFactory{TOperations,TQuerySession}" />. The generic form layers over
///     the non-generic one exactly as <c>IEventStore&lt;,&gt;</c> layers over <c>IEventStore</c>, and it
///     is the non-generic form that lets a consumer open sessions without referencing Polecat at all.
/// </remarks>
public interface IDocumentStore : IDisposable, IAsyncDisposable, IDocumentStoreUsageSource,
    IDocumentSessionFactory<IDocumentSession, IQuerySession>
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
    ///     #443: the non-generic tier of <see cref="IDocumentSessionFactory" /> returns the shared
    ///     contract types. C# does not allow a covariant return to implement an interface member
    ///     implicitly, so the two are bridged explicitly here — once, rather than in every store type.
    /// </summary>
    IDocumentSessionOperations IDocumentSessionFactory.LightweightSession() => LightweightSession();

    /// <inheritdoc cref="IDocumentSessionFactory.LightweightSession()" />
    IDocumentReadOperations IDocumentSessionFactory.QuerySession() => QuerySession();

    /// <summary>
    ///     Open a read-only query session with custom options.
    /// </summary>
    IQuerySession QuerySession(SessionOptions options);

    /// <summary>
    ///     Open a session with the specified session options.
    ///     Session type is determined by the Tracking property.
    /// </summary>
    IDocumentSession OpenSession(SessionOptions options);

    /// <summary>
    ///     Open a session asynchronously with the specified options.
    ///     If IsolationLevel is not ReadCommitted, eagerly opens a connection
    ///     and begins a transaction with the specified isolation level.
    /// </summary>
    Task<IDocumentSession> OpenSessionAsync(SessionOptions options, CancellationToken token = default);
}
