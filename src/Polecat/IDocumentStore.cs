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
    new IDocumentSession LightweightSession();

    /// <summary>
    ///     Open a lightweight session with custom options.
    /// </summary>
    IDocumentSession LightweightSession(SessionOptions options);

    /// <summary>
    ///     Open a lightweight session scoped to one tenant (#682 / jasperfx#898).
    /// </summary>
    new IDocumentSession LightweightSession(string tenantId);

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
    new IQuerySession QuerySession();

    /// <summary>
    ///     #443: the non-generic tier of <see cref="IDocumentSessionFactory" /> returns the shared
    ///     contract types. C# does not allow a covariant return to implement an interface member
    ///     implicitly, so the two are bridged explicitly here — once, rather than in every store type.
    /// </summary>
    IDocumentSessionOperations IDocumentSessionFactory.LightweightSession() => LightweightSession();

    /// <inheritdoc cref="IDocumentSessionFactory.LightweightSession()" />
    IDocumentReadOperations IDocumentSessionFactory.QuerySession() => QuerySession();

    /// <summary>
    ///     Open a read-only session scoped to one tenant (#682 / jasperfx#898).
    /// </summary>
    new IQuerySession QuerySession(string tenantId);

    /// <remarks>
    ///     ⚠️ <b>Both tiers of the tenant-scoped pair need an explicit forwarder, for the same reason
    ///     the parameterless pair above does, and jasperfx#898 states it because it is a near-miss
    ///     rather than a compile error.</b> C# interface implementation is not return-type covariant, so
    ///     <c>IDocumentSession LightweightSession(string)</c> satisfies
    ///     <see cref="IDocumentSessionFactory{TOperations,TQuerySession}" /> and leaves the non-generic
    ///     <see cref="IDocumentSessionFactory" /> member bound to its <b>throwing</b> default
    ///     implementation. A store that stopped here builds, runs, and then throws
    ///     <see cref="NotSupportedException" /> the moment a store-agnostic caller — the compliance
    ///     suites, Wolverine — opens a tenant-scoped session through the contract.
    /// </remarks>
    IDocumentSessionOperations IDocumentSessionFactory.LightweightSession(string tenantId)
        => LightweightSession(tenantId);

    /// <inheritdoc cref="IDocumentSessionFactory.LightweightSession(string)" />
    IDocumentReadOperations IDocumentSessionFactory.QuerySession(string tenantId)
        => QuerySession(tenantId);

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

    /// <summary>
    ///     marten#6004: wait until the asynchronous projections in <paramref name="projections" /> have processed
    ///     <paramref name="position" />, so you can read your own writes. Take the position from the session's
    ///     <c>LastAppendedPosition</c> after <c>SaveChangesAsync</c>, and pass the projection classes or the
    ///     documents they write.
    /// </summary>
    /// <remarks>
    ///     Polls <c>pc_event_progression</c>, so it works on any node. Waits for your events only, not for other
    ///     writers' events after them. Never throws on timeout: check <see cref="JasperFx.Events.Daemon.ProjectionWaitResult.Reached" />,
    ///     and when it is false, <see cref="JasperFx.Events.Daemon.ProjectionWaitResult.Shards" /> says how far each shard
    ///     got. A paused or rebuilding projection returns at once with that status.
    /// </remarks>
    Task<JasperFx.Events.Daemon.ProjectionWaitResult> WaitForProjectionsAsync(EventStorePosition position,
        IReadOnlyList<Type> projections, TimeSpan timeout, CancellationToken token = default)
        => throw new NotSupportedException(
            $"{GetType().FullName} does not implement {nameof(IDocumentStore)}.{nameof(WaitForProjectionsAsync)}.");

    /// <summary>
    ///     marten#6004: <see cref="WaitForProjectionsAsync(EventStorePosition, IReadOnlyList{Type}, TimeSpan, CancellationToken)" />
    ///     by projection or subscription name.
    /// </summary>
    Task<JasperFx.Events.Daemon.ProjectionWaitResult> WaitForProjectionsAsync(EventStorePosition position,
        IReadOnlyList<string> projectionNames, TimeSpan timeout, CancellationToken token = default)
        => throw new NotSupportedException(
            $"{GetType().FullName} does not implement {nameof(IDocumentStore)}.{nameof(WaitForProjectionsAsync)}.");
}
