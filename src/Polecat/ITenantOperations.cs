using Polecat.Events;

namespace Polecat;

/// <summary>
///     Provides document and event operations scoped to a specific tenant.
///     Created by calling ForTenant() on an IDocumentSession.
/// </summary>
public interface ITenantOperations : IDocumentOperations
{
    /// <summary>
    ///     The tenant id of this tenant operations scope.
    /// </summary>
    new string TenantId { get; }

    /// <summary>
    ///     The parent IDocumentSession that created this tenant operations scope.
    /// </summary>
    IDocumentSession Parent { get; }

    /// <summary>
    ///     Event store operations scoped to this tenant.
    /// </summary>
    new IEventOperations Events { get; }
}
