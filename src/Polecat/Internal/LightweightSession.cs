using JasperFx.Events.Projections;
using Polecat.Events;
using Polecat.Internal.Sessions;

namespace Polecat.Internal;

/// <summary>
///     Lightweight session — no identity map or tracking.
/// </summary>
internal class LightweightSession : DocumentSessionBase
{
    public LightweightSession(
        StoreOptions options,
        IAlwaysConnectedLifetime lifetime,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer,
        EventGraph eventGraph,
        IInlineProjection<IDocumentSession>[] inlineProjections,
        string tenantId,
        IReadOnlyList<IDocumentSessionListener>? sessionListeners = null,
        IReadOnlyList<JasperFx.Events.Documents.IDocumentCommitListener>? sessionCommitListeners = null)
        : base(options, lifetime, providers, tableEnsurer, eventGraph, inlineProjections, tenantId, sessionListeners,
            sessionCommitListeners)
    {
    }
}
