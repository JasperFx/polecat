using JasperFx.Events.Projections;
using Polecat.Events;

namespace Polecat.Internal;

/// <summary>
///     Lightweight session — no identity map or tracking.
/// </summary>
internal class LightweightSession : DocumentSessionBase
{
    public LightweightSession(
        StoreOptions options,
        ConnectionFactory connectionFactory,
        DocumentProviderRegistry providers,
        DocumentTableEnsurer tableEnsurer,
        EventGraph eventGraph,
        IInlineProjection<IDocumentSession>[] inlineProjections,
        string tenantId)
        : base(options, connectionFactory, providers, tableEnsurer, eventGraph, inlineProjections, tenantId)
    {
    }
}
