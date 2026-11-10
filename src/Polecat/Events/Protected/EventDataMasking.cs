using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using JasperFx.Events;
using JasperFx.Events.Protected;
using Polecat.Linq;

namespace Polecat.Events.Protected;

/*
 * IEventDataMasking used to be declared here. It was lifted into JasperFx.Events.Protected in
 * jasperfx#635 (2.41.0), beside the StreamCompactingRequest<T> it belongs with: a fluent
 * description of a masking intent is database-agnostic, while executing it is unavoidably
 * store-specific. Marten's copy and this one were member-for-member identical -- ForTenant, four
 * IncludeStream overloads, IncludeEvents, AddHeader, same order and same signatures -- which is
 * what made the lift a straight swap.
 *
 * This DOES move the interface's namespace, so user code that names Polecat.Events.Protected.
 * IEventDataMasking explicitly has to update its using. The common fluent usage,
 * Advanced.ApplyEventDataMasking(x => x.IncludeStream(...)), never names the type.
 */

/// <summary>
///     Polecat's half of <see cref="EventDataMaskingBase{TSession}" /> (jasperfx#1048, polecat#806): fetches
///     the events a pass selects, archived ones included, through a lightweight session, applies the
///     store's masking rules, and overwrites the rows in one <c>SaveChangesAsync</c>, so a failure commits
///     nothing.
/// </summary>
public class EventDataMasking : EventDataMaskingBase<IDocumentSession>
{
    private readonly DocumentStore _store;

    public EventDataMasking(DocumentStore store)
    {
        _store = store;
    }

    protected override IDocumentSession OpenSession(string? tenantId)
    {
        return string.IsNullOrEmpty(tenantId)
            ? _store.LightweightSession()
            : _store.LightweightSession(new SessionOptions { TenantId = tenantId });
    }

    protected override IEventStoreOperations EventsFor(IDocumentSession session) => session.Events;

    protected override Task<IReadOnlyList<IEvent>> FetchStreamIncludingArchivedAsync(IDocumentSession session,
        Guid streamId, CancellationToken token)
        => eventsOf(session).FetchStreamIncludingArchivedAsync(streamId, token);

    protected override Task<IReadOnlyList<IEvent>> FetchStreamIncludingArchivedAsync(IDocumentSession session,
        string streamKey, CancellationToken token)
        => eventsOf(session).FetchStreamIncludingArchivedAsync(streamKey, token);

    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. This member cannot propagate the annotation -- it implements a JasperFx interface that does not carry one, and annotating an implementation alone is IL2046. A CONSUMER is still warned, at the public entry point they call. ⚠️ This is NOT the suppression #733 removed: that one asserted the path was SAFE. This one records that the path is unsafe and that the diagnostic has nowhere to go from here. Upstream: annotate the JasperFx interface.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Polecat's own use of its LINQ async wrappers, which carry [RequiresDynamicCode] since #733. This member cannot propagate the annotation -- it implements a JasperFx interface that does not carry one, and annotating an implementation alone is IL2046. A CONSUMER is still warned, at the public entry point they call. ⚠️ This is NOT the suppression #733 removed: that one asserted the path was SAFE. This one records that the path is unsafe and that the diagnostic has nowhere to go from here. Upstream: annotate the JasperFx interface.")]
    protected override Task<IReadOnlyList<IEvent>> QueryEventsIncludingArchivedAsync(IDocumentSession session,
        Expression<Func<IEvent, bool>> filter, CancellationToken token)
        => eventsOf(session).QueryAllRawEventsIncludingArchived().Where(filter).ToListAsync(token);

    protected override bool TryMask(IEvent @event) => _store.Events.TryMask(@event);

    protected override Task SaveChangesAsync(IDocumentSession session, CancellationToken token)
        => session.SaveChangesAsync(token);

    // Archiving a stream takes it out of the normal reads and leaves it on disk, so masking has to reach it.
    // The public reads leave archived rows out, which found nothing, erased nothing and reported success,
    // so every source here goes through the store's own reads that keep them. A filter naming IsArchived
    // still narrows IncludeEvents, so it can still rule them back out.
    private static QueryEventStore eventsOf(IDocumentSession session) => (QueryEventStore)session.Events;
}
