using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;

namespace Polecat.Events.TestSupport;

/// <summary>
///     Polecat's implementation of the JasperFx.Events projection scenario test harness, closing the
///     generic session pair over <see cref="IDocumentSession" /> / <see cref="IQuerySession" />. All
///     scripting and execution behavior lives on the
///     <see cref="JasperFx.Events.TestSupport.ProjectionScenario{TOperations,TQuerySession}" /> base
///     type; this class only supplies the store-specific seam.
/// </summary>
/// <remarks>
///     Replaced a seven-file copy of Marten's pre-lift harness (#404, jasperfx#616). The seam below
///     is deliberately the same shape as <c>EventStoreComplianceFixture</c>'s, including the
///     <c>object</c>-id load dispatch, so both are implemented the same way against the same store.
/// </remarks>
public class ProjectionScenario: JasperFx.Events.TestSupport.ProjectionScenario<IDocumentSession, IQuerySession>
{
    private readonly DocumentStore _store;

    public ProjectionScenario(DocumentStore store)
    {
        _store = store;
    }

    protected override bool HasAnyAsyncProjections => _store.Options.Projections.HasAnyAsyncProjections();

    /// <summary>
    ///     Hand the harness this store's daemon settings so it can borrow
    ///     <c>DaemonSettings.Wakeup</c> for the duration of the run (marten#5195).
    /// </summary>
    /// <remarks>
    ///     Opting in matters more on Polecat than on Marten. Polecat has no LISTEN/NOTIFY, so without
    ///     a wakeup the high-water agent pays the full polling interval at every batch boundary and a
    ///     scenario with a handful of commit-and-wait rounds spends most of its 30 second budget
    ///     asleep. The base restores whatever was there before and never displaces a wakeup the store
    ///     already had, so this is safe to answer unconditionally.
    /// </remarks>
    protected override DaemonSettings? DaemonSettings => _store.Options.DaemonSettings;

    /// <summary>
    ///     Wipe the event store, then exactly the document types the registered projections own —
    ///     not every table in the schema, which would take out documents a scenario deliberately
    ///     seeded beforehand.
    /// </summary>
    protected override async Task DeleteExistingDataAsync(CancellationToken ct)
    {
        await _store.Advanced.CleanAllEventDataAsync(ct).ConfigureAwait(false);

        foreach (var storageType in _store.Options.Projections.All.SelectMany(x => x.Options.StorageTypes))
        {
            await _store.Advanced.CleanAsync(storageType, ct).ConfigureAwait(false);
        }
    }

    protected override async ValueTask<IProjectionDaemon> BuildDaemonAsync(string? tenantId)
    {
        return await _store.BuildProjectionDaemonAsync(tenantId).ConfigureAwait(false);
    }

    protected override IDocumentSession OpenSession(string? tenantId)
    {
        return tenantId.IsNotEmpty()
            ? _store.LightweightSession(new SessionOptions { TenantId = tenantId })
            : _store.LightweightSession();
    }

    // No shared JasperFx interface declares SaveChangesAsync.
    protected override Task SaveChangesAsync(IDocumentSession session, CancellationToken ct)
    {
        return session.SaveChangesAsync(ct);
    }

    protected override IEventOperations EventsFor(IDocumentSession session)
    {
        return session.Events;
    }

    protected override Task<T?> LoadDocumentAsync<T>(IQuerySession session, object id, CancellationToken ct)
        where T : class
    {
        return id switch
        {
            Guid guidId => session.LoadAsync<T>(guidId, ct),
            int intId => session.LoadAsync<T>(intId, ct),
            long longId => session.LoadAsync<T>(longId, ct),
            string stringId => session.LoadAsync<T>(stringId, ct),
            _ => throw new ArgumentOutOfRangeException(nameof(id),
                $"Polecat cannot load documents by an identity of type {id.GetType().FullName}")
        };
    }
}
