using JasperFx.Events;

namespace Polecat.Internal;

/// <summary>
///     Built-in document-session listener (polecat#213) that forwards the events appended in each unit
///     of work to <see cref="IEventStoreInstrumentation.AppendObserver" />, for storage-agnostic
///     runtime append observation (CritterWatch#500). Auto-registered on every Polecat store —
///     primary and ancillary — when <c>JasperFxOptions.EnableAdvancedTracking</c> is on (see
///     <see cref="StoreOptions.ReadJasperFxOptions" />).
/// </summary>
/// <remarks>
///     Reads the change-set in <see cref="BeforeSaveChangesAsync" />: Polecat resets the work tracker
///     before the <c>AfterCommit</c> phase, so the appended events are only visible pre-commit — the
///     same reason <c>Wolverine.Polecat</c>'s append listener uses this hook. Each <see cref="IEvent" />
///     already carries its event type and stream id/key by this point.
/// </remarks>
internal sealed class EventAppendObservationListener : IDocumentSessionListener
{
    private readonly StoreOptions _options;

    public EventAppendObservationListener(StoreOptions options)
    {
        _options = options;
    }

    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
    {
        // Read lazily so an observer registered after this listener (the usual order) is still seen.
        var observer = _options.Events.AppendObserver;
        if (observer == null)
        {
            return Task.CompletedTask;
        }

        var events = session.PendingChanges.Streams.SelectMany(s => s.Events).ToList();
        if (events.Count == 0)
        {
            return Task.CompletedTask;
        }

        try
        {
            observer(events);
        }
        catch (Exception ex)
        {
            // Best-effort observation: an observer fault must never break the user's SaveChanges.
            session.Logger.LogFailure("IEventStoreInstrumentation.AppendObserver threw", ex);
        }

        return Task.CompletedTask;
    }

    public Task AfterCommitAsync(IDocumentSession session, CancellationToken token) => Task.CompletedTask;
}
