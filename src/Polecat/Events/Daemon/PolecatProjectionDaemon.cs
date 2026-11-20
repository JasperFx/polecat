using JasperFx;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using JasperFx.Events.Projections;
using Microsoft.Extensions.Logging;
using Polecat.Projections;
using Polecat.Storage;

namespace Polecat.Events.Daemon;

/// <summary>
///     Polecat's async projection daemon. Thin wrapper over JasperFxAsyncDaemon
///     that wires Polecat's event store, database, and projection graph.
/// </summary>
public class PolecatProjectionDaemon
    : JasperFxAsyncDaemon<IDocumentSession, IQuerySession, IProjection>, IProjectionDaemon
{
    private readonly DocumentStore _store;

    public PolecatProjectionDaemon(
        DocumentStore store,
        PolecatDatabase database,
        ILoggerFactory loggerFactory,
        IHighWaterDetector detector)
        : base(store, database, loggerFactory, detector, store.Options.Projections)
    {
        _store = store;
        store.TrackDaemon(this);
        database.TrackDaemon(this);
        logIdentityAccess();
    }

    public PolecatProjectionDaemon(
        DocumentStore store,
        PolecatDatabase database,
        ILogger logger,
        IHighWaterDetector detector)
        : base(store, database, logger, detector, store.Options.Projections)
    {
        _store = store;
        store.TrackDaemon(this);
        database.TrackDaemon(this);
        logIdentityAccess();
    }

    /// <summary>
    ///     marten#6003 / #1053: say once, when the daemon is built, how each async projection finds one identity's
    ///     events, and so whether a failing identity parks or the shard pauses. Information when the store parks,
    ///     debug otherwise.
    /// </summary>
    private void logIdentityAccess()
    {
        var parking = _store.Options.Projections.Errors.ApplyErrors == ProjectionErrorAction.Park
                      && _store.Options.Events.EnableParkedIdentities;
        var level = parking ? LogLevel.Information : LogLevel.Debug;
        if (!Logger.IsEnabled(level)) return;

        foreach (var projection in _store.Options.Projections.All.Where(x => x.Lifecycle == ProjectionLifecycle.Async))
        {
            var access = (projection as IHasIdentityAccess)?.IdentityAccess;
            var parks = parking && access is { CanPark: true }
                        && (access.Source is not { } source
                            || ((JasperFx.Events.IEventStore<IDocumentSession, IQuerySession>)_store)
                            .SupportsIdentityEventSource(source));

            Logger.Log(level, "Projection {Projection} is {IdentityAccess}: a failing identity {Outcome}. {Reason}",
                projection.Name, access?.Class.ToString() ?? nameof(IdentityAccessClass.NotSupported),
                parks ? "parks"
                : _store.Options.Projections.Errors.ApplyErrors == ProjectionErrorAction.Skip
                    ? "skips the event"
                    : "pauses the shard",
                access?.Reason ?? "It is not sliced by identity.");
        }
    }

    /// <summary>
    ///     Rebuild one identity of a single-stream projection inside the shard that runs it (#1060,
    ///     jasperfx#1201). JasperFx's, with one refusal of Polecat's own in front of it.
    /// </summary>
    /// <remarks>
    ///     A projection whose documents are kept in custom projection storage (an EF Core entity, say) is refused
    ///     with <see cref="IdentityRebuildRefusal.NotSupportedByStore" />. The re-fold clears the identity with
    ///     <c>IProjectionStorage.HardDeleteById</c> inside the shard's own batch, and that storage need not
    ///     implement it. Finding that out inside the batch would fail the batch, so it is refused here, before
    ///     anything is queued. Rebuild such a projection with <c>RebuildProjectionAsync</c>.
    /// </remarks>
    public new Task<IdentityRebuildResult> RebuildIdentityAsync(string projectionName, string? tenantId,
        object identity, CancellationToken token)
        => RebuildIdentityAsync(projectionName,
            new IdentityRebuildRequest(tenantId ?? StorageConstants.DefaultTenantId, identity), token);

    /// <inheritdoc cref="RebuildIdentityAsync(string, string, object, CancellationToken)" />
    public new Task<IdentityRebuildResult> RebuildIdentityAsync(string projectionName,
        IdentityRebuildRequest request, CancellationToken token)
    {
        ArgumentNullException.ThrowIfNull(projectionName);
        ArgumentNullException.ThrowIfNull(request);

        if (_store.Options.Projections.TryFindProjection(projectionName, out var source)
            && source is IAggregateProjection aggregate
            && _store.Options.CustomProjectionStorageProviders.ContainsKey(aggregate.AggregateType))
        {
            return Task.FromException<IdentityRebuildResult>(new IdentityRebuildRefusedException(projectionName,
                request.Identity, IdentityRebuildRefusal.NotSupportedByStore,
                $"'{aggregate.AggregateType.Name}' is kept in custom projection storage, which Polecat cannot clear "
                + "inside the shard's batch. Rebuild the projection with RebuildProjectionAsync."));
        }

        return base.RebuildIdentityAsync(projectionName, request, token);
    }
}
