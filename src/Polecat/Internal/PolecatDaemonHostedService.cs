using JasperFx.Events.Daemon;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polecat.Events.Daemon.Coordination;

namespace Polecat.Internal;

/// <summary>
///     What <c>AddAsyncDaemon()</c> used to host, kept as a view onto the projection coordinator so
///     <see cref="Daemon" /> and <see cref="Daemons" /> go on working for code that resolves this type.
/// </summary>
/// <remarks>
///     #847: this used to call <c>StartAllAsync</c> on one daemon per database and do nothing else. That
///     takes no lock and runs no loop, so <see cref="DaemonMode.HotCold" /> had no leader election, a paused
///     shard stayed paused until the process restarted, and the tenant list was read once. The coordinator
///     does all three, so <c>AddAsyncDaemon()</c> now registers it as Marten's does, and the coordinator is
///     the hosted service. This one is not registered as an <see cref="IHostedService" /> any more. It stays
///     one so that a caller who built it by hand can still start it, which starts its coordinator.
/// </remarks>
public class PolecatDaemonHostedService : IHostedService, IAsyncDisposable, IDisposable
{
    private readonly ProjectionCoordinator _coordinator;

    public PolecatDaemonHostedService(DocumentStore store, ILoggerFactory loggerFactory)
        : this(new ProjectionCoordinator(store, loggerFactory, soloTakesNoLocks: true))
    {
    }

    internal PolecatDaemonHostedService(ProjectionCoordinator coordinator)
    {
        _coordinator = coordinator;
    }

    /// <summary>
    ///     The projection daemon for the store's database. Under database-per-tenant tenancy there is one
    ///     per tenant database — see <see cref="Daemons" />; this returns the first for backwards
    ///     compatibility.
    /// </summary>
    public IProjectionDaemon? Daemon => Daemons.FirstOrDefault();

    /// <summary>
    ///     A projection daemon per database the tenancy has now — one per tenant database under a
    ///     multi-database tenancy, otherwise a single daemon for the store's database. The list follows the
    ///     tenancy, so a master table's tenants come and go from it. Under
    ///     <see cref="DaemonMode.HotCold" /> a daemon on a node that does not hold the lock has no agents.
    /// </summary>
    public IReadOnlyList<IProjectionDaemon> Daemons => _coordinator.AllDaemons();

    public Task StartAsync(CancellationToken token) => ((IHostedService)_coordinator).StartAsync(token);

    public Task StopAsync(CancellationToken token) => _coordinator.StopAsync(token);

    public ValueTask DisposeAsync() => _coordinator.DisposeAsync();

    public void Dispose() => _coordinator.Dispose();
}
