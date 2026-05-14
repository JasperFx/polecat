using JasperFx;
using JasperFx.Core;
using JasperFx.Descriptors;
using JasperFx.Events.Daemon;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Storage;

namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     Concrete <see cref="IProjectionCoordinator"/> for Polecat. Hosted as
///     an <see cref="IHostedService"/> so the runtime starts/stops it with
///     the application lifetime.
/// </summary>
/// <remarks>
///     Owns a <see cref="IProjectionDistributor"/> chosen at construction
///     based on <see cref="ITenancy.Cardinality"/> (Solo / SingleTenant /
///     MultiTenanted). The execute loop polls the distributor for the
///     current view of (database × shards) sets and:
///
///     <list type="bullet">
///       <item>If we already hold a set's lock — keep its agents running.</item>
///       <item>If not, race for the lock; on success start the agents,
///       on failure stop any agents we used to run for that set
///       (recovery-from-lost-lock).</item>
///     </list>
///
///     Mirrors the Marten executeAsync loop slot-for-slot; the differences
///     are SQL Server (sp_getapplock) and Polecat tenancy types.
/// </remarks>
internal class ProjectionCoordinator : IProjectionCoordinator
{
    private readonly DocumentStore _store;
    private readonly StoreOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<ProjectionCoordinator> _logger;

    private readonly Dictionary<string, PolecatProjectionDaemon> _daemons = new();
    private IProjectionDistributor? _distributor;
    private CancellationTokenSource? _runCts;
    private Task? _runTask;
    private volatile bool _paused;

    public ProjectionCoordinator(DocumentStore store, ILoggerFactory? loggerFactory = null)
    {
        _store = store;
        _options = store.Options;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<ProjectionCoordinator>();
    }

    internal IProjectionDistributor Distributor => _distributor
        ??= BuildDistributor();

    public IProjectionDaemon DaemonForMainDatabase()
    {
        var main = _options.Tenancy?.AllDatabases().FirstOrDefault()
            ?? throw new InvalidOperationException(
                "No databases registered on the active tenancy; cannot resolve a main projection daemon.");
        return DaemonFor(main);
    }

    public ValueTask<IProjectionDaemon> DaemonForDatabase(string databaseIdentifier)
    {
        var match = _options.Tenancy?.AllDatabases()
            .FirstOrDefault(d => string.Equals(d.Identifier, databaseIdentifier, StringComparison.Ordinal))
            ?? throw new InvalidOperationException(
                $"No database registered with identifier '{databaseIdentifier}' on the active tenancy.");
        return ValueTask.FromResult<IProjectionDaemon>(DaemonFor(match));
    }

    public ValueTask<IReadOnlyList<IProjectionDaemon>> AllDaemonsAsync()
    {
        var databases = _options.Tenancy?.AllDatabases() ?? [];
        IReadOnlyList<IProjectionDaemon> daemons = databases.Select(DaemonFor).Cast<IProjectionDaemon>().ToList();
        return ValueTask.FromResult(daemons);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_runTask is not null) return Task.CompletedTask;

        _paused = false;
        _runCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runTask = Task.Run(() => ExecuteAsync(_runCts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_runCts is not null)
            {
                await _runCts.CancelAsync().ConfigureAwait(false);
            }

            if (_runTask is not null)
            {
                try
                {
                    await _runTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Graceful shutdown — nothing to do.
                }
            }
        }
        finally
        {
            await StopAllAgentsAsync().ConfigureAwait(false);

            if (_distributor is not null)
            {
                await _distributor.DisposeAsync().ConfigureAwait(false);
                _distributor = null;
            }

            _runTask = null;
            _runCts?.Dispose();
            _runCts = null;
        }
    }

    public async Task PauseAsync()
    {
        _paused = true;

        if (_runCts is not null)
        {
            await _runCts.CancelAsync().ConfigureAwait(false);
        }

        if (_runTask is not null)
        {
            try
            {
                await _runTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        await StopAllAgentsAsync().ConfigureAwait(false);

        _runTask = null;
        _runCts?.Dispose();
        _runCts = null;
    }

    public Task ResumeAsync()
    {
        if (!_paused) return Task.CompletedTask;
        _paused = false;

        _runCts = new CancellationTokenSource();
        _runTask = Task.Run(() => ExecuteAsync(_runCts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    /// <summary>
    ///     The leadership-election + agent-lifecycle loop. Mirrors Marten's
    ///     <c>ProjectionCoordinator.executeAsync</c> body slot-for-slot
    ///     (modulo SQL Server idioms via <see cref="SqlServerAppLock"/>).
    /// </summary>
    private async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var distributor = Distributor;

        await distributor.RandomWait(stoppingToken).ConfigureAwait(false);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var sets = await distributor.BuildDistributionAsync().ConfigureAwait(false);

                foreach (var set in sets)
                {
                    if (stoppingToken.IsCancellationRequested) return;

                    if (distributor.HasLock(set))
                    {
                        var daemon = DaemonFor(set.Database);
                        await StartAgentsIfNecessaryAsync(set, daemon, stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    try
                    {
                        if (await distributor.TryAttainLockAsync(set, stoppingToken).ConfigureAwait(false))
                        {
                            var daemon = DaemonFor(set.Database);
                            await StartAgentsIfNecessaryAsync(set, daemon, stoppingToken).ConfigureAwait(false);
                        }
                        else
                        {
                            // Don't hold the lock — make sure we're not still
                            // running these shards from a previous round where
                            // we did own them.
                            var daemon = DaemonFor(set.Database);
                            await StopAgentsIfNecessaryAsync(set, daemon).ConfigureAwait(false);
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e,
                            "Error attempting lock for set {Names} (lock id {LockId}). Will retry next cycle.",
                            string.Join(", ", set.Names.Select(n => n.Identity)), set.LockId);

                        await Task.Delay(TimeSpan.FromMilliseconds(_options.DaemonSettings.LeadershipPollingTime),
                            stoppingToken).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception e) when (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogError(e, "Error resolving the projection distribution.");
            }

            if (stoppingToken.IsCancellationRequested) return;

            try
            {
                var anyPaused = _daemons.Values.Any(d => d.HasAnyPaused());
                var delay = anyPaused
                    ? _options.DaemonSettings.AgentPauseTime
                    : TimeSpan.FromMilliseconds(_options.DaemonSettings.LeadershipPollingTime);
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                // graceful shutdown
            }
            catch (OperationCanceledException)
            {
                // graceful shutdown
            }
        }
    }

    private static async Task StartAgentsIfNecessaryAsync(
        ProjectionSet set, PolecatProjectionDaemon daemon, CancellationToken token)
    {
        foreach (var name in set.Names)
        {
            if (token.IsCancellationRequested) return;
            if (daemon.StatusFor(name.Identity) == AgentStatus.Running) continue;
            await daemon.StartAgentAsync(name.Identity, token).ConfigureAwait(false);
        }
    }

    private static async Task StopAgentsIfNecessaryAsync(
        ProjectionSet set, PolecatProjectionDaemon daemon)
    {
        foreach (var name in set.Names)
        {
            if (daemon.StatusFor(name.Identity) == AgentStatus.Stopped) continue;
            try
            {
                await daemon.StopAgentAsync(name).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort — losing the lock is not exceptional, but
                // we don't want a stop failure to take down the loop.
            }
        }
    }

    private async Task StopAllAgentsAsync()
    {
        foreach (var daemon in _daemons.Values)
        {
            try
            {
                await daemon.StopAllAsync().ConfigureAwait(false);
            }
            catch
            {
                // Best-effort during shutdown
            }
        }
    }

    private PolecatProjectionDaemon DaemonFor(PolecatDatabase database)
    {
        if (!_daemons.TryGetValue(database.Identifier, out var daemon))
        {
            daemon = database.StartProjectionDaemon(_store, _loggerFactory);
            _daemons[database.Identifier] = daemon;
        }
        return daemon;
    }

    private IProjectionDistributor BuildDistributor()
    {
        // Mirrors Marten's pick: tenancy with multiple databases →
        // MultiTenantedProjectionDistributor; single-database tenancy →
        // SingleTenantProjectionDistributor (per-shard hot-cold locks via
        // sp_getapplock). SoloProjectionDistributor is reachable via
        // ProjectionCoordinator subclassing for tests / opt-out scenarios
        // but not auto-selected — production single-node deployments
        // benefit from the lock-based coordination too because it's the
        // only safe path when a second instance shows up unexpectedly.
        var cardinality = _options.Tenancy?.Cardinality ?? DatabaseCardinality.Single;

        return cardinality == DatabaseCardinality.StaticMultiple
            ? new MultiTenantedProjectionDistributor(_store, _loggerFactory)
            : new SingleTenantProjectionDistributor(_store, _loggerFactory);
    }
}

/// <summary>
///     Typed marker variant of <see cref="ProjectionCoordinator"/> for
///     ancillary store registrations in DI.
/// </summary>
internal sealed class ProjectionCoordinator<T> : ProjectionCoordinator, IProjectionCoordinator<T>
    where T : IDocumentStore
{
    public ProjectionCoordinator(DocumentStore store, ILoggerFactory? loggerFactory = null)
        : base(store, loggerFactory)
    {
    }
}
