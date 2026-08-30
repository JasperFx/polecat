using JasperFx.Descriptors;
using JasperFx.Events.Daemon;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Polecat.Internal;

/// <summary>
///     Hosted service that starts and stops the async projection daemon.
///     Registered by AddAsyncDaemon().
/// </summary>
public class PolecatDaemonHostedService : IHostedService, IAsyncDisposable
{
    private readonly DocumentStore _store;
    private readonly ILoggerFactory _loggerFactory;

    public PolecatDaemonHostedService(DocumentStore store, ILoggerFactory loggerFactory)
    {
        _store = store;
        _loggerFactory = loggerFactory;
    }

    private readonly List<IProjectionDaemon> _daemons = new();

    /// <summary>
    ///     The running projection daemon. Under database-per-tenant tenancy there is one per tenant
    ///     database — see <see cref="Daemons" />; this returns the first for backwards compatibility.
    /// </summary>
    public IProjectionDaemon? Daemon => _daemons.FirstOrDefault();

    /// <summary>
    ///     Every running projection daemon — one per tenant database under a multi-database tenancy,
    ///     otherwise a single daemon for the store's database.
    /// </summary>
    public IReadOnlyList<IProjectionDaemon> Daemons => _daemons;

    public async Task StartAsync(CancellationToken token)
    {
        var logger = _loggerFactory.CreateLogger<PolecatDaemonHostedService>();
        var tenancy = _store.Options.Tenancy;

        // #514: a database-per-tenant store needs a daemon PER tenant database. This used to build
        // exactly one, with no tenant, which resolved to whatever database backed
        // StoreOptions.ConnectionString — so every tenant but that one silently never had its
        // projections run, and nothing said so. Marten's AddAsyncDaemon registers the
        // ProjectionCoordinator, which fans out the same way.
        if (tenancy != null && tenancy.Cardinality != DatabaseCardinality.Single)
        {
            var databases = await tenancy.BuildDatabasesAsync(token);
            foreach (var database in databases)
            {
                // Matches what the single-database path gets from BuildProjectionDaemonAsync.
                await database.EnsureStorageExistsAsync(typeof(JasperFx.Events.IEvent), token);
                _daemons.Add(database.StartProjectionDaemon(_store, _loggerFactory));
            }
        }
        else
        {
            _daemons.Add(await _store.BuildProjectionDaemonAsync(logger: logger));
        }

        foreach (var daemon in _daemons)
        {
            await daemon.StartAllAsync();
        }
    }

    public async Task StopAsync(CancellationToken token)
    {
        foreach (var daemon in _daemons)
        {
            await daemon.StopAllAsync();
        }
    }

    public ValueTask DisposeAsync()
    {
        foreach (var daemon in _daemons)
        {
            daemon.Dispose();
        }

        _daemons.Clear();
        return ValueTask.CompletedTask;
    }
}
