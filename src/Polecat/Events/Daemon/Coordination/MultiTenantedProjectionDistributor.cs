using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     Multi-node, multi-tenant distributor. One set per database — all shards
///     for a given tenant database run together on whichever node holds that
///     database's lock. Trades per-shard scaling for the simpler invariant
///     that a tenant's projections never split across nodes.
/// </summary>
internal sealed class MultiTenantedProjectionDistributor : IProjectionDistributor
{
    private readonly DocumentStore _store;
    private readonly ILogger _logger;
    private readonly Dictionary<string, IAdvisoryLock> _locks = new();

    public MultiTenantedProjectionDistributor(DocumentStore store, ILoggerFactory loggerFactory)
    {
        _store = store;
        _logger = loggerFactory.CreateLogger<MultiTenantedProjectionDistributor>();
    }

    public ValueTask<IReadOnlyList<IProjectionSet>> BuildDistributionAsync()
    {
        var databases = _store.Options.Tenancy?.AllDatabases() ?? [];
        var allShards = _store.Options.Projections.AllShards()
            .Select(s => s.Name)
            .ToList();
        var lockId = _store.Options.DaemonSettings.DaemonLockId;

        IReadOnlyList<IProjectionSet> sets = databases
            .Select(db => (IProjectionSet)new ProjectionSet(lockId, db, allShards))
            .OrderBy(_ => Random.Shared.NextDouble())
            .ToList();

        return ValueTask.FromResult(sets);
    }

    public Task RandomWait(CancellationToken token) =>
        Task.Delay(TimeSpan.FromMilliseconds(Random.Shared.Next(0, 500)), token);

    public bool HasLock(IProjectionSet set) =>
        _locks.TryGetValue(set.Database.Identifier, out var l) && l.HasLock(set.LockId);

    public Task<bool> TryAttainLockAsync(IProjectionSet set, CancellationToken token) =>
        LockFor((PolecatDatabase)set.Database).TryAttainLockAsync(set.LockId, token);

    public Task ReleaseLockAsync(IProjectionSet set) =>
        LockFor((PolecatDatabase)set.Database).ReleaseLockAsync(set.LockId);

    public async Task ReleaseAllLocks()
    {
        foreach (var (_, l) in _locks)
        {
            try
            {
                await l.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Best-effort during shutdown
            }
        }
        _locks.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        await ReleaseAllLocks().ConfigureAwait(false);
    }

    private IAdvisoryLock LockFor(PolecatDatabase database)
    {
        if (!_locks.TryGetValue(database.Identifier, out var l))
        {
            // Weasel.SqlServer.AdvisoryLock holds a single dedicated SqlConnection
            // for the lifetime of the lock owner — sp_getapplock with
            // @LockOwner='Session' is bound to the connection, so the connection
            // must stay open as long as we hold any lock. The factory delegate
            // is invoked once on first acquisition.
            l = new AdvisoryLock(
                () => new SqlConnection(database.ConnectionString),
                _logger,
                database.Identifier);
            _locks[database.Identifier] = l;
        }
        return l;
    }
}
