using Microsoft.Extensions.Logging;
using Polecat.Storage;

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
    private readonly Dictionary<string, SqlServerAppLock> _locks = new();

    public MultiTenantedProjectionDistributor(DocumentStore store, ILoggerFactory loggerFactory)
    {
        _store = store;
        _logger = loggerFactory.CreateLogger<MultiTenantedProjectionDistributor>();
    }

    public ValueTask<IReadOnlyList<ProjectionSet>> BuildDistributionAsync()
    {
        var databases = _store.Options.Tenancy?.AllDatabases() ?? [];
        var allShards = _store.Options.Projections.AllShards()
            .Select(s => s.Name)
            .ToList();
        var lockId = _store.Options.DaemonSettings.DaemonLockId;

        IReadOnlyList<ProjectionSet> sets = databases
            .Select(db => new ProjectionSet(lockId, db, allShards))
            .OrderBy(_ => Random.Shared.NextDouble())
            .ToList();

        return ValueTask.FromResult(sets);
    }

    public Task RandomWait(CancellationToken token) =>
        Task.Delay(TimeSpan.FromMilliseconds(Random.Shared.Next(0, 500)), token);

    public bool HasLock(ProjectionSet set) =>
        _locks.TryGetValue(set.Database.Identifier, out var l) && l.HasLock(set.LockId);

    public Task<bool> TryAttainLockAsync(ProjectionSet set, CancellationToken token) =>
        LockFor(set.Database).TryAttainLockAsync(set.LockId, token);

    public Task ReleaseLockAsync(ProjectionSet set) =>
        LockFor(set.Database).ReleaseLockAsync(set.LockId);

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

    private SqlServerAppLock LockFor(PolecatDatabase database)
    {
        if (!_locks.TryGetValue(database.Identifier, out var l))
        {
            l = new SqlServerAppLock(database.ConnectionString, database.Identifier, _logger);
            _locks[database.Identifier] = l;
        }
        return l;
    }
}
