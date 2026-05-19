using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     Multi-node, single-database distributor. One set per shard, so each
///     individual projection / subscription can run on a different node
///     (hot-cold leadership per shard) — maximizing throughput while still
///     enforcing single-writer semantics per shard.
/// </summary>
internal sealed class SingleTenantProjectionDistributor : IProjectionDistributor
{
    private readonly DocumentStore _store;
    private readonly ILogger _logger;
    private readonly Dictionary<string, Weasel.Core.IAdvisoryLock> _locks = new();

    public SingleTenantProjectionDistributor(DocumentStore store, ILoggerFactory loggerFactory)
    {
        _store = store;
        _logger = loggerFactory.CreateLogger<SingleTenantProjectionDistributor>();
    }

    public ValueTask<IReadOnlyList<IProjectionSet>> BuildDistributionAsync()
    {
        var databases = _store.Options.Tenancy?.AllDatabases() ?? [];
        var schema = _store.Options.DatabaseSchemaName;
        var baseLockId = _store.Options.DaemonSettings.DaemonLockId;

        var allShards = _store.Options.Projections.AllShards();

        IReadOnlyList<IProjectionSet> sets = databases
            .SelectMany(db => allShards.Select(shard =>
            {
                // Deterministic per (schema, shard) via JasperFx.Events'
                // ProjectionLockIds.Compute — same formula Marten's
                // SingleTenantProjectionDistributor uses, so Marten and Polecat
                // nodes co-deployed against the same SQL Server instance
                // negotiate identical lock ids. (Issue #117.)
                var lockId = ProjectionLockIds.Compute(schema, shard.Name, baseLockId);

                return (IProjectionSet)new ProjectionSet(lockId, db, [shard.Name]);
            }))
            // Random ordering so multiple nodes coming up together stagger
            // their lock-acquisition attempts.
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

    private Weasel.Core.IAdvisoryLock LockFor(PolecatDatabase database)
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
