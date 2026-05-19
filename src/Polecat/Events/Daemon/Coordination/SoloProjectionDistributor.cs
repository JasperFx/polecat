using JasperFx.Events.Daemon;

namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     Single-node distributor — every set is owned by this node, no locks.
///     Use when the application is known not to scale out, or for local /
///     test deployments.
/// </summary>
internal sealed class SoloProjectionDistributor : IProjectionDistributor
{
    private readonly DocumentStore _store;

    public SoloProjectionDistributor(DocumentStore store)
    {
        _store = store;
    }

    public ValueTask<IReadOnlyList<IProjectionSet>> BuildDistributionAsync()
    {
        var databases = _store.Options.Tenancy?.AllDatabases() ?? [];
        var allShards = _store.Options.Projections.AllShards()
            .Select(s => s.Name)
            .ToList();

        IReadOnlyList<IProjectionSet> sets = databases
            .Select(db => new ProjectionSet(
                lockId: _store.Options.DaemonSettings.DaemonLockId,
                database: db,
                names: allShards))
            .ToList();

        return ValueTask.FromResult(sets);
    }

    public Task RandomWait(CancellationToken token) => Task.CompletedTask;

    public bool HasLock(IProjectionSet set) => true;

    public Task<bool> TryAttainLockAsync(IProjectionSet set, CancellationToken token) =>
        Task.FromResult(true);

    public Task ReleaseLockAsync(IProjectionSet set) => Task.CompletedTask;

    public Task ReleaseAllLocks() => Task.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
