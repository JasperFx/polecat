using JasperFx.Events.Projections;
using Polecat.Storage;

namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     Strategy for distributing projection shards across Polecat nodes.
///     Mirrors Marten's distributor design: the coordinator polls
///     <see cref="BuildDistributionAsync"/>, races for locks via
///     <see cref="TryAttainLockAsync"/>, and starts/stops daemon agents
///     based on which sets it currently owns.
/// </summary>
internal interface IProjectionDistributor : IAsyncDisposable
{
    ValueTask<IReadOnlyList<ProjectionSet>> BuildDistributionAsync();
    Task RandomWait(CancellationToken token);
    bool HasLock(ProjectionSet set);
    Task<bool> TryAttainLockAsync(ProjectionSet set, CancellationToken token);
    Task ReleaseLockAsync(ProjectionSet set);
    Task ReleaseAllLocks();
}

/// <summary>
///     A set of projection shards that get scheduled together under a single
///     distributed lock. For single-tenant deployments this is one set per
///     shard (per-shard lock); for multi-tenant deployments this is one set
///     per database (all shards grouped behind one per-database lock).
/// </summary>
internal sealed class ProjectionSet
{
    public ProjectionSet(int lockId, PolecatDatabase database, IReadOnlyList<ShardName> names)
    {
        LockId = lockId;
        Database = database;
        Names = names;
    }

    public int LockId { get; }
    public PolecatDatabase Database { get; }
    public IReadOnlyList<ShardName> Names { get; }
}
