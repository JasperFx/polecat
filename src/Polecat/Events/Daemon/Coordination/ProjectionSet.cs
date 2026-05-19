using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Storage;

namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     A group of projection shards scheduled together under a single distributed
///     lock. Polecat's concrete implementation of JasperFx.Events' lifted
///     <see cref="IProjectionSet"/> contract — exposes the database both as the
///     interface-typed <see cref="IProjectionDatabase"/> (for the lifted
///     coordinator plumbing) and as the concrete <see cref="PolecatDatabase"/>
///     (for distributor call sites that need the SQL Server connection string).
/// </summary>
/// <remarks>
///     For single-tenant deployments this is one set per shard (per-shard lock);
///     for multi-tenant deployments this is one set per database (all shards
///     grouped behind one per-database lock). Mirrors Marten's
///     <c>Marten.Events.Daemon.Coordination.ProjectionSet</c>.
/// </remarks>
internal sealed class ProjectionSet : IProjectionSet
{
    public ProjectionSet(int lockId, PolecatDatabase database, IReadOnlyList<ShardName> names)
    {
        LockId = lockId;
        Database = database;
        Names = names;
    }

    public int LockId { get; }

    /// <summary>
    ///     The Polecat database the shards in this set run against. Covariant
    ///     return type of <see cref="IProjectionSet.Database"/> — distributor
    ///     code accessing the connection string can use this typed view rather
    ///     than casting from <see cref="IProjectionDatabase"/>.
    /// </summary>
    public PolecatDatabase Database { get; }

    IProjectionDatabase IProjectionSet.Database => Database;

    public IReadOnlyList<ShardName> Names { get; }
}
