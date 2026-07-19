using System.Collections.Concurrent;
using Polecat.Storage;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Events.Schema;

/// <summary>
///     Resolves (and lazily provisions) each tenant's compact partition ordinal against the store's
///     shared <see cref="ManagedTenantPartitions" /> strategy (#335). This is the single tenant →
///     ordinal cache shared by every managed-tenant-partitioned table in the store: <c>pc_events</c>
///     and <c>pc_streams</c> under <see cref="EventGraph.UseTenantPartitionedEvents" />, and the
///     tenant-partitioned document tables under
///     <see cref="StorePolicies.PartitionMultiTenantedDocumentsUsingPolecatManagement" />.
///     <para>
///     <see cref="ResolveAsync" /> is idempotent — a known tenant returns its cached ordinal with no
///     database traffic; an unknown tenant is registered in <c>pc_tenant_partitions</c> and every
///     table wired to the strategy is SPLIT for the new ordinal (all via Weasel's
///     <see cref="ManagedTenantPartitions.AddPartitionToAllTables(Weasel.Core.Migrations.IDatabase{Microsoft.Data.SqlClient.SqlConnection},string,CancellationToken)" />).
///     <see cref="TryGetOrdinal" /> is the synchronous read used by SQL-building closures that run
///     strictly after a resolve (the append planner, flush-time binders).
///     </para>
/// </summary>
internal sealed class TenantPartitionOrdinalRegistry
{
    private readonly ManagedTenantPartitions _partitions;
    private readonly PolecatDatabase _database;

    private readonly ConcurrentDictionary<string, int> _cache = new(StringComparer.Ordinal);

    public TenantPartitionOrdinalRegistry(ManagedTenantPartitions partitions, PolecatDatabase database)
    {
        _partitions = partitions;
        _database = database;
    }

    /// <summary>
    ///     The tenant's partition ordinal, provisioning the registry row + physical partitions on
    ///     first use (idempotent — an already-registered tenant just hydrates the cache).
    /// </summary>
    public async ValueTask<int> ResolveAsync(string tenantId, CancellationToken token)
    {
        if (_cache.TryGetValue(tenantId, out var cached)) return cached;

        var ordinal = await _partitions.AddPartitionToAllTables(_database, tenantId, token)
            .ConfigureAwait(false);

        return _cache.GetOrAdd(tenantId, ordinal);
    }

    /// <summary>
    ///     Synchronous cache read for SQL-building code that runs after the planner/flush pipeline
    ///     has already resolved the tenant. Returns false when the tenant has not been resolved in
    ///     this process yet.
    /// </summary>
    public bool TryGetOrdinal(string tenantId, out int ordinal) => _cache.TryGetValue(tenantId, out ordinal);

    /// <summary>
    ///     Drop a tenant from the in-memory cache after
    ///     <see cref="AdvancedOperations.RemovePolecatManagedTenantsAsync(string[],CancellationToken)" />
    ///     removes it from the registry, so a later write for the same tenant re-provisions instead of
    ///     stamping a stale ordinal.
    /// </summary>
    public void Evict(string tenantId) => _cache.TryRemove(tenantId, out _);
}
