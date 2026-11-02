using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
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
///     <see cref="ManagedTenantPartitions.AddPartitionsToAllTables(ILogger,Weasel.Core.Migrations.IDatabase{Microsoft.Data.SqlClient.SqlConnection},IEnumerable{string},CancellationToken)" />).
///     <see cref="TryGetOrdinal" /> is the synchronous read used by SQL-building closures that run
///     strictly after a resolve (the append planner, flush-time binders).
///     </para>
///     <para>
///     #892: having an ordinal and having a partition are two different things. Weasel registers the
///     ordinal and then isolates each table's SPLIT, so a SPLIT that loses to another session's lock
///     still returns the ordinal, and the write goes ahead into the neighbouring partition. That is the
///     chosen outcome, because a busy <c>pc_streams</c> should not become a failed write, so the tenant
///     keeps its ordinal here and is also recorded as unsplit until a later resolve gets the SPLIT
///     through. SQL Server moves the misplaced rows when that happens.
///     </para>
/// </summary>
internal sealed class TenantPartitionOrdinalRegistry
{
    private readonly ManagedTenantPartitions _partitions;
    private readonly PolecatDatabase _database;

    private readonly ILogger _logger;

    private readonly ConcurrentDictionary<string, int> _cache = new(StringComparer.Ordinal);

    // #892: tenants whose SPLIT failed on at least one table, and the Environment.TickCount64 from which
    // the next attempt may start. Claiming a retry is what moves the value on, so a save with five streams
    // for the tenant, or fifty saves at once, make one attempt between them.
    private readonly ConcurrentDictionary<string, long> _unsplit = new(StringComparer.Ordinal);

    private PolecatDatabase? _splitRetryDatabase;

    public TenantPartitionOrdinalRegistry(ManagedTenantPartitions partitions, PolecatDatabase database,
        ILogger logger)
    {
        _partitions = partitions;
        _database = database;
        _logger = logger;
    }

    /// <summary>
    ///     How long a retried SPLIT may run, in seconds, where a first attempt has the connection's 30
    ///     second default. It is short because of who else pays: while a SPLIT waits for its
    ///     schema-modification lock, later readers and writers of the table queue behind it, so every
    ///     tenant waits out this attempt and not only the one that made it. It covers moving the rows as
    ///     well as waiting for the lock, because the connection's <c>Command Timeout</c> is the only limit
    ///     that can be set from here. See <see cref="PolecatDatabase.WithCommandTimeout" />.
    /// </summary>
    internal int SplitRetryTimeoutSeconds { get; set; } = 3;

    /// <summary>
    ///     The least time between two attempts at one tenant's SPLIT in this process. With
    ///     <see cref="SplitRetryTimeoutSeconds" />, this caps the share of the time a tenant that keeps
    ///     failing can hold everyone else up.
    /// </summary>
    internal TimeSpan SplitRetryInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     The tenant's partition ordinal, provisioning the registry row + physical partitions on
    ///     first use (idempotent — an already-registered tenant just hydrates the cache).
    /// </summary>
    /// <param name="tenantId">The tenant.</param>
    /// <param name="token">The token.</param>
    /// <param name="retryFailedSplit">
    ///     #892: whether a tenant whose SPLIT failed should try it again now. A SPLIT cannot run behind a
    ///     lock the caller holds on the table (#864), so anything inside a transaction passes false. It
    ///     changes nothing for a tenant with no ordinal yet, which has to be provisioned for the write to
    ///     happen at all.
    /// </param>
    public async ValueTask<int> ResolveAsync(string tenantId, CancellationToken token, bool retryFailedSplit = true)
    {
        if (_cache.TryGetValue(tenantId, out var cached))
        {
            if (retryFailedSplit && tryClaimSplitRetry(tenantId))
            {
                await retrySplitAsync(tenantId, cached, token).ConfigureAwait(false);
            }

            return cached;
        }

        var result = await _partitions.AddPartitionsToAllTables(_logger, _database, new[] { tenantId }, token)
            .ConfigureAwait(false);

        var ordinal = result.Ordinals[tenantId];
        recordSplitOutcome(tenantId, ordinal, result.Tables);

        return _cache.GetOrAdd(tenantId, ordinal);
    }

    private bool tryClaimSplitRetry(string tenantId)
    {
        if (!_unsplit.TryGetValue(tenantId, out var due)) return false;

        var now = Environment.TickCount64;
        return now >= due && _unsplit.TryUpdate(tenantId, now + (long)SplitRetryInterval.TotalMilliseconds, due);
    }

    private async Task retrySplitAsync(string tenantId, int ordinal, CancellationToken token)
    {
        _splitRetryDatabase ??= _database.WithCommandTimeout(SplitRetryTimeoutSeconds);

        try
        {
            var result = await _partitions
                .AddPartitionsToAllTables(_logger, _splitRetryDatabase, new[] { tenantId }, token)
                .ConfigureAwait(false);

            recordSplitOutcome(tenantId, ordinal, result.Tables);
        }
        catch (Exception e) when (!token.IsCancellationRequested)
        {
            // Unlike a first attempt, nothing about this write depends on the answer, so a retry that
            // could not even run is not worth failing the write for.
            _logger.LogWarning(e,
                "Retrying the partition SPLIT for tenant {TenantId} (ordinal {Ordinal}) failed. A later save will try again.",
                tenantId, ordinal);
        }
    }

    private void recordSplitOutcome(string tenantId, int ordinal, TablePartitionStatus[] tables)
    {
        var failed = tables
            .Where(x => x.Status != PartitionMigrationStatus.Complete)
            .Select(x => x.Identifier.QualifiedName)
            .ToArray();

        if (failed.Length == 0)
        {
            if (_unsplit.TryRemove(tenantId, out _))
            {
                _logger.LogInformation("The partition SPLIT for tenant {TenantId} (ordinal {Ordinal}) has caught up.",
                    tenantId, ordinal);
            }

            return;
        }

        _unsplit[tenantId] = Environment.TickCount64 + (long)SplitRetryInterval.TotalMilliseconds;

        _logger.LogWarning(
            "The partition SPLIT for tenant {TenantId} (ordinal {Ordinal}) failed on {Tables}. The tenant's rows on those tables go into a neighbouring partition until it succeeds, and it reads them correctly in the meantime. A later save retries the SPLIT, which moves the rows, and AddPolecatManagedTenantsAsync does the same on demand.",
            tenantId, ordinal, string.Join(", ", failed));
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
    public void Evict(string tenantId)
    {
        _cache.TryRemove(tenantId, out _);
        _unsplit.TryRemove(tenantId, out _);
    }
}
