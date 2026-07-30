using Microsoft.Extensions.Logging;
using Weasel.SqlServer.Tables;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Storage;

/// <summary>
///     #386: drives the rolling time-window RANGE partitions configured through
///     <see cref="PartitioningExpression{T,TValue}.ByRollingRange(Weasel.Core.Partitioning.PartitionPeriod,int,int,TimeProvider)" />
///     (built on weasel#401).
///     <para>
///         Provisioning at the leading edge is already covered by ordinary schema migration: with a
///         <see cref="ManagedRangePartitions" /> attached, <c>CreateDelta</c> is purely additive, so a
///         window that has rolled forward diffs as "SPLIT in the new boundary" rather than as a rebuild
///         of the partition function and every table sitting on it. What migration deliberately does NOT
///         do is remove data, so retiring the aged periods at the trailing edge has to be driven
///         explicitly — that is what this type is for, and it is why the maintenance pass runs alongside
///         the startup schema application rather than inside it.
///     </para>
///     <para>
///         The managers are discovered from each database's own schema objects rather than tracked on
///         <see cref="StoreOptions" />, so this stays correct for a document type that grows a rolling
///         window later, and re-running it is always idempotent.
///     </para>
/// </summary>
internal static class RollingPartitions
{
    /// <summary>
    ///     Every distinct rolling-window manager attached to a table of this database. Reference identity
    ///     is the key, exactly as <see cref="ManagedRangePartitions.ResolveManagedTables" /> matches tables
    ///     back to their manager — so one manager shared across several document types rolls all of their
    ///     tables forward in a single pass.
    /// </summary>
    public static IReadOnlyList<ManagedRangePartitions> ManagersFor(PolecatDatabase database)
    {
        var managers = new List<ManagedRangePartitions>();

        foreach (var table in database.AllObjects().OfType<Table>())
        {
            if (table.SqlServerPartitioning is ManagedRangePartitions manager
                && !managers.Any(x => ReferenceEquals(x, manager)))
            {
                managers.Add(manager);
            }
        }

        return managers;
    }

    /// <summary>
    ///     Run the maintenance pass over every database.
    /// </summary>
    /// <param name="rollForward">SPLIT in the boundaries of the current window that do not exist yet.</param>
    /// <param name="dropAged">
    ///     Retire the periods that have fallen below the policy's retention floor. This removes data by
    ///     design — the partition TRUNCATE is what reclaims the storage in O(1).
    /// </param>
    public static async Task<TablePartitionStatus[]> ApplyAsync(IEnumerable<PolecatDatabase> databases,
        ILogger logger, bool rollForward, bool dropAged, CancellationToken token)
    {
        var statuses = new List<TablePartitionStatus>();

        foreach (var database in databases)
        {
            foreach (var manager in ManagersFor(database))
            {
                var results = (rollForward, dropAged) switch
                {
                    (true, true) => await manager.ApplyAsync(database, logger, token).ConfigureAwait(false),
                    (true, false) => await manager.RollForwardAsync(database, logger, token).ConfigureAwait(false),
                    (false, true) => await manager.DropAgedPartitionsAsync(database, logger, token)
                        .ConfigureAwait(false),
                    _ => []
                };

                statuses.AddRange(results);
            }
        }

        return statuses.ToArray();
    }
}
