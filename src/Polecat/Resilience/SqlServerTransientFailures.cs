using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;

namespace Polecat.Resilience;

/// <summary>
///     Tells JasperFx which <see cref="SqlException" />s are transient, for
///     <see cref="ShardFailure.IsTransient" /> (jasperfx#1016, wolverine#4986). A supervisor uses that
///     label to restart a paused projection or subscription shard after a pool timeout, a deadlock or
///     a failover, while leaving a permanent failure (a missing column, a bug) paused for an operator
///     instead of restarting it in a loop.
/// </summary>
/// <remarks>
///     <para>
///         <b>Why Polecat needs this at all:</b> JasperFx's built-in rule for a database fault is
///         <see cref="System.Data.Common.DbException.IsTransient" />, and Microsoft.Data.SqlClient
///         (probed on 7.0.0) does not override it, so every <see cref="SqlException" /> reports false.
///         Without a classifier a command timeout (-2) or a deadlock victim (1205) would be labelled
///         permanent. A refused connection already reads as transient without help, through the
///         <see cref="System.Net.Sockets.SocketException" /> SqlClient puts inside it.
///     </para>
///     <para>
///         The verdict is a label only. It does not change the daemon's own retries, nor
///         <see cref="Internal.ConnectionFactory" />'s connection-open retry list, nor anything
///         <see cref="PolecatRetryPredicates" /> says about replaying a unit of work: those answer
///         different questions. Restarting a shard re-reads from its persisted progress, so a
///         deadlock or a lock timeout IS worth a restart even though Polecat never replays one.
///     </para>
///     <para>
///         <b>Walk semantics.</b> <see cref="TransientFailures" /> walks the whole exception graph and
///         asks this classifier about each exception in it. Answering false for a SqlException only
///         settles that one exception — the walk still goes on into its inner exceptions — so a
///         non-transient SqlException wrapping a transient fault cannot hide it.
///     </para>
/// </remarks>
internal static class SqlServerTransientFailures
{
    /// <summary>
    ///     The SQL Server error numbers that mark a fault expected to clear on its own.
    /// </summary>
    /// <remarks>
    ///     Three Microsoft sources, rather than an invented list:
    ///     <list type="number">
    ///         <item>
    ///             SqlClient's own configurable retry logic default list,
    ///             <see cref="SqlConfigurableRetryFactory.BaselineTransientErrors" />, taken from the
    ///             referenced driver so a SqlClient bump carries its changes in. On 7.0.0 that is 233,
    ///             997, 1204, 1205, 1222, 4060, 4221, 10060, 10928, 10929, 40143, 40197, 40501, 40540,
    ///             40613, 42108, 42109, 49918, 49919, 49920. See
    ///             https://learn.microsoft.com/sql/connect/ado-net/internal-retry-logic-providers-sqlclient#built-in-transient-error-list
    ///         </item>
    ///         <item>
    ///             The Azure SQL "List of transient fault error codes",
    ///             https://learn.microsoft.com/azure/azure-sql/database/troubleshoot-common-errors-issues .
    ///             Everything there is already in the baseline except 926 (database marked SUSPECT) and
    ///             615 (could not find database id), which are transient only on Azure's own replicated
    ///             master; on a SQL Server 2025 box both mean an operator is needed, so they are left out.
    ///         </item>
    ///         <item>
    ///             The client-side and connection-level numbers below, which neither list carries
    ///             because SqlClient raises them itself rather than the server. They are the ones EF
    ///             Core's <c>SqlServerTransientExceptionDetector</c> and Polecat's own
    ///             <see cref="Internal.ConnectionFactory.TransientSqlErrors" /> treat as transient.
    ///         </item>
    ///     </list>
    ///     EF Core leaves -2 out on purpose, because a timed-out command may in fact have committed and
    ///     a blind retry could apply it twice. That does not apply here: restarting a shard re-reads
    ///     from its persisted progress rather than replaying the command.
    /// </remarks>
    internal static readonly IReadOnlySet<int> TransientErrorNumbers = new HashSet<int>(
    [
        -2,     // client-side command / connection timeout
        20,     // the instance does not support encryption (raised on a dropped pre-login handshake)
        64,     // the specified network name is no longer available (connection dropped during login)
        121,    // the semaphore timeout period has expired
        10053,  // transport-level error: connection aborted by the host
        10054,  // transport-level error: connection reset by the remote host
        11001,  // host not found (name resolution failed)
        ..SqlConfigurableRetryFactory.BaselineTransientErrors
    ]);

    private static readonly Func<Exception, bool?> _classifier = Classify;

    static SqlServerTransientFailures()
    {
        // TransientFailures.AddClassifier ignores the same delegate instance twice, and the static
        // constructor runs once per process anyway, so every DocumentStore can call EnsureRegistered.
        TransientFailures.AddClassifier(_classifier);
    }

    /// <summary>
    ///     Make sure the classifier is registered with <see cref="TransientFailures" />. Cheap and
    ///     idempotent; the work happens once, in the static constructor.
    /// </summary>
    internal static void EnsureRegistered()
    {
        // Intentionally empty: touching the type runs the static constructor
    }

    /// <summary>
    ///     True for a <see cref="SqlException" /> carrying any transient error number, false for any
    ///     other <see cref="SqlException" /> so a permanent error is definitively not transient, and
    ///     null for anything that is not a <see cref="SqlException" />, which leaves it to JasperFx's
    ///     built-in rules.
    /// </summary>
    internal static bool? Classify(Exception exception)
    {
        if (exception is not SqlException sql) return null;

        // A batch can report several errors, and the first (sql.Number) is not necessarily the one
        // that matters; one transient error is enough.
        foreach (SqlError error in sql.Errors)
        {
            if (TransientErrorNumbers.Contains(error.Number)) return true;
        }

        return TransientErrorNumbers.Contains(sql.Number);
    }
}
