using System.Globalization;
using System.Resources;
using JasperFx.Events.Daemon;
using Microsoft.Data.SqlClient;

namespace Polecat.Resilience;

/// <summary>
///     Tells JasperFx which SQL Server failures are transient, for
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
///         <b>A connection pool timeout is not a <see cref="SqlException" /> (#845).</b> SqlClient raises
///         it as a bare <see cref="InvalidOperationException" /> with nothing inside it: no
///         <see cref="TimeoutException" />, no <see cref="System.Data.Common.DbException" />. That shape
///         defeats all of JasperFx's built-in rules, so a projection that lost the race for a connection
///         had its failure wrapped as <c>ApplyEventException</c> and, with the default
///         <c>SkipApplyErrors</c>, the event dead-lettered and skipped. Calling it transient here is
///         what stops that, because <c>ProjectionExceptions.IsExceptionTransient</c> reads this same
///         answer before it wraps anything: the failure goes up unwrapped, the shard pauses with
///         <see cref="ShardFailure.IsTransient" /> true, and a restart loses nothing.
///         <c>ProjectionExceptions.IsSystemicDatabaseFailure</c> needs no separate treatment: that
///         check lives in JasperFx, and a failure <see cref="TransientFailures" /> calls transient is
///         never systemic.
///     </para>
///     <para>
///         <b>Why that means reading the message.</b> SqlClient gives this failure no type, error
///         number or data of its own, and <see cref="Exception.HResult" /> is the one every
///         <see cref="InvalidOperationException" /> has. The text is not a constant either: it comes
///         from one of 13 satellite assemblies, so an English comparison would quietly stop working
///         on a German host. Nor can <see cref="CultureInfo.CurrentUICulture" /> say which language to
///         expect. An async open is failed by SqlClient's background waiter, which raises the text in
///         the culture of whichever open started it, so a caller in French can be handed German. So
///         the match is on the exact type and on the whole message, against every text the loaded
///         SqlClient can produce, read from SqlClient's own resources. In 7.0.0 those strings have one
///         source, <c>ADP.PooledOpenTimeout()</c>, and nothing in Polecat raises them. If a SqlClient
///         bump renames the resource, a pool timeout on a localized host goes back to reading as
///         permanent, which <c>every_language_sqlclient_ships_contributes_its_message</c> is there to
///         catch.
///     </para>
///     <para>
///         Beyond that, the verdict is a label. It does not change the daemon's own retries, nor
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

    /// <summary>
    ///     The languages Microsoft.Data.SqlClient ships satellite resources for. A language that is not
    ///     here cannot produce a pool timeout message we would fail to recognize unless SqlClient starts
    ///     shipping it, which <c>the_language_list_covers_every_satellite_actually_deployed</c> catches.
    /// </summary>
    internal static readonly string[] SqlClientResourceCultures =
        ["cs", "de", "es", "fr", "it", "ja", "ko", "pl", "pt-BR", "ru", "tr", "zh-Hans", "zh-Hant"];

    // The neutral resource, so the failure is still recognized if the lookup below comes back empty
    private const string PoolTimeoutMessage =
        "Timeout expired.  The timeout period elapsed prior to obtaining a connection from the pool.  "
        + "This may have occurred because all pooled connections were in use and max pool size was reached.";

    private static readonly Lazy<IReadOnlySet<string>> _poolTimeoutMessages = new(LoadPoolTimeoutMessages);

    /// <summary>
    ///     Every text SqlClient's <c>ADP.PooledOpenTimeout()</c> can raise, one per language it ships.
    /// </summary>
    internal static IReadOnlySet<string> PoolTimeoutMessages => _poolTimeoutMessages.Value;

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
    ///     True for a <see cref="SqlException" /> carrying any transient error number and for a
    ///     connection pool timeout, false for any other <see cref="SqlException" /> so a permanent error
    ///     is definitively not transient, and null for anything else, which leaves it to JasperFx's
    ///     built-in rules.
    /// </summary>
    internal static bool? Classify(Exception exception)
    {
        if (IsConnectionPoolTimeout(exception)) return true;

        if (exception is not SqlException sql) return null;

        // A batch can report several errors, and the first (sql.Number) is not necessarily the one
        // that matters; one transient error is enough.
        foreach (SqlError error in sql.Errors)
        {
            if (TransientErrorNumbers.Contains(error.Number)) return true;
        }

        return TransientErrorNumbers.Contains(sql.Number);
    }

    /// <summary>
    ///     Whether <paramref name="exception" /> is SqlClient reporting that it could not get a
    ///     connection from the pool in time. See the remarks on the class for why this reads the message.
    /// </summary>
    internal static bool IsConnectionPoolTimeout(Exception exception)
    {
        // Exactly this type: ObjectDisposedException derives from InvalidOperationException, and nothing
        // that adds meaning to the type is the failure we are looking for.
        return exception.GetType() == typeof(InvalidOperationException)
               && PoolTimeoutMessages.Contains(exception.Message);
    }

    private static IReadOnlySet<string> LoadPoolTimeoutMessages()
    {
        var messages = new HashSet<string>(StringComparer.Ordinal) { PoolTimeoutMessage };

        var resources = new ResourceManager("Microsoft.Data.SqlClient.Resources.Strings",
            typeof(SqlConnection).Assembly);

        foreach (var name in SqlClientResourceCultures.Prepend(""))
        {
            try
            {
                // Falls back to the neutral text where a satellite is not deployed, which is also what
                // SqlClient itself raises there, so a trimmed set of satellites stays consistent.
                var message = resources.GetString("ADP_PooledOpenTimeout", CultureInfo.GetCultureInfo(name));
                if (!string.IsNullOrEmpty(message)) messages.Add(message);
            }
            catch (Exception e) when (e is CultureNotFoundException or MissingManifestResourceException
                                          or MissingSatelliteAssemblyException)
            {
                // A classifier must not throw: it runs while a failure is already being reported. A
                // culture that cannot be built (invariant globalization) cannot be raised in either.
            }
        }

        return messages;
    }
}
