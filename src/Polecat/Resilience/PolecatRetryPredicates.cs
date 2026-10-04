using System.Data;
using Microsoft.Data.SqlClient;

namespace Polecat.Resilience;

/// <summary>
///     Which SQL Server failures a retry strategy on <see cref="StoreOptions.ConfigurePolly" /> or
///     <see cref="StoreOptions.ExtendPolly" /> must <b>not</b> replay (#724).
/// </summary>
/// <remarks>
///     <para>
///         <b>A retry on Polecat's pipeline replays computed operations, not the code that computed
///         them.</b> By the time the pipeline sees a failure, application code has already read, made
///         its decisions and handed the session a set of operations. Replaying those re-issues
///         decisions taken against a snapshot the server has since rejected.
///     </para>
///     <para>
///         ⚠️ <b>Polecat adds no retry strategy by default, so nothing replays anything today</b> —
///         see <see cref="PolecatResilienceDefaults.AddPolecatDefaults" />. This type exists because
///         the extension points are public and documented, and the numbers below are not something a
///         caller should have to rediscover. It is a predicate to compose into your own
///         <c>ShouldHandle</c>, deliberately <b>not</b> an override of your policy: Polecat refusing
///         to honour a strategy you explicitly configured would be a worse surprise than the one it
///         prevents.
///     </para>
///     <para>
///         <b>The isolation level is a parameter because it is a per-session decision</b>
///         (<c>SessionOptions.IsolationLevel</c>), not a store-wide one, so this type cannot read it.
///         Pass the level your sessions actually use; the default is the
///         <see cref="IsolationLevel.ReadCommitted" /> that <see cref="DocumentStore" /> uses when
///         nothing else is asked for.
///     </para>
/// </remarks>
public static class PolecatRetryPredicates
{
    /// <summary>
    ///     Snapshot isolation update conflict. <b>Never</b> safe to replay: only the application can
    ///     resolve it, by re-reading and recomputing.
    /// </summary>
    public const int SnapshotUpdateConflict = 3960;

    /// <summary>Deadlock victim.</summary>
    public const int DeadlockVictim = 1205;

    /// <summary>Lock request timeout.</summary>
    public const int LockRequestTimeout = 1222;

    /// <summary>
    ///     True when replaying the unit of work that raised <paramref name="exception" /> could
    ///     commit a lost update, so a retry strategy must not handle it.
    /// </summary>
    /// <remarks>
    ///     Walks the whole exception chain, for the same reason
    ///     <c>EventOperations.FindLockFailure</c> does (#652): the append path throws an
    ///     <see cref="SqlException" /> unwrapped, while a caller-supplied pipeline may wrap it.
    /// </remarks>
    /// <param name="exception">The failure a retry strategy is deciding whether to handle.</param>
    /// <param name="isolationLevel">
    ///     The isolation level the failing session was using. See the remarks on the class.
    /// </param>
    public static bool IsUnsafeToReplay(
        Exception exception, IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        ArgumentNullException.ThrowIfNull(exception);

        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            if (current is SqlException sql && IsUnsafeToReplay(sql.Number, isolationLevel))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    ///     The decision table, by error number. The <see cref="Exception" /> overload is the public
    ///     surface; this is where the reasoning lives, so it can be asserted without fabricating an
    ///     <see cref="SqlException" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>3960</b> is unsafe at every isolation level, including ones that cannot raise it —
    ///         answering "safe" for an error that cannot occur would be true but useless, and a
    ///         caller passing the wrong level would then get the dangerous answer.
    ///     </para>
    ///     <para>
    ///         <b>1205 and 1222 fork on the level.</b> At <see cref="IsolationLevel.ReadCommitted" />
    ///         a replay re-reads current data, so it is the ordinary safe retry. Under
    ///         <c>Snapshot</c> or <c>Serializable</c> the operations being replayed were derived from
    ///         a snapshot the replay will not re-take, which is marten#5528's mechanism reached
    ///         through a different error number. Marten's issue explicitly leaves this case named
    ///         rather than guessed, and naming it is what this fork does.
    ///     </para>
    ///     <para>
    ///         Note 1222 is separately excluded from <c>ConnectionFactory.TransientSqlErrors</c>,
    ///         which governs the connection OPEN and nothing else (#652). The two lists answer
    ///         different questions and are deliberately not shared.
    ///     </para>
    /// </remarks>
    internal static bool IsUnsafeToReplay(int sqlErrorNumber, IsolationLevel isolationLevel)
    {
        if (sqlErrorNumber == SnapshotUpdateConflict) return true;

        if (sqlErrorNumber is DeadlockVictim or LockRequestTimeout)
        {
            return isolationLevel is IsolationLevel.Snapshot or IsolationLevel.Serializable;
        }

        return false;
    }
}
