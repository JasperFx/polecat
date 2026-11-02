namespace Polecat.Exceptions;

/// <summary>
///     Thrown when a session can no longer be used because the transaction it was relying on has ended
///     without committing. Nothing is written after this point. Dispose the session, open a new one, and
///     redo the work from its inputs.
/// </summary>
/// <remarks>
///     <para>
///         #832. A deadlock (1205), a snapshot update conflict (3960) and a few rarer errors roll the
///         transaction back on the server while the connection stays open. SqlClient then marks the
///         <see cref="Microsoft.Data.SqlClient.SqlTransaction" /> completed and a <c>SqlCommand</c>
///         holding it silently drops it, so everything sent afterwards runs in autocommit. Each
///         statement is durable as it finishes, and only the commit fails: the caller is told the save
///         failed, and it landed anyway, without any of the locks the transaction was holding.
///     </para>
///     <para>
///         A session that took <c>FetchForExclusiveWriting</c>'s or <c>AppendExclusive</c>'s lock
///         decided what to write while it held that lock, and a <c>Snapshot</c> or
///         <c>Serializable</c> session decided it against that transaction's view of the data. Both
///         are gone with the transaction, and a new one cannot put them back, so such a session
///         refuses everything from then on, reads included. A session that held nothing (a
///         <c>ReadCommitted</c> session whose transaction belonged to the save that failed) keeps
///         working: that save failed atomically and the next one begins a clean transaction.
///     </para>
///     <para>
///         #854. A save that fails on the client, with a <c>ConcurrencyException</c> or a cancelled
///         token, rolls that same transaction back itself, and the lock or the view of the data goes
///         the same way. Such a session refuses to write from then on. A <c>ReadCommitted</c> session
///         goes on reading, because its reads run outside any transaction and report what is there; a
///         session at any other level refuses reads as well, because the next one would begin a new
///         transaction and with it a different view of the data.
///     </para>
///     <para>
///         The same refusal is what a retry strategy on the resilience pipeline meets when it replays
///         a command whose transaction has gone, so a replay cannot run outside that transaction. See
///         <see cref="Resilience.PolecatRetryPredicates" />.
///     </para>
///     <para>
///         Recovery belongs above the session, as Marten's exception of the same name says. A
///         Wolverine message retry does exactly that.
///     </para>
/// </remarks>
public class SessionTransactionUnusableException : Exception
{
    internal SessionTransactionUnusableException(Exception? cause) : base(
        "This session cannot be used any further: SQL Server ended its transaction, so the locks and "
        + "the view of the data it held are gone, and running more commands would run them outside it. "
        + "Nothing further has been written. Dispose this session, open a new one and redo the work."
        + (cause is null ? "" : " See the inner exception for the original failure."),
        cause)
    {
    }

    private SessionTransactionUnusableException(string message, Exception? cause) : base(message, cause)
    {
    }

    /// <summary>
    ///     The refusal for a transaction a failed save rolled back, rather than one the server ended.
    /// </summary>
    internal static SessionTransactionUnusableException AfterRollback(Exception? cause) => new(
        "This session cannot write any further: a failed save rolled back the transaction it was relying "
        + "on, so the exclusive lock or the view of the data that transaction held is gone, and anything "
        + "written now would be written without it. Nothing further has been written. Dispose this "
        + "session, open a new one and redo the work."
        + (cause is null ? "" : " See the inner exception for the original failure."),
        cause);
}
