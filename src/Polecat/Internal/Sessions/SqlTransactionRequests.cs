using System.Collections.Concurrent;
using System.Data;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal.Sessions;

/// <summary>
///     Begins, commits and rolls back a <see cref="SqlTransaction" /> without parking a thread-pool
///     thread on a MARS connection.
/// </summary>
/// <remarks>
///     <para>
///         <b>Microsoft.Data.SqlClient has no asynchronous transaction requests.</b> It overrides none
///         of <c>DbConnection.BeginDbTransactionAsync</c>, <c>DbTransaction.CommitAsync</c> or
///         <c>DbTransaction.RollbackAsync</c> (checked against 7.0.0), so every one of them runs the
///         synchronous round trip inline on the awaiting thread — a thread-pool thread, in practice.
///     </para>
///     <para>
///         <b>With <c>MultipleActiveResultSets=True</c> that synchronous wait depends on the thread
///         pool itself.</b> The managed network layer (every non-Windows platform) demultiplexes a MARS
///         connection's reads through asynchronous socket callbacks, which are queued to the same pool,
///         and the blocked caller waits for one of them. N saves beginning their transactions at once on
///         a small pool block N threads whose replies are queued behind more of the same work, and the
///         pool grows by roughly one thread per half second. Each request waits out the connection's
///         <i>connect</i> timeout, not its command timeout, and then fails with
///         <c>SqlException</c> -2, "Execution Timeout Expired" — a failure that names neither the
///         starvation nor the transaction.
///     </para>
///     <para>
///         That is what failed <c>racing_writers_never_report_success_for_a_write_that_did_not_land</c>
///         on a 4-vCPU CI runner: one of eight racing sessions surfaced -2 from
///         <c>BeginTransaction</c> 5 s in (the suite's <c>Timeout=5</c>) instead of the
///         <c>ConcurrencyException</c> its write would have earned. Reproduced locally with
///         <c>DOTNET_PROCESSOR_COUNT=2</c>; with MARS off the same run never stalls.
///     </para>
///     <para>
///         The request therefore runs on a dedicated thread when the connection uses MARS, which costs a
///         thread start per request and only in that configuration. Without MARS the synchronous wait is
///         an ordinary blocking socket read with no dependency on the pool, so the call stays inline,
///         exactly as before.
///     </para>
/// </remarks>
internal static class SqlTransactionRequests
{
    private static readonly ConcurrentDictionary<string, bool> _usesMars = new();

    public static Task<SqlTransaction> BeginAsync(SqlConnection connection, CancellationToken token)
        => BeginAsync(connection, IsolationLevel.Unspecified, token);

    public static async Task<SqlTransaction> BeginAsync(SqlConnection connection, IsolationLevel isolationLevel,
        CancellationToken token)
    {
        if (!UsesMars(connection))
        {
            return (SqlTransaction)await connection.BeginTransactionAsync(isolationLevel, token);
        }

        token.ThrowIfCancellationRequested();
        return await OnDedicatedThread(() => connection.BeginTransaction(isolationLevel));
    }

    public static Task CommitAsync(SqlTransaction transaction, CancellationToken token)
    {
        var connection = transaction.Connection;
        if (connection is null || !UsesMars(connection))
        {
            return transaction.CommitAsync(token);
        }

        if (token.IsCancellationRequested) return Task.FromCanceled(token);
        return OnDedicatedThread(transaction.Commit);
    }

    /// <summary>
    ///     Deliberately takes no token: a rollback that is skipped because the caller's token was
    ///     cancelled leaves the transaction, and its locks, open (#783).
    /// </summary>
    public static Task RollbackAsync(SqlTransaction transaction)
    {
        var connection = transaction.Connection;
        if (connection is null || !UsesMars(connection))
        {
            return transaction.RollbackAsync(CancellationToken.None);
        }

        return OnDedicatedThread(transaction.Rollback);
    }

    private static bool UsesMars(SqlConnection connection)
        => _usesMars.GetOrAdd(connection.ConnectionString,
            static s => new SqlConnectionStringBuilder(s).MultipleActiveResultSets);

    private static Task OnDedicatedThread(Action request)
        => Task.Factory.StartNew(request, CancellationToken.None,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);

    private static Task<T> OnDedicatedThread<T>(Func<T> request)
        => Task.Factory.StartNew(request, CancellationToken.None,
            TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
}
