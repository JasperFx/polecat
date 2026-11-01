using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polly;

namespace Polecat.Internal.Sessions;

/// <summary>
///     Maintains a persistent connection with optional transaction.
///     Used by document sessions for SaveChangesAsync transactional semantics.
/// </summary>
/// <remarks>
///     <para>
///         <b>#832. A session never runs a command outside a transaction it believes it is inside.</b>
///         A <see cref="SqlTransaction" /> that SQL Server rolled back reports a null
///         <see cref="SqlTransaction.Connection" />, and <c>SqlCommand</c> quietly drops a completed
///         transaction rather than refusing it, so every command here checks first. That check sits
///         below the resilience pipeline on purpose: a retry strategy replays the lifetime call, so
///         the replay meets it too, whatever the strategy's <c>ShouldHandle</c> says.
///     </para>
///     <para>
///         <b>#844. The isolation level belongs to the session, not to one transaction.</b> Every
///         transaction this lifetime begins uses it. A session at anything but
///         <see cref="IsolationLevel.ReadCommitted" /> also runs every command inside a transaction,
///         beginning one on demand, because <c>Snapshot</c> and <c>Serializable</c> protect a read
///         followed by a write and a transaction that begins at the save has already missed the read.
///     </para>
/// </remarks>
internal class TransactionalConnection : IAlwaysConnectedLifetime
{
    private const string ResetIsolationLevelSql = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;";

    private readonly ConnectionFactory _connectionFactory;
    private readonly IsolationLevel _isolationLevel;
    private readonly ResiliencePipeline _resilience;
    private SqlConnection? _connection;

    // What ended the current transaction, for the exception that reports it.
    private Exception? _transactionFailure;

    // Outlives the Transaction property, which DocumentSessionBase clears after every save.
    private bool _unusable;

    public TransactionalConnection(ConnectionFactory connectionFactory, int commandTimeout,
        IsolationLevel isolationLevel, ResiliencePipeline resilience)
    {
        _connectionFactory = connectionFactory;
        _isolationLevel = isolationLevel;
        _resilience = resilience;
        CommandTimeout = commandTimeout;
    }

    public int CommandTimeout { get; }
    public ConnectionFactory ConnectionFactory => _connectionFactory;
    public SqlConnection Connection => _connection ?? throw new InvalidOperationException("Connection not yet opened.");
    public SqlTransaction? Transaction { get; set; }
    public bool HoldsTransactionForSession => _isolationLevel != IsolationLevel.ReadCommitted;

    public async ValueTask BeginTransactionAsync(CancellationToken token)
    {
        AssertTransactionIsUsable();
        await EnsureConnectionOpenAsync(token);
        if (Transaction != null) return;

        _transactionFailure = null;
        Transaction = await SqlTransactionRequests.BeginAsync(_connection!, _isolationLevel, token);
    }

    public void AssertTransactionIsUsable()
    {
        if (_unusable || Transaction is { Connection: null })
        {
            throw new SessionTransactionUnusableException(_transactionFailure);
        }
    }

    public void MarkSessionUnusable(Exception cause)
    {
        _unusable = true;
        _transactionFailure ??= cause;
    }

    private async ValueTask EnsureConnectionOpenAsync(CancellationToken token)
    {
        if (_connection == null)
        {
            _connection = _connectionFactory.Create();
            await _connection.OpenAsync(token);
        }
    }

    private async ValueTask PrepareToExecuteAsync(CancellationToken token)
    {
        if (HoldsTransactionForSession)
        {
            await BeginTransactionAsync(token);
            return;
        }

        AssertTransactionIsUsable();
        await EnsureConnectionOpenAsync(token);
    }

    /// <summary>
    ///     Records a failure that took the transaction with it. Most failures do not: a lock timeout
    ///     (1222), a constraint violation or a cancelled statement abort the statement and leave the
    ///     transaction open, which is why this asks the transaction rather than the error number.
    /// </summary>
    private void NoteFailure(SqlException failure)
    {
        if (Transaction is not { Connection: null }) return;

        _transactionFailure = failure;
    }

    public async Task<int> ExecuteAsync(SqlCommand command, CancellationToken token)
    {
        await PrepareToExecuteAsync(token);
        command.Connection = _connection;
        if (Transaction != null) command.Transaction = Transaction;
        command.CommandTimeout = CommandTimeout;

        try
        {
            return await command.ExecuteNonQueryAsync(token);
        }
        catch (SqlException e)
        {
            NoteFailure(e);
            throw;
        }
    }

    public async Task<object?> ExecuteScalarAsync(SqlCommand command, CancellationToken token)
    {
        await PrepareToExecuteAsync(token);
        command.Connection = _connection;
        if (Transaction != null) command.Transaction = Transaction;
        command.CommandTimeout = CommandTimeout;

        try
        {
            return await command.ExecuteScalarAsync(token);
        }
        catch (SqlException e)
        {
            NoteFailure(e);
            throw;
        }
    }

    public async Task<DbDataReader> ExecuteReaderAsync(SqlCommand command, CancellationToken token)
    {
        await PrepareToExecuteAsync(token);
        command.Connection = _connection;
        if (Transaction != null) command.Transaction = Transaction;
        command.CommandTimeout = CommandTimeout;

        try
        {
            return await command.ExecuteReaderAsync(token);
        }
        catch (SqlException e)
        {
            NoteFailure(e);
            throw;
        }
    }

    public async Task<DbDataReader> ExecuteReaderAsync(SqlBatch batch, CancellationToken token)
    {
        await PrepareToExecuteAsync(token);
        batch.Connection = _connection;
        if (Transaction != null) batch.Transaction = Transaction;
        batch.Timeout = CommandTimeout;

        try
        {
            return await batch.ExecuteReaderAsync(token);
        }
        catch (SqlException e)
        {
            NoteFailure(e);
            throw;
        }
    }

    /// <summary>
    ///     Puts a connection back at the default level before the pool gets it (#844).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <c>BeginTransaction(level)</c> changes the connection's level, not just the
    ///         transaction's, and <c>sp_reset_connection</c> does not change it back: a Serializable
    ///         session's connection, reopened from the pool, still reports level 4. Before this, the
    ///         next user of that connection to read outside a transaction (every query session) ran
    ///         at a level nobody asked for.
    ///     </para>
    ///     <para>
    ///         A connection that cannot be reset must not be reused, and disposal must not throw, so a
    ///         failure here clears the pool instead.
    ///     </para>
    /// </remarks>
    private void ResetIsolationLevel()
    {
        if (!HoldsTransactionForSession || _connection is not { State: ConnectionState.Open }) return;

        try
        {
            using var command = new SqlCommand(ResetIsolationLevelSql, _connection);
            command.CommandTimeout = CommandTimeout;
            _resilience.Execute(static c => c.ExecuteNonQuery(), command);
        }
        catch (Exception)
        {
            SqlConnection.ClearPool(_connection);
        }
    }

    /// <inheritdoc cref="ResetIsolationLevel" />
    private async ValueTask ResetIsolationLevelAsync()
    {
        if (!HoldsTransactionForSession || _connection is not { State: ConnectionState.Open }) return;

        try
        {
            await using var command = new SqlCommand(ResetIsolationLevelSql, _connection);
            command.CommandTimeout = CommandTimeout;
            await _resilience.ExecuteAsync(
                static (c, t) => new ValueTask<int>(c.ExecuteNonQueryAsync(t)), command, CancellationToken.None);
        }
        catch (Exception)
        {
            SqlConnection.ClearPool(_connection);
        }
    }

    public void Dispose()
    {
        Transaction?.Dispose();
        ResetIsolationLevel();
        _connection?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (Transaction != null) await Transaction.DisposeAsync();
        await ResetIsolationLevelAsync();
        if (_connection != null) await _connection.DisposeAsync();
    }
}
