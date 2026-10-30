using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal.Sessions;

/// <summary>
///     Maintains a persistent connection with optional transaction.
///     Used by document sessions for SaveChangesAsync transactional semantics.
/// </summary>
internal class TransactionalConnection : IAlwaysConnectedLifetime
{
    private readonly ConnectionFactory _connectionFactory;
    private SqlConnection? _connection;

    public TransactionalConnection(ConnectionFactory connectionFactory, int commandTimeout)
    {
        _connectionFactory = connectionFactory;
        CommandTimeout = commandTimeout;
    }

    public int CommandTimeout { get; }
    public SqlConnection Connection => _connection ?? throw new InvalidOperationException("Connection not yet opened.");
    public SqlTransaction? Transaction { get; set; }

    public async ValueTask BeginTransactionAsync(CancellationToken token)
    {
        await EnsureConnectionOpenAsync(token);
        Transaction ??= await SqlTransactionRequests.BeginAsync(_connection!, token);
    }

    public async ValueTask BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken token)
    {
        await EnsureConnectionOpenAsync(token);
        Transaction ??= await SqlTransactionRequests.BeginAsync(_connection!, isolationLevel, token);
    }

    private async ValueTask EnsureConnectionOpenAsync(CancellationToken token)
    {
        if (_connection == null)
        {
            _connection = _connectionFactory.Create();
            await _connection.OpenAsync(token);
        }
    }

    public async Task<int> ExecuteAsync(SqlCommand command, CancellationToken token)
    {
        await EnsureConnectionOpenAsync(token);
        command.Connection = _connection;
        if (Transaction != null) command.Transaction = Transaction;
        command.CommandTimeout = CommandTimeout;
        return await command.ExecuteNonQueryAsync(token);
    }

    public async Task<object?> ExecuteScalarAsync(SqlCommand command, CancellationToken token)
    {
        await EnsureConnectionOpenAsync(token);
        command.Connection = _connection;
        if (Transaction != null) command.Transaction = Transaction;
        command.CommandTimeout = CommandTimeout;
        return await command.ExecuteScalarAsync(token);
    }

    public async Task<DbDataReader> ExecuteReaderAsync(SqlCommand command, CancellationToken token)
    {
        await EnsureConnectionOpenAsync(token);
        command.Connection = _connection;
        if (Transaction != null) command.Transaction = Transaction;
        command.CommandTimeout = CommandTimeout;
        return await command.ExecuteReaderAsync(token);
    }

    public async Task<DbDataReader> ExecuteReaderAsync(SqlBatch batch, CancellationToken token)
    {
        await EnsureConnectionOpenAsync(token);
        batch.Connection = _connection;
        if (Transaction != null) batch.Transaction = Transaction;
        batch.Timeout = CommandTimeout;
        return await batch.ExecuteReaderAsync(token);
    }

    public void Dispose()
    {
        Transaction?.Dispose();
        _connection?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (Transaction != null) await Transaction.DisposeAsync();
        if (_connection != null) await _connection.DisposeAsync();
    }
}
