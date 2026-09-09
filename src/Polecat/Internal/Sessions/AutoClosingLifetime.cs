using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal.Sessions;

/// <summary>
///     Opens a fresh connection per call and closes it when done.
///     For readers, uses CommandBehavior.CloseConnection so the connection
///     is closed when the reader is disposed.
///     Default lifetime for query sessions.
/// </summary>
internal class AutoClosingLifetime : IConnectionLifetime
{
    private readonly ConnectionFactory _connectionFactory;

    public AutoClosingLifetime(ConnectionFactory connectionFactory, int commandTimeout)
    {
        _connectionFactory = connectionFactory;
        CommandTimeout = commandTimeout;
    }

    public int CommandTimeout { get; }

    public async Task<int> ExecuteAsync(SqlCommand command, CancellationToken token)
    {
        await using var conn = _connectionFactory.Create();
        await conn.OpenAsync(token);
        command.Connection = conn;
        command.CommandTimeout = CommandTimeout;
        return await command.ExecuteNonQueryAsync(token);
    }

    public async Task<object?> ExecuteScalarAsync(SqlCommand command, CancellationToken token)
    {
        await using var conn = _connectionFactory.Create();
        await conn.OpenAsync(token);
        command.Connection = conn;
        command.CommandTimeout = CommandTimeout;
        return await command.ExecuteScalarAsync(token);
    }

    public async Task<DbDataReader> ExecuteReaderAsync(SqlCommand command, CancellationToken token)
    {
        var conn = _connectionFactory.Create();
        try
        {
            await conn.OpenAsync(token);
            command.Connection = conn;
            command.CommandTimeout = CommandTimeout;
            return await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, token);
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    ///     The batch counterpart of <see cref="ExecuteReaderAsync(SqlCommand, CancellationToken)" />,
    ///     and the one place the two genuinely differ: Microsoft.Data.SqlClient honours
    ///     <see cref="CommandBehavior.CloseConnection" /> for a <see cref="SqlCommand" /> reader and
    ///     silently ignores it for a <see cref="SqlBatch" /> one.
    /// </summary>
    /// <remarks>
    ///     This method used to pass the flag and rely on it, exactly as the command overload does, so
    ///     every batch-executed read leaked its pooled connection — the reader was disposed, the
    ///     connection was not, and it never went back to the pool. Nearly every read on a query session
    ///     runs through a batch (document and event LINQ, batched queries, session loads), so a process
    ///     hit the pool ceiling in proportion to how many queries it had issued, and failed on whichever
    ///     query crossed it with "Timeout expired. The timeout period elapsed prior to obtaining a
    ///     connection from the pool" — an error naming neither the leak nor the query that caused it.
    ///     <see cref="ConnectionClosingDataReader" /> supplies the missing behaviour explicitly.
    /// </remarks>
    public async Task<DbDataReader> ExecuteReaderAsync(SqlBatch batch, CancellationToken token)
    {
        var conn = _connectionFactory.Create();
        try
        {
            await conn.OpenAsync(token);
            batch.Connection = conn;
            batch.Timeout = CommandTimeout;
            var reader = await batch.ExecuteReaderAsync(token);
            return new ConnectionClosingDataReader(reader, conn);
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    public void Dispose()
    {
        // No persistent connection to dispose
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
