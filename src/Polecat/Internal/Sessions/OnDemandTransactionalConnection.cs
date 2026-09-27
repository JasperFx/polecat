using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal.Sessions;

/// <summary>
///     A writable session's connection lifetime that pins a pooled connection only while a transaction
///     is actually open (#683). Until then every command opens a connection, runs, and returns it to the
///     pool — <see cref="AutoClosingLifetime" />'s behaviour — and from
///     <see cref="BeginTransactionAsync(CancellationToken)" /> onward it is
///     <see cref="TransactionalConnection" /> exactly.
/// </summary>
/// <remarks>
///     <para>
///         Why this exists rather than <see cref="TransactionalConnection" />: the async daemon's
///         projection batch creates one <em>writable</em> session per tenant it projects under and keeps
///         every one of them alive until the batch commits. Those sessions never commit — the batch
///         drains their work trackers and executes the operations itself, on one connection and one
///         transaction of its own — so the only thing their own connection ever does is read the
///         existing projected document. A <see cref="TransactionalConnection" /> opens on that first
///         read and then holds the connection for the session's whole life, which means a conjoined
///         store whose batch spans more tenants than the pool has connections cannot finish the batch:
///         every tenant session holds one and the batch's own commit queues behind them. The default
///         pool is 100, so it began at 101 co-located tenants in a single batch.
///     </para>
///     <para>
///         The batch cannot simply use <see cref="AutoClosingLifetime" /> because a writable session
///         requires <see cref="IAlwaysConnectedLifetime" /> — and that requirement is real, not
///         incidental: a session that <em>does</em> commit needs one connection for the transaction its
///         operations share. Hence the switch rather than a second session type. A caller that begins a
///         transaction gets the old semantics in full; a caller that only reads costs the pool nothing
///         between commands.
///     </para>
///     <para>
///         ⚠️ Before a transaction is begun, two reads through this lifetime run on <em>different</em>
///         connections, so they are not isolated from each other. That is the same guarantee a query
///         session gives and it is why this is not the default for
///         <c>IDocumentStore.LightweightSession()</c>, whose callers do commit.
///     </para>
/// </remarks>
internal sealed class OnDemandTransactionalConnection : IAlwaysConnectedLifetime
{
    private readonly TransactionalConnection _pinned;
    private readonly AutoClosingLifetime _pooled;

    public OnDemandTransactionalConnection(ConnectionFactory connectionFactory, int commandTimeout)
    {
        _pinned = new TransactionalConnection(connectionFactory, commandTimeout);
        _pooled = new AutoClosingLifetime(connectionFactory, commandTimeout);
        CommandTimeout = commandTimeout;
    }

    public int CommandTimeout { get; }

    /// <summary>
    ///     True once a transaction has been begun (or assigned), after which every command runs on the
    ///     pinned connection so that it enlists in that transaction.
    /// </summary>
    private bool _isPinned;

    public SqlConnection Connection => _pinned.Connection;

    /// <remarks>
    ///     Assigning a non-null transaction pins this lifetime for the same reason beginning one does:
    ///     a command that does not run on the transaction's own connection cannot enlist in it.
    ///     <c>DocumentSessionBase</c> clears this back to null after a commit, which deliberately does
    ///     <b>not</b> unpin — the connection is already open and the session is about to be disposed.
    /// </remarks>
    public SqlTransaction? Transaction
    {
        get => _pinned.Transaction;
        set
        {
            if (value != null) _isPinned = true;
            _pinned.Transaction = value;
        }
    }

    public async ValueTask BeginTransactionAsync(CancellationToken token)
    {
        _isPinned = true;
        await _pinned.BeginTransactionAsync(token);
    }

    public async ValueTask BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken token)
    {
        _isPinned = true;
        await _pinned.BeginTransactionAsync(isolationLevel, token);
    }

    public Task<int> ExecuteAsync(SqlCommand command, CancellationToken token)
        => _isPinned ? _pinned.ExecuteAsync(command, token) : _pooled.ExecuteAsync(command, token);

    public Task<object?> ExecuteScalarAsync(SqlCommand command, CancellationToken token)
        => _isPinned ? _pinned.ExecuteScalarAsync(command, token) : _pooled.ExecuteScalarAsync(command, token);

    public Task<DbDataReader> ExecuteReaderAsync(SqlCommand command, CancellationToken token)
        => _isPinned ? _pinned.ExecuteReaderAsync(command, token) : _pooled.ExecuteReaderAsync(command, token);

    public Task<DbDataReader> ExecuteReaderAsync(SqlBatch batch, CancellationToken token)
        => _isPinned ? _pinned.ExecuteReaderAsync(batch, token) : _pooled.ExecuteReaderAsync(batch, token);

    public void Dispose()
    {
        _pinned.Dispose();
        _pooled.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await _pinned.DisposeAsync();
        await _pooled.DisposeAsync();
    }
}
