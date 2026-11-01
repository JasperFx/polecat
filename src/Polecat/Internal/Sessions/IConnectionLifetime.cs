using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal.Sessions;

/// <summary>
///     Abstracts SQL Server connection lifetime management.
///     All SQL execution flows through this interface.
/// </summary>
internal interface IConnectionLifetime : IAsyncDisposable, IDisposable
{
    int CommandTimeout { get; }

    /// <summary>
    ///     What this lifetime opens connections from, which under database-per-tenant tenancy is the
    ///     only thing that knows the tenant's database (#846).
    /// </summary>
    ConnectionFactory ConnectionFactory { get; }

    /// <summary>
    ///     Whether a transaction is open on this lifetime right now, which is to say whether it may be
    ///     holding locks that something else could be waiting on (#868).
    /// </summary>
    bool HasOpenTransaction { get; }

    Task<int> ExecuteAsync(SqlCommand command, CancellationToken token);
    Task<object?> ExecuteScalarAsync(SqlCommand command, CancellationToken token);
    Task<DbDataReader> ExecuteReaderAsync(SqlCommand command, CancellationToken token);
    Task<DbDataReader> ExecuteReaderAsync(SqlBatch batch, CancellationToken token);
}

/// <summary>
///     A connection lifetime that maintains a persistent connection with optional transaction.
///     Used by document sessions that need SaveChangesAsync transactional semantics.
/// </summary>
internal interface IAlwaysConnectedLifetime : IConnectionLifetime
{
    SqlConnection Connection { get; }
    SqlTransaction? Transaction { get; set; }

    /// <summary>
    ///     True for a session at anything but <c>ReadCommitted</c>: every command runs inside a
    ///     transaction, at that level, whoever began it (#844).
    /// </summary>
    bool HoldsTransactionForSession { get; }

    /// <summary>
    ///     Begin a transaction at the session's isolation level, unless one is already open.
    /// </summary>
    ValueTask BeginTransactionAsync(CancellationToken token);

    /// <summary>
    ///     Throw <see cref="Exceptions.SessionTransactionUnusableException" /> if the transaction this
    ///     session is inside has been ended by the server, or the session has been marked unusable (#832).
    /// </summary>
    void AssertTransactionIsUsable();

    /// <summary>
    ///     Refuse all further work on this session, because the transaction it was relying on is gone.
    /// </summary>
    void MarkSessionUnusable(Exception cause);
}
