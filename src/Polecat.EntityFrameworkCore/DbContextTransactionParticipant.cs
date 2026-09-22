using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Transaction participant that swaps a DbContext's placeholder connection
///     to Polecat's real connection/transaction before commit, then calls
///     DbContext.SaveChangesAsync() within the same atomic transaction.
/// </summary>
/// <remarks>
///     <para>
///     #650 (marten#5457 / marten#5228): this is also the owner of the
///     <typeparamref name="TDbContext" />'s lifetime. The context is built once per tenant per
///     batch by <c>RegisterEfCoreStorage</c>'s storage factory, and
///     <c>IProjectionStorage&lt;,&gt;</c> declares no disposal contract at all — so the storage
///     that holds the context has nothing to hook. This participant is the one object in the graph
///     whose lifetime already matches the context's: it is registered on the session that owns the
///     batch, and <c>DocumentSessionBase.DisposeAsync</c> drains its participants exactly once, on
///     the success path and the failure path alike.
///     </para>
///     <para>
///     The placeholder connection is released eagerly at the end of
///     <see cref="BeforeCommitAsync" /> — it is provably finished with there, having just been
///     swapped out — but the context is NOT. It is still reachable by an inline projection whose
///     session has not ended, so tearing it down there would be a behaviour change rather than a
///     leak fix. Release is idempotent so the success path's eager call and disposal do not
///     double-dispose.
///     </para>
///     <para>
///     <see cref="IAsyncDisposable" /> only, where Marten's twin also implements
///     <see cref="IDisposable" />: Polecat's <c>IQuerySession</c> is async-disposable and has no
///     synchronous disposal path, so a sync overload here would be unreachable.
///     </para>
/// </remarks>
internal class DbContextTransactionParticipant<TDbContext> : ITransactionParticipant, IAsyncDisposable
    where TDbContext : DbContext
{
    private readonly SqlConnection _placeholderConnection;
    private bool _released;
    private bool _disposed;

    public DbContextTransactionParticipant(TDbContext dbContext, SqlConnection placeholderConnection)
    {
        DbContext = dbContext;
        _placeholderConnection = placeholderConnection;
    }

    /// <summary>
    ///     The context this participant flushes and owns.
    /// </summary>
    public TDbContext DbContext { get; }

    public async Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        // Swap DbContext to Polecat's real connection and transaction
        DbContext.Database.SetDbConnection(connection);
        await DbContext.Database.UseTransactionAsync(transaction, token);

        // Flush EF Core changes within Polecat's transaction
        await DbContext.SaveChangesAsync(token);

        // Release the placeholder connection (never actually opened for SQL Server) now that it has
        // been swapped out. Idempotent, because disposal releases it too — this line used to be the
        // ONLY release, so a projection that threw while applying, an optimistic-concurrency failure
        // in SaveChangesAsync, or a throw from anywhere above skipped it entirely.
        await ReleaseAsync();
    }

    private async ValueTask ReleaseAsync()
    {
        if (_released) return;

        _released = true;
        await _placeholderConnection.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        _disposed = true;

        await ReleaseAsync();
        await DbContext.DisposeAsync();
    }
}
