using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Polecat.Internal;

namespace Polecat.EntityFrameworkCore;

/// <summary>
///     Transaction participant that enlists a DbContext in the transaction Polecat is about to
///     commit, then calls DbContext.SaveChangesAsync() within it.
/// </summary>
/// <remarks>
///     <para>
///     #650 (marten#5457 / marten#5228): this is also the owner of the
///     <typeparamref name="TDbContext" />'s lifetime. The context is built once per tenant per
///     batch by <c>RegisterEfCoreStorage</c>'s storage factory, and
///     <c>IProjectionStorage&lt;,&gt;</c> declares no disposal contract at all — so the storage
///     that holds the context has nothing to hook. This participant is the one object in the graph
///     whose lifetime already matches the context's: it belongs to whatever commits the transaction,
///     and that owner drains its participants exactly once, on the success path and the failure path
///     alike. Inline, the owner is the session and the drain is <c>DocumentSessionBase.DisposeAsync</c>.
///     In the async daemon it is <c>PolecatProjectionBatch</c>, because the daemon can dispose a tenant
///     session well before the batch commits (#867).
///     </para>
///     <para>
///     A placeholder connection, when there is one, is released eagerly at the end of
///     <see cref="BeforeCommitAsync" /> — it is provably finished with there, having just been
///     swapped out — but the context is NOT. It is still reachable by an inline projection whose
///     session has not ended, so tearing it down there would be a behaviour change rather than a
///     leak fix. Release is idempotent so the success path's eager call and disposal do not
///     double-dispose. A context attached to the session's transaction has no placeholder, because
///     the connection it uses belongs to the session (#846); see <see cref="EfCoreDbContextFactory" />.
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
    private readonly SqlConnection? _placeholderConnection;
    private readonly DocumentSessionBase? _session;
    private bool _released;
    private bool _disposed;

    /// <param name="dbContext">The context to flush, and to own.</param>
    /// <param name="placeholderConnection">The connection to release, if the context has its own.</param>
    /// <param name="session">The session the context was built for.</param>
    public DbContextTransactionParticipant(TDbContext dbContext, SqlConnection? placeholderConnection,
        DocumentSessionBase? session = null)
    {
        DbContext = dbContext;
        _placeholderConnection = placeholderConnection;
        _session = session;
    }

    /// <summary>
    ///     The context this participant flushes and owns.
    /// </summary>
    public TDbContext DbContext { get; }

    public async Task BeforeCommitAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken token)
    {
        // Point the context at this transaction, whatever it was pointing at before: the placeholder
        // on the first attempt of a daemon batch, the previous attempt's dead connection on a retry
        // of one, or on a session that saves twice, the transaction its first save already committed.
        // A context already attached to this very transaction goes round the same way, and EF takes
        // all of those without complaint. The one thing it refuses is a transaction whose connection
        // has gone, so a dead one fails here, loudly, rather than flushing in autocommit.
        DbContext.Database.SetDbConnection(connection);
        await DbContext.Database.UseTransactionAsync(transaction, token);

        if (_session != null && ReferenceEquals(transaction, _session.ActiveTransaction))
        {
            // The session's own commit. Polecat does not re-run this block, so nothing can arrive
            // back here wanting these changes again, and accepting them now is what stops the next
            // save on this session from writing them twice. A flush on the session's connection is
            // not inside anybody's pipeline yet, so this is where it gets one.
            await _session.ExecuteInUnitOfWorkAsync(
                t => new ValueTask<int>(DbContext.SaveChangesAsync(acceptAllChangesOnSuccess: true, t)), token);
        }
        else
        {
            // #846, the twin of marten#5603. This is the daemon's batch, and this method is running
            // INSIDE the block PolecatProjectionBatch.ExecuteAsync hands to the resilience pipeline,
            // on a connection and a transaction that block owns. So the flush is covered already, and
            // it must not accept: plain SaveChangesAsync(token) marks every entry Unchanged the moment
            // its statements run, before anything has committed. A failure between here and the
            // commit rolls those rows back and the pipeline replays the block, into a change tracker
            // that believes it has nothing left to write, so attempt 2 commits the progression
            // without the rows. Marten accepts in an AfterCommitAsync hook; there is no need for one
            // here, because this context dies with the batch and is never flushed after its commit.
            await DbContext.SaveChangesAsync(acceptAllChangesOnSuccess: false, token);
        }

        // Idempotent, because disposal releases it too — this line used to be the ONLY release, so a
        // projection that threw while applying, an optimistic-concurrency failure in SaveChangesAsync,
        // or a throw from anywhere above skipped it entirely.
        await ReleaseAsync();
    }

    private async ValueTask ReleaseAsync()
    {
        if (_released) return;

        _released = true;

        if (_placeholderConnection != null)
        {
            await _placeholderConnection.DisposeAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        _disposed = true;

        await ReleaseAsync();
        await DbContext.DisposeAsync();
    }
}
