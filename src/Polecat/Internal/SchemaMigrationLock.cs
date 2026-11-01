using Microsoft.Data.SqlClient;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Internal;

/// <summary>
///     The one <c>sp_getapplock</c> lock that everything provisioning a store's schema serializes on:
///     startup migrations, the daemon's ensure, and a session's first use of the event store or of a
///     document table.
/// </summary>
/// <remarks>
///     <para>
///     #664 / weasel#599. Until Weasel 9.33.0 there was no <c>IGlobalLock&lt;SqlConnection&gt;</c> at all,
///     so two replicas starting together both introspected the catalog, both derived a patch from the same
///     state, and both ran DDL. Polecat does not opt into <c>Migrator.UseSchemaFingerprinting</c>, so
///     there is no stamp short-circuit either — the race was on every deploy, not only the first.
///     </para>
///     <para>
///     #868. That lock was only taken at startup, so a session's first write and the daemon's ensure still
///     met on a fresh schema, and the loser failed with
///     <c>There is already an object named 'fk_pc_events_stream_id' in the database</c>. A
///     <c>SemaphoreSlim</c> cannot answer this: it covers one ensurer in one process, where this is two
///     unrelated code paths and quite possibly two processes. Every path has to ask the server for the same
///     resource, which is why that resource is decided here and nowhere else.
///     </para>
///     <para>
///     Application locks are scoped to the CURRENT DATABASE, and every path opens its own connection per
///     tenant database, so per-database isolation falls out for free: one tenant's migration must not block
///     another's, and it does not. What does not fall out is two Polecat stores inside one database under
///     different schemas — the ancillary-store pattern, and every test in this repo — so the resource name
///     is the schema, and nothing else. Naming it after <c>PolecatDatabase.Identifier</c> would be actively
///     wrong: that is the constant "Polecat" for a single-database store, so two stores sharing one database
///     would share one lock.
///     </para>
///     <para>
///     The timeout is <see cref="StoreOptions.StartupMigrationLockTimeout" /> rather than Weasel's 1000ms
///     default, because contention is not retried and the loser would otherwise give up after one second of
///     waiting on a migration that takes longer than that. See that property for the whole reasoning.
///     </para>
/// </remarks>
internal sealed class SchemaMigrationLock : IGlobalLock<SqlConnection>
{
    /// <summary>
    ///     SqlClient's own number for a command that ran out of time on the client.
    /// </summary>
    private const int ClientCommandTimeout = -2;

    private readonly SqlServerGlobalLock _lock;
    private readonly string _resource;
    private readonly TimeSpan _timeout;

    internal SchemaMigrationLock(string schemaName, TimeSpan timeout)
    {
        _resource = $"polecat:migrate:{schemaName}";
        _timeout = timeout;
        _lock = new SqlServerGlobalLock(_resource, (int)Math.Clamp(timeout.TotalMilliseconds, 0, int.MaxValue));
    }

    /// <summary>
    ///     Whether the last attempt actually got the lock. <c>ApplyAllConfiguredChangesToDatabaseAsync</c>
    ///     returns <c>None</c> both for a schema with nothing to do and for a loser under
    ///     <c>ContinueOnFailures</c>, which has checked nothing, so the return value cannot say.
    /// </summary>
    internal bool Attained { get; private set; }

    /// <remarks>
    ///     ⚠️ Weasel runs <c>sp_getapplock</c> as an ordinary command under the connection's default command
    ///     timeout, 30 seconds unless the connection string says otherwise. So with the default 30 second
    ///     lock timeout the client gives up first, and a busy lock arrives here as a raw
    ///     <c>SqlException</c> rather than a failed result, which meant <c>ContinueOnFailures</c> could never
    ///     engage on the default settings and nothing told the caller what it had been waiting for. All this
    ///     command does is wait for the lock, so a timeout on it is reported as what it almost always is.
    /// </remarks>
    public async Task<AttainLockResult> TryAttainLock(SqlConnection conn, CancellationToken ct = default)
    {
        AttainLockResult result;
        try
        {
            result = await _lock.TryAttainLock(conn, ct).ConfigureAwait(false);
        }
        catch (SqlException e) when (e.Number == ClientCommandTimeout)
        {
            result = AttainLockResult.Failure();
        }

        Attained = result.Succeeded;
        return result;
    }

    public Task ReleaseLock(SqlConnection conn, CancellationToken ct = default) => _lock.ReleaseLock(conn, ct);

    /// <summary>
    ///     Hold the lock for a first-use ensure running on <paramref name="conn" />, until the result is
    ///     disposed. A null result means the caller must skip its ensure, and must not remember it as done.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     ⚠️ <paramref name="insideTransaction" /> is what stops this being worse than the bug it fixes.
    ///     The ensure runs on a connection of its own, so a session waiting here while its transaction sits
    ///     open on ANOTHER connection is invisible to SQL Server's deadlock monitor: this connection waits on
    ///     the lock holder, the holder's DDL waits for a schema lock on a table the transaction has touched,
    ///     and the transaction is idle, waiting on nothing the server can see. Nothing breaks that cycle
    ///     until something times out. So a caller inside a transaction never waits: it takes the lock if it
    ///     is free this instant, and otherwise it does not run the ensure at all.
    ///     </para>
    ///     <para>
    ///     Skipping is deliberate, and carrying on unlocked is not the safer choice it looks like. That was
    ///     the first cut of this, and it stalled for 29.6 seconds against a real migration, on a save that
    ///     never touched the lock: the ensure's catalog reads queue behind the migration's pending schema
    ///     lock, which is queued behind the transaction, which is waiting on the ensure. It is the same
    ///     cycle one step later. A table the transaction has already touched plainly exists, so skipping
    ///     costs nothing there. A table that really is missing fails its statement at once, with nothing
    ///     remembered, and the next use tries again.
    ///     </para>
    ///     <para>
    ///     A loser with no transaction open throws. Nothing has been memoized by then, so the next use
    ///     tries again, and the failure names the setting that decides how long it waited.
    ///     </para>
    /// </remarks>
    internal static async Task<IAsyncDisposable?> HoldForFirstUseAsync(StoreOptions options, SqlConnection conn,
        bool insideTransaction, CancellationToken token)
    {
        var migrationLock = insideTransaction
            ? new SchemaMigrationLock(options.DatabaseSchemaName, TimeSpan.Zero)
            : options.BuildMigrationLock();

        await migrationLock.TryAttainLock(conn, token).ConfigureAwait(false);

        if (migrationLock.Attained)
        {
            return new Held(migrationLock, conn);
        }

        if (!insideTransaction)
        {
            throw new InvalidOperationException(
                $"Timed out waiting for the schema migration lock '{migrationLock._resource}', because something "
                + "else is applying schema changes to this database. Nothing was changed here, and the next use "
                + "will try again. If migrations legitimately take this long, raise "
                + $"{nameof(StoreOptions)}.{nameof(StoreOptions.StartupMigrationLockTimeout)}, currently "
                + $"{migrationLock._timeout}, and the connection string's Command Timeout along with it.");
        }

        return null;
    }

    /// <summary>
    ///     A session-scoped lock outlives the connection handed back to the pool: only an explicit release,
    ///     or the reset when that connection is next reused, gives it up (weasel#659). So this releases on
    ///     every way out, including a failed migration, and with no token, because a cancelled ensure is one
    ///     of those ways out.
    /// </summary>
    private sealed class Held(SchemaMigrationLock migrationLock, SqlConnection conn) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try
            {
                await migrationLock.ReleaseLock(conn, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // The ensure's own failure is the one the caller needs to see, and a connection that cannot
                // run the release is broken, which ends its session and the lock along with it.
            }
        }
    }
}
