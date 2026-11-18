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
    private readonly SqlServerGlobalLock _lock;
    private readonly string _resource;
    private readonly TimeSpan _timeout;

    internal SchemaMigrationLock(string schemaName, TimeSpan timeout)
    {
        _resource = ResourceFor(schemaName);
        _timeout = timeout;
        _lock = new SqlServerGlobalLock(_resource, (int)Math.Clamp(timeout.TotalMilliseconds, 0, int.MaxValue));
    }

    /// <summary>
    ///     The <c>sp_getapplock</c> resource every path provisioning <paramref name="schemaName" /> serializes on.
    /// </summary>
    internal static string ResourceFor(string schemaName) => $"polecat:migrate:{schemaName}";

    /// <summary>
    ///     Whether the last attempt actually got the lock. <c>ApplyAllConfiguredChangesToDatabaseAsync</c>
    ///     returns <c>None</c> both for a schema with nothing to do and for a loser under
    ///     <c>ContinueOnFailures</c>, which has checked nothing, so the return value cannot say.
    /// </summary>
    internal bool Attained { get; private set; }

    public async Task<AttainLockResult> TryAttainLock(SqlConnection conn, CancellationToken ct = default)
    {
        var result = await _lock.TryAttainLock(conn, ct).ConfigureAwait(false);

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
    /// <param name="options"></param>
    /// <param name="conn"></param>
    /// <param name="insideTransaction"></param>
    /// <param name="token"></param>
    /// <param name="schemaName">
    ///     The schema being provisioned, when that is not the store's own. <c>pc_tenants</c> is the one
    ///     case: its schema is an argument to <c>MultiTenantedMasterTable</c>.
    /// </param>
    internal static async Task<IAsyncDisposable?> HoldForFirstUseAsync(StoreOptions options, SqlConnection conn,
        bool insideTransaction, CancellationToken token, string? schemaName = null)
    {
        var migrationLock = new SchemaMigrationLock(schemaName ?? options.DatabaseSchemaName,
            insideTransaction ? TimeSpan.Zero : options.StartupMigrationLockTimeout);

        await migrationLock.TryAttainLock(conn, token).ConfigureAwait(false);

        if (migrationLock.Attained)
        {
            return new Held(migrationLock, conn);
        }

        if (!insideTransaction)
        {
            throw timedOut(migrationLock._resource, migrationLock._timeout);
        }

        return null;
    }

    /// <summary>
    ///     The synchronous counterpart of <see cref="HoldForFirstUseAsync" />, for a first-use ensure whose caller is
    ///     synchronous: runs <paramref name="ddl" /> on <paramref name="conn" /> under the same lock, with blocking
    ///     I/O from start to finish. False means the lock was busy for a caller inside a transaction, and nothing
    ///     ran: the caller skips its ensure and must not remember it as done.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     #1088 (marten#5969): a synchronous <c>Store()</c> used to block a pool thread on
    ///     <see cref="HoldForFirstUseAsync" /> and an asynchronous migration, which needed another pool thread to
    ///     finish, so the first burst of them starved the thread pool. Waiting here is waiting on another session's
    ///     DDL, which needs no thread of this process.
    ///     </para>
    ///     <para>
    ///     The lock is owned by a transaction on <paramref name="conn" /> that the DDL runs in, so committing or
    ///     failing gives it up, and nothing has to release it. <paramref name="ddl" /> must be idempotent: the
    ///     table may already exist, created by another path or another process. Waiting follows
    ///     <see cref="HoldForFirstUseAsync" />: a caller inside a transaction doesn't, and one that is not throws
    ///     when the lock times out.
    ///     </para>
    /// </remarks>
    internal static bool TryRunForFirstUse(StoreOptions options, SqlConnection conn, bool insideTransaction,
        string ddl, string? schemaName = null)
    {
        var resource = ResourceFor(schemaName ?? options.DatabaseSchemaName);
        var timeout = insideTransaction ? TimeSpan.Zero : options.StartupMigrationLockTimeout;

        using var tx = conn.BeginTransaction();

        using (var lockCmd = conn.CreateCommand())
        {
            lockCmd.Transaction = tx;
            lockCmd.CommandText =
                "DECLARE @result int; " +
                "EXEC @result = sp_getapplock @Resource = @resource, @LockMode = 'Exclusive', " +
                "@LockOwner = 'Transaction', @LockTimeout = @timeout; " +
                "SELECT @result;";
            lockCmd.Parameters.AddWithValue("@resource", resource);
            lockCmd.Parameters.AddWithValue("@timeout", (int)Math.Clamp(timeout.TotalMilliseconds, 0, int.MaxValue));

            // The server waits up to the lock timeout, so the client must wait at least that long
            lockCmd.CommandTimeout = (int)Math.Min(int.MaxValue, Math.Ceiling(timeout.TotalSeconds) + 30);

            var result = Convert.ToInt32(lockCmd.ExecuteScalar());
            if (result < 0)
            {
                tx.Rollback();

                if (!insideTransaction)
                {
                    throw timedOut(resource, timeout);
                }

                return false;
            }
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = ddl;
            cmd.ExecuteNonQuery();
        }

        tx.Commit();
        return true;
    }

    private static InvalidOperationException timedOut(string resource, TimeSpan timeout)
    {
        return new InvalidOperationException(
            $"Timed out waiting for the schema migration lock '{resource}', because something "
            + "else is applying schema changes to this database. Nothing was changed here, and the next use "
            + "will try again. If migrations legitimately take this long, raise "
            + $"{nameof(StoreOptions)}.{nameof(StoreOptions.StartupMigrationLockTimeout)}, currently "
            + $"{timeout}.");
    }

    /// <summary>
    ///     Hold the lock for provisioning a tenant, which unlike a first-use ensure can be neither skipped
    ///     nor refused, so a null result means the caller goes ahead without it. See
    ///     <see cref="Polecat.Events.Schema.TenantPartitionOrdinalRegistry.ProvisionAsync" />. The waiting is
    ///     the same as <see cref="HoldForFirstUseAsync" />: a caller inside a transaction doesn't.
    /// </summary>
    internal static async Task<IAsyncDisposable?> TryHoldForProvisioningAsync(StoreOptions options,
        SqlConnection conn, bool insideTransaction, CancellationToken token)
    {
        var migrationLock = insideTransaction
            ? new SchemaMigrationLock(options.DatabaseSchemaName, TimeSpan.Zero)
            : options.BuildMigrationLock();

        await migrationLock.TryAttainLock(conn, token).ConfigureAwait(false);

        return migrationLock.Attained ? new Held(migrationLock, conn) : null;
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
