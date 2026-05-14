using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     SQL Server application-lock primitive used by the projection coordinator
///     to negotiate hot-cold leadership across Polecat nodes for the same database.
/// </summary>
/// <remarks>
///     Wraps <c>sp_getapplock</c> / <c>sp_releaseapplock</c> with
///     <c>@LockOwner = 'Session'</c>. Holds a single dedicated <see cref="SqlConnection"/>
///     for the lifetime of this lock owner — the locks are session-bound, so the
///     connection must stay open as long as we hold any lock. If the connection
///     drops, SQL Server auto-releases all locks on this session and we
///     proactively clear our local <c>_handles</c> tracking on the next operation
///     so the coordinator's <see cref="HasLock"/> check stays honest.
///
///     Modelled on Marten's <c>Weasel.Postgresql.AdvisoryLock</c>; differences
///     are purely SQL-dialect.
/// </remarks>
internal sealed class SqlServerAppLock : IAsyncDisposable
{
    private readonly string _connectionString;
    private readonly string _databaseIdentifier;
    private readonly ILogger _logger;
    private readonly Dictionary<int, byte> _handles = new();
    private readonly SemaphoreSlim _gate = new(1, 1);

    private SqlConnection? _connection;
    private bool _disposed;

    public SqlServerAppLock(string connectionString, string databaseIdentifier, ILogger logger)
    {
        _connectionString = connectionString;
        _databaseIdentifier = databaseIdentifier;
        _logger = logger;
    }

    /// <summary>
    ///     Whether this owner currently holds the given lock id. O(1) — checks
    ///     in-memory tracking only; does not contact the server.
    /// </summary>
    public bool HasLock(int lockId)
    {
        // If we hold the handle but the connection is broken, the SQL Server
        // session has already released the lock for us. Clear and return false
        // so the coordinator re-attempts acquisition rather than acting on a
        // stale belief that we own it.
        if (_handles.ContainsKey(lockId))
        {
            if (_connection is { State: ConnectionState.Open })
            {
                return true;
            }

            // Connection dropped — clear stale handle tracking.
            _handles.Clear();
            _connection = null;
            return false;
        }

        return false;
    }

    /// <summary>
    ///     Attempt to acquire the lock with no wait. Returns true if granted
    ///     immediately, false if another session already holds it.
    /// </summary>
    public async Task<bool> TryAttainLockAsync(int lockId, CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _gate.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_handles.ContainsKey(lockId)) return true;

            var connection = await EnsureConnectionAsync(token).ConfigureAwait(false);

            await using var cmd = connection.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = "sp_getapplock";

            cmd.Parameters.AddWithValue("@Resource", ResourceName(lockId));
            cmd.Parameters.AddWithValue("@LockMode", "Exclusive");
            cmd.Parameters.AddWithValue("@LockOwner", "Session");
            cmd.Parameters.AddWithValue("@LockTimeout", 0); // no wait
            cmd.Parameters.AddWithValue("@DbPrincipal", "public");

            var result = cmd.Parameters.Add("@Result", SqlDbType.Int);
            result.Direction = ParameterDirection.ReturnValue;

            await cmd.ExecuteNonQueryAsync(token).ConfigureAwait(false);

            var code = (int)result.Value!;
            // Per docs: 0 = granted synchronously, 1 = granted after a wait,
            // negatives are timeout / cancel / deadlock / parameter error.
            if (code >= 0)
            {
                _handles[lockId] = 0;
                return true;
            }

            if (code != -1)
            {
                _logger.LogWarning(
                    "sp_getapplock for resource {Resource} on database {Database} returned {Code}",
                    ResourceName(lockId), _databaseIdentifier, code);
            }
            return false;
        }
        catch (Exception e)
        {
            _logger.LogError(e,
                "Error attempting sp_getapplock for resource {Resource} on database {Database}",
                ResourceName(lockId), _databaseIdentifier);
            // Connection may be dead — drop it so the next attempt reconnects.
            await DisposeConnectionAsync().ConfigureAwait(false);
            return false;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    ///     Release a lock we hold. No-op if we don't hold it. Best-effort —
    ///     swallows errors and clears local tracking so the coordinator can
    ///     move on after a failed release.
    /// </summary>
    public async Task ReleaseLockAsync(int lockId)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (!_handles.Remove(lockId)) return;
            if (_connection is not { State: ConnectionState.Open }) return;

            await using var cmd = _connection.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = "sp_releaseapplock";

            cmd.Parameters.AddWithValue("@Resource", ResourceName(lockId));
            cmd.Parameters.AddWithValue("@LockOwner", "Session");
            cmd.Parameters.AddWithValue("@DbPrincipal", "public");

            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogWarning(e,
                "Error releasing sp_releaseapplock for resource {Resource} on database {Database}",
                ResourceName(lockId), _databaseIdentifier);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            // Closing the connection auto-releases every Session-scoped lock
            // we hold, so we don't need to call sp_releaseapplock per id.
            await DisposeConnectionAsync().ConfigureAwait(false);
            _handles.Clear();
        }
        finally
        {
            _gate.Release();
            _gate.Dispose();
        }
    }

    private async ValueTask<SqlConnection> EnsureConnectionAsync(CancellationToken token)
    {
        if (_connection is { State: ConnectionState.Open })
        {
            return _connection;
        }

        await DisposeConnectionAsync().ConfigureAwait(false);

        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(token).ConfigureAwait(false);
        _connection = conn;
        return conn;
    }

    private async ValueTask DisposeConnectionAsync()
    {
        if (_connection is null) return;
        try
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogWarning(e,
                "Error closing the SQL Server lock connection for database {Database}",
                _databaseIdentifier);
        }
        finally
        {
            _connection = null;
        }
    }

    private static string ResourceName(int lockId) =>
        $"polecat_projection_{lockId}";
}
