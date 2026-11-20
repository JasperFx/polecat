using System.Data.Common;
using Microsoft.Data.SqlClient;
using Polecat.Internal;

namespace Polecat.Storage;

/// <summary>
///     The control-plane data source behind <see cref="MasterTableTenancy" />. Asynchronous opens are exactly what
///     <c>SqlClientFactory.CreateDataSource</c> gives, on the connection string as configured. A synchronous open,
///     which is Weasel's <c>TryResolveTenant</c> reading <c>pc_tenants</c> for an uncached tenant, gets a connection
///     without MARS.
/// </summary>
/// <remarks>
///     #1100, the leftover from #1090 / #1096. SqlClient's managed network layer (Linux and macOS) completes a
///     synchronous read on a MARS connection through the thread pool, so a burst of synchronous tenant lookups holding
///     the pool's threads each waits for the pool to inject another one. Weasel's base class opens this data source
///     for both paths, so Polecat splits them here rather than in Weasel. A connection string that already has MARS
///     off is used as is for both (<see cref="ConnectionFactory.WithoutMars" />), so it keeps one pool.
/// </remarks>
internal sealed class ControlPlaneDataSource : DbDataSource
{
    private readonly DbDataSource _inner;
    private readonly string _synchronousConnectionString;

    public ControlPlaneDataSource(string connectionString)
    {
        _inner = SqlClientFactory.Instance.CreateDataSource(connectionString);
        _synchronousConnectionString = ConnectionFactory.WithoutMars(connectionString);
    }

    public override string ConnectionString => _inner.ConnectionString;

    /// <summary>
    ///     The connection string a synchronous open uses: <see cref="ConnectionString" /> with MARS off.
    /// </summary>
    internal string SynchronousConnectionString => _synchronousConnectionString;

    protected override DbConnection CreateDbConnection() => _inner.CreateConnection();

    protected override DbConnection OpenDbConnection()
    {
        var connection = new SqlConnection(_synchronousConnectionString);
        try
        {
            connection.Open();
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    protected override ValueTask<DbConnection> OpenDbConnectionAsync(CancellationToken cancellationToken = default)
        => _inner.OpenConnectionAsync(cancellationToken);

    protected override void Dispose(bool disposing)
    {
        if (disposing) _inner.Dispose();
        base.Dispose(disposing);
    }

    protected override async ValueTask DisposeAsyncCore()
    {
        await _inner.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsyncCore().ConfigureAwait(false);
    }
}
