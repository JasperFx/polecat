using Microsoft.Data.SqlClient;

namespace Polecat.Internal;

/// <summary>
///     Creates SqlConnection instances from a connection string.
/// </summary>
internal class ConnectionFactory
{
    private readonly string _connectionString;

    public ConnectionFactory(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public string ConnectionString => _connectionString;

    /// <summary>
    ///     Create a new SqlConnection. Caller is responsible for opening and disposing it.
    /// </summary>
    public SqlConnection Create()
    {
        return new SqlConnection(_connectionString);
    }
}
