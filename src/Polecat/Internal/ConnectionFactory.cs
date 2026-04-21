using Microsoft.Data.SqlClient;

namespace Polecat.Internal;

/// <summary>
///     Creates SqlConnection instances from a connection string.
/// </summary>
public class ConnectionFactory
{
    private static readonly int[] TransientSqlErrors = 
    [
        1205,   // deadlock victim
        -2,     // timeout
        20,     // transport-level error
        64,     // login failed (transient)
        ..SqlConfigurableRetryFactory.BaselineTransientErrors
    ];

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
        var connection = new SqlConnection(_connectionString);

        var options = new SqlRetryLogicOption()
        {
            // Tries 5 times before throwing an exception
            NumberOfTries = 5,
            // Preferred gap time to delay before retry
            DeltaTime = TimeSpan.FromSeconds(1),
            // Maximum gap time for each delay time before retry
            MaxTimeInterval = TimeSpan.FromSeconds(20),
            TransientErrors = TransientSqlErrors
        };

        connection.RetryLogicProvider = SqlConfigurableRetryFactory.CreateExponentialRetryProvider(options);

        return connection;
    }
}
