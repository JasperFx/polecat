using Microsoft.Data.SqlClient;

namespace Polecat.Internal;

/// <summary>
///     Creates SqlConnection instances from a connection string.
/// </summary>
public class ConnectionFactory
{
    /// <summary>
    ///     Errors that make <c>Open()</c> worth retrying. This list governs the CONNECTION OPEN and
    ///     nothing else: in Microsoft.Data.SqlClient, <see cref="SqlConnection.RetryLogicProvider" />
    ///     is consulted only by <c>Open()</c>/<c>OpenAsync()</c>, while command execution is retried
    ///     only when <c>SqlCommand.RetryLogicProvider</c> is set — which Polecat never does.
    /// </summary>
    /// <remarks>
    ///     #652: 1205 (deadlock victim) used to be listed here explicitly, and it was inert and
    ///     misleading twice over. You cannot be chosen as a deadlock victim while opening a
    ///     connection, and a deadlock raised by an exclusive append is a COMMAND failure, which this
    ///     provider never sees — every <c>SqlCommand</c> carries its own provider, defaulted to one
    ///     try, and assigning <c>Connection</c> does not change that. The code implied five attempts
    ///     where there had only ever been one.
    ///     <para>
    ///         SqlClient's own baseline list also carries 1205 and 1222, for the same non-reason, so
    ///         dropping the local entry alone would have left the claim standing. Both are filtered
    ///         out below. 1204 ("no more lock resources") stays: that is server-wide lock-memory
    ///         exhaustion rather than contention for a row, and retrying an OPEN through it is what
    ///         SqlClient intends.
    ///     </para>
    ///     <para>
    ///         Polecat does not retry a deadlock or a lock timeout internally, and deliberately so: by
    ///         the time 1205 is raised the transaction is already rolled back, so anything worth
    ///         calling a retry is a replay of the whole unit of work, not of one command. The contract
    ///         is that you get <see cref="Exceptions.StreamLockedException" /> and retry the command at
    ///         the application layer — e.g. a Wolverine
    ///         <c>OnException&lt;StreamLockedException&gt;().RetryWithCooldown(...)</c> policy. See
    ///         <c>docs/events/appending.md</c>.
    ///     </para>
    ///     The remaining entries do real work, but only for the open.
    /// </remarks>
    internal static readonly int[] TransientSqlErrors =
    [
        -2,     // timeout
        20,     // transport-level error
        64,     // login failed (transient)
        ..SqlConfigurableRetryFactory.BaselineTransientErrors.Where(x => x is not (1205 or 1222))
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
