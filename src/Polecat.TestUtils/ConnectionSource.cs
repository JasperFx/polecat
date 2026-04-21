using Microsoft.Data.SqlClient;

namespace Polecat.TestUtils;

/// <summary>
///     Centralizes the SQL Server connection string for integration tests.
///     Uses the POLECAT_TESTING_DATABASE environment variable if set,
///     otherwise falls back to the local Docker Compose instance.
/// </summary>
public static class ConnectionSource
{
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("POLECAT_TESTING_DATABASE")
        ?? "Server=localhost,11433;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False";

    private static bool? _supportsNativeJson;

    /// <summary>
    ///     Detects whether the connected SQL Server instance supports the native json data type
    ///     (SQL Server 2025+). Returns false for Azure SQL Edge and older versions.
    /// </summary>
    public static bool SupportsNativeJson
    {
        get
        {
            if (_supportsNativeJson.HasValue) return _supportsNativeJson.Value;
            _supportsNativeJson = DetectNativeJsonSupport();
            return _supportsNativeJson.Value;
        }
    }

    private static bool DetectNativeJsonSupport()
    {
        using var conn = new SqlConnection(ConnectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sys.types WHERE name = 'json'";
        var result = cmd.ExecuteScalar();
        return result != null && (int)result > 0;
    }
}
