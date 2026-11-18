using Microsoft.Data.SqlClient;
using Polecat.TestUtils;

namespace Polecat.Tests.Harness;

/// <summary>
///     Turns on <c>ALLOW_SNAPSHOT_ISOLATION</c> for the catalog a test runs in, for the snapshot session
///     tests. On by default in <c>master</c>, where CI runs, and off in a catalog made for a parallel worker.
/// </summary>
public static class SnapshotIsolation
{
    public static Task AllowAsync(CancellationToken token) => AllowAsync(ConnectionSource.ConnectionString, token);

    /// <remarks>
    ///     #1039: the ALTER goes through dynamic SQL, by the catalog's name. SQL Server refuses
    ///     <c>ALTER DATABASE CURRENT</c> on a system database when it compiles the batch, before the IF
    ///     is evaluated, so a plain run against <c>master</c> failed here although master already allows
    ///     snapshot isolation and the ALTER would never have run.
    /// </remarks>
    public static async Task AllowAsync(string connectionString, CancellationToken token)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            IF (SELECT snapshot_isolation_state FROM sys.databases WHERE name = DB_NAME()) = 0
            BEGIN
                DECLARE @sql nvarchar(400) = N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + N' SET ALLOW_SNAPSHOT_ISOLATION ON;';
                EXEC sp_executesql @sql;
            END
            """;
        await cmd.ExecuteNonQueryAsync(token);
    }
}
