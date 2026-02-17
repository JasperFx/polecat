namespace Polecat.Tests.Harness;

/// <summary>
///     Centralizes the SQL Server connection string for integration tests.
///     Uses the POLECAT_TESTING_DATABASE environment variable if set,
///     otherwise falls back to the local Docker Compose instance.
/// </summary>
public static class ConnectionSource
{
    public static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("POLECAT_TESTING_DATABASE")
        ?? "Server=localhost,11433;Database=polecat_testing;User Id=sa;Password=Polecat#Dev2025;TrustServerCertificate=true";
}
