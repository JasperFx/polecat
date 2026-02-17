using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Storage;

namespace Polecat.Tests.Harness;

/// <summary>
///     Shared xUnit fixture that creates and caches a DocumentStore instance.
///     Applied once per test collection via <see cref="IntegrationCollection" />.
/// </summary>
public class DefaultStoreFixture : IAsyncLifetime
{
    public PolecatDatabase Database { get; private set; } = null!;
    public StoreOptions Options { get; private set; } = null!;
    public DocumentStore Store { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        Options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            AutoCreateSchemaObjects = AutoCreate.All
        };

        Store = new DocumentStore(Options);
        Database = Store.Database;

        // Clean slate: drop existing event store tables, then recreate
        await DropAllEventStoreTablesAsync();
        await Database.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    public Task DisposeAsync()
    {
        Store?.Dispose();
        return Task.CompletedTask;
    }

    internal async Task DropAllEventStoreTablesAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            IF OBJECT_ID('dbo.pc_events', 'U') IS NOT NULL DROP TABLE dbo.pc_events;
            IF OBJECT_ID('dbo.pc_streams', 'U') IS NOT NULL DROP TABLE dbo.pc_streams;
            IF OBJECT_ID('dbo.pc_event_progression', 'U') IS NOT NULL DROP TABLE dbo.pc_event_progression;
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    internal async Task TruncateAllEventStoreTablesAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DELETE FROM dbo.pc_events;
            DELETE FROM dbo.pc_streams;
            DELETE FROM dbo.pc_event_progression;
            """;
        await cmd.ExecuteNonQueryAsync();
    }
}
