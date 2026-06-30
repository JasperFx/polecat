using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
/// #267: with AutoCreate.None the user owns the schema (e.g. EF migrations on a least-privilege
/// Azure SQL connection with no ALTER). NO runtime schema DDL may run on any storage-ensure —
/// not the async daemon's first storage access, and not the HiLo sequence's pc_hilo provisioning.
/// Previously PolecatDatabase.EnsureStorageExistsAsync force-applied the WHOLE schema (and Weasel
/// promotes None -> CreateOrUpdate), so the daemon emitted ALTER on any model drift and failed.
/// </summary>
public class auto_create_none_runtime_ddl_tests : OneOffConfigurationsContext
{
    public record ThingHappened(string Name);

    public class NumberedDoc
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private string SchemaName => GetType().Name.ToLowerInvariant();

    private async Task<bool> TableExistsAsync(string tableName)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*) FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema AND t.name = @table;
            """;
        cmd.Parameters.AddWithValue("@schema", SchemaName);
        cmd.Parameters.AddWithValue("@table", tableName);
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        return count > 0;
    }

    [Fact]
    public async Task building_the_daemon_under_auto_create_none_does_not_create_the_event_store()
    {
        ConfigureStore(opts => opts.AutoCreateSchemaObjects = AutoCreate.None);

        // The daemon's first storage access must NOT force-apply schema under None.
        var daemon = await theStore.BuildProjectionDaemonAsync();
        if (daemon is IAsyncDisposable asyncDisposable) AsyncDisposables.Add(asyncDisposable);
        else if (daemon is IDisposable disposable) Disposables.Add(disposable);

        (await TableExistsAsync("pc_events")).ShouldBeFalse();
        (await TableExistsAsync("pc_streams")).ShouldBeFalse();
        (await TableExistsAsync("pc_event_progression")).ShouldBeFalse();
    }

    [Fact]
    public async Task hilo_sequence_under_auto_create_none_does_not_create_pc_hilo()
    {
        ConfigureStore(opts => opts.AutoCreateSchemaObjects = AutoCreate.None);

        await using var session = theStore.LightweightSession();

        // HiLo id assignment runs synchronously at Store() time. Under None the sequence must not
        // provision pc_hilo, so the advance fails fast against the (user-owned, here absent) table.
        Should.Throw<Exception>(() => session.Store(new NumberedDoc { Name = "x" }));

        // Critically, the HiLo id-assignment path must not have created pc_hilo on the way.
        (await TableExistsAsync("pc_hilo")).ShouldBeFalse();
    }
}
