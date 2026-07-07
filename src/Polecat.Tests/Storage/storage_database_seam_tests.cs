using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Weasel.Storage;

namespace Polecat.Tests.Storage;

/// <summary>
///     PolecatDatabase implements the dialect-neutral Weasel.Storage.IStorageDatabase seam of the
///     shared closed-shape storage runtime (#273): sequence resolution, connection creation, and
///     raw SQL execution. The closed-shape IProviderGraph stays unavailable until Polecat's
///     document storage retargets onto the shared bases (phases D/E).
/// </summary>
public class storage_database_seam_tests : OneOffConfigurationsContext
{
    private IStorageDatabase theSeam => theDatabase;

    [Fact]
    public void polecat_database_is_the_shared_storage_database_seam()
    {
        theDatabase.ShouldBeAssignableTo<IStorageDatabase>();
    }

    [Fact]
    public async Task create_storage_connection_returns_an_openable_sql_connection()
    {
        await using var conn = theSeam.CreateStorageConnection();

        conn.ShouldBeOfType<SqlConnection>();
        conn.State.ShouldBe(System.Data.ConnectionState.Closed);

        await conn.OpenAsync();
        conn.State.ShouldBe(System.Data.ConnectionState.Open);
    }

    [Fact]
    public async Task run_sql_async_executes_against_the_database()
    {
        var tableName = $"dbo.pc_seam_smoke_{Guid.NewGuid():N}";

        await theSeam.RunSqlAsync($"CREATE TABLE {tableName} (id INT NOT NULL)");

        try
        {
            await using var conn = await OpenConnectionAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT OBJECT_ID('{tableName}')";
            var objectId = await cmd.ExecuteScalarAsync();
            objectId.ShouldNotBe(DBNull.Value);
        }
        finally
        {
            await theSeam.RunSqlAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task sequence_for_resolves_the_hilo_sequence_for_a_numeric_id_document()
    {
        // Force provider + schema creation so the Hi-Lo table exists
        await theStore.Advanced.Clean.CompletelyRemoveAllAsync();

        var sequence = theSeam.SequenceFor(typeof(IntDoc));

        sequence.ShouldNotBeNull();
        sequence.NextInt().ShouldBeGreaterThan(0);
    }

    [Fact]
    public void provider_graph_resolves_closed_shape_providers()
    {
        // #273 phase E1: the graph lazily builds the four closed-shape storage flavors
        var provider = theSeam.Providers.StorageFor<Polecat.Tests.Harness.Target>();
        provider.QueryOnly.ShouldNotBeNull();
        provider.Lightweight.ShouldNotBeNull();
        provider.IdentityMap.ShouldNotBeNull();
        provider.DirtyTracking.ShouldBeSameAs(provider.IdentityMap); // no dirty tracking by design
    }
}
