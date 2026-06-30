using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Tests.Storage;

/// <summary>
/// #267 (secondary modeling bug): a pc_doc_ table may carry columns/indexes Polecat does not model —
/// the persisted computed columns + secondary indexes Polecat itself manages via raw DDL, or objects a
/// user adds (e.g. a unique index + persisted computed column via an EF migration). Weasel's default
/// table diff would treat those as "extras" and DROP them, and the resulting permanently non-empty diff
/// keeps every storage-ensure emitting DDL. DocumentTable.CreateDeltaAsync now strips unmodeled objects
/// from the diff so Polecat is purely additive: it never drops them and the diff settles to None.
/// </summary>
public class document_table_ignores_unmodeled_objects_tests : OneOffConfigurationsContext
{
    public class Customer
    {
        public Guid Id { get; set; }
        public string? Name { get; set; }
    }

    private string SchemaName => GetType().Name.ToLowerInvariant();
    private const string TableName = "pc_doc_customer";

    private async Task AddUserManagedObjectsAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        // A persisted computed column + unique filtered index, as a user would add via a raw EF migration.
        cmd.CommandText = $"""
            ALTER TABLE [{SchemaName}].[{TableName}]
                ADD external_code AS CAST(JSON_VALUE(data, '$.name') AS varchar(100)) PERSISTED;
            """;
        await cmd.ExecuteNonQueryAsync();

        await using var indexCmd = conn.CreateCommand();
        indexCmd.CommandText = $"""
            CREATE UNIQUE INDEX ux_customer_external_code
                ON [{SchemaName}].[{TableName}] (external_code);
            """;
        await indexCmd.ExecuteNonQueryAsync();
    }

    private async Task<(bool columnExists, bool indexExists)> ProbeUserObjectsAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();

        await using var colCmd = conn.CreateCommand();
        // COL_LENGTH returns smallint (or NULL when the column is absent), so test for non-null
        // rather than a specific CLR numeric type.
        colCmd.CommandText = $"SELECT COL_LENGTH('{SchemaName}.{TableName}', 'external_code');";
        var colResult = await colCmd.ExecuteScalarAsync();
        var columnExists = colResult != null && colResult != DBNull.Value;

        await using var idxCmd = conn.CreateCommand();
        idxCmd.CommandText = """
            SELECT COUNT(*) FROM sys.indexes i
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema AND t.name = @table AND i.name = 'ux_customer_external_code';
            """;
        idxCmd.Parameters.AddWithValue("@schema", SchemaName);
        idxCmd.Parameters.AddWithValue("@table", TableName);
        var indexExists = (int)(await idxCmd.ExecuteScalarAsync())! > 0;

        return (columnExists, indexExists);
    }

    [Fact]
    public async Task user_added_column_and_index_survive_a_full_migration()
    {
        ConfigureStore(_ => { }); // default AutoCreate.All (CreateOrUpdate semantics)

        // Provision the document table.
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Customer { Id = Guid.NewGuid(), Name = "first" });
            await session.SaveChangesAsync();
        }

        await AddUserManagedObjectsAsync();

        // A full schema reconciliation must NOT drop the user-managed objects.
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var (columnExists, indexExists) = await ProbeUserObjectsAsync();
        columnExists.ShouldBeTrue();
        indexExists.ShouldBeTrue();
    }

    [Fact]
    public async Task diff_settles_to_none_with_unmodeled_objects_present()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Customer { Id = Guid.NewGuid(), Name = "first" });
            await session.SaveChangesAsync();
        }

        await AddUserManagedObjectsAsync();

        // The diff (via DetermineAsync -> DocumentTable.CreateDeltaAsync) must report NO drift, so
        // storage-ensure no longer force-applies DDL on every access.
        var mapping = theStore.GetProvider(typeof(Customer)).Mapping;
        var docTable = new DocumentTable(mapping);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        var migration = await SchemaMigration.DetermineAsync(conn, CancellationToken.None, docTable);

        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
