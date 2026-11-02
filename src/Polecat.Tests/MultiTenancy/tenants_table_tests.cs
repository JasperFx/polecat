using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polecat.TestUtils;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     #883: <c>pc_tenants</c> is a Weasel table now rather than a guarded CREATE string, and every
///     control database in the field has one that string made. Those have never been diffed against
///     anything, so their first delta has to be clean.
/// </summary>
public class tenants_table_tests
{
    private static string NewSchema() => "tt_" + Guid.NewGuid().ToString("N")[..10];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    ///     What <c>MasterTableTenancy.EnsureMasterTableAsync</c> used to run, verbatim.
    /// </summary>
    private static async Task CreateTheWayTheOldPathDidAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF SCHEMA_ID(@schema) IS NULL
            BEGIN
                DECLARE @createSchema NVARCHAR(MAX) = N'CREATE SCHEMA ' + QUOTENAME(@schema);
                EXEC sp_executesql @createSchema;
            END
            IF OBJECT_ID(@qualified, 'U') IS NULL
                CREATE TABLE [{schema}].[pc_tenants] (
                    tenant_id NVARCHAR(200) NOT NULL CONSTRAINT pk_pc_tenants PRIMARY KEY,
                    connection_string NVARCHAR(MAX) NOT NULL,
                    is_disabled BIT NOT NULL CONSTRAINT df_pc_tenants_is_disabled DEFAULT 0
                );
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@qualified", $"[{schema}].[pc_tenants]");
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<SchemaPatchDifference> DifferenceOfAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        var migration = await SchemaMigration.DetermineAsync(conn, Ct, new TenantsTable(schema));
        return migration.Difference;
    }

    private static MasterTableTenancy TenancyFor(string schema) => new(
        new StoreOptions { ConnectionString = ConnectionSource.ConnectionString, AutoCreateSchemaObjects = AutoCreate.All },
        ConnectionSource.ConnectionString, schema);

    [Fact]
    public async Task a_table_the_old_path_created_needs_no_migration()
    {
        var schema = NewSchema();
        await CreateTheWayTheOldPathDidAsync(schema);

        (await DifferenceOfAsync(schema)).ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_new_path_builds_the_table_the_old_path_built()
    {
        var schema = NewSchema();
        var tenancy = TenancyFor(schema);

        await tenancy.AddDatabaseRecordAsync("a", ConnectionSource.ConnectionString, Ct);
        await tenancy.DisableTenantAsync("a", Ct);

        (await tenancy.AllDisabledAsync(Ct)).ShouldBe(["a"]);
        (await DifferenceOfAsync(schema)).ShouldBe(SchemaPatchDifference.None);

        // A second run of the old statement is a no-op against what Weasel made: same name, and the
        // constraint names are the ones anything written against the old table would expect.
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT name FROM sys.key_constraints
            WHERE parent_object_id = OBJECT_ID('[{schema}].[pc_tenants]') AND type = 'PK';
            """;
        (await cmd.ExecuteScalarAsync(Ct)).ShouldBe("pk_pc_tenants");
    }

    /// <summary>
    ///     A control table is a natural place for an operator's own bookkeeping, and nothing has ever
    ///     compared this one to anything. <c>pc_tenants</c> owns three columns, not the table.
    /// </summary>
    [Fact]
    public async Task a_column_somebody_added_is_left_alone()
    {
        var schema = NewSchema();
        await CreateTheWayTheOldPathDidAsync(schema);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using (var alter = conn.CreateCommand())
        {
            alter.CommandText = $"ALTER TABLE [{schema}].[pc_tenants] ADD owner_team nvarchar(100) NULL;";
            await alter.ExecuteNonQueryAsync(Ct);
        }

        (await DifferenceOfAsync(schema)).ShouldBe(SchemaPatchDifference.None);

        var tenancy = TenancyFor(schema);
        await tenancy.AddDatabaseRecordAsync("a", ConnectionSource.ConnectionString, Ct);

        await using var count = conn.CreateCommand();
        count.CommandText =
            $"SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID('[{schema}].[pc_tenants]') AND name = 'owner_team';";
        (await count.ExecuteScalarAsync(Ct)).ShouldBe(1);
    }

    [Fact]
    public async Task auto_create_none_does_not_create_the_table()
    {
        var schema = NewSchema();
        var tenancy = new MasterTableTenancy(
            new StoreOptions
            {
                ConnectionString = ConnectionSource.ConnectionString,
                AutoCreateSchemaObjects = AutoCreate.None
            },
            ConnectionSource.ConnectionString, schema);

        var failure = await Should.ThrowAsync<SqlException>(() => tenancy.AllDisabledAsync(Ct));
        failure.Number.ShouldBe(208);
    }
}
