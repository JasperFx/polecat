using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer.Tables;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     #1057 (port of marten#5684): Storage.PartitionPerTenant(table) partitions one of your own tables per
///     tenant on the store's shared tenant partition scheme, by tenant_ordinal, and a re-applied schema leaves
///     the tenants' partitions alone rather than rebuilding the table. A database of its own, because the
///     partition function and scheme are database-wide objects.
/// </summary>
public class partition_extended_table_per_tenant : IAsyncLifetime
{
    private const string Schema = "pp_tenant";
    private static readonly string Database = ConnectionSource.Scoped("pp_tenant");

    private readonly List<DocumentStore> _stores = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await dropAsync();
        await masterAsync($"CREATE DATABASE [{Database}];");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _stores) store.Dispose();
        await dropAsync();
    }

    private static async Task dropAsync()
    {
        SqlConnection.ClearAllPools();
        await masterAsync($"""
            IF DB_ID('{Database}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{Database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{Database}];
            END
            """);
    }

    private static async Task masterAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> scalarAsync(string sql, Action<SqlCommand>? configure = null)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(Database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        configure?.Invoke(cmd);
        return await cmd.ExecuteScalarAsync(Ct);
    }

    private static Table ledgerTable()
    {
        var table = new Table(new DbObjectName(Schema, "ledger_entries"));
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn("tenant_id", "varchar(250)").NotNull();
        table.AddColumn<int>("amount");
        return table;
    }

    private DocumentStore track(DocumentStore store)
    {
        _stores.Add(store);
        return store;
    }

    private DocumentStore buildStore()
    {
        return track(DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionStringFor(Database);
            opts.DatabaseSchemaName = Schema;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Policies.AllDocumentsAreMultiTenantedWithPartitioning();

            #region sample_partition_extended_table_per_tenant

            var table = new Table(new DbObjectName(Schema, "ledger_entries"));
            table.AddColumn<Guid>("id").AsPrimaryKey();
            table.AddColumn("tenant_id", "varchar(250)").NotNull();
            table.AddColumn<int>("amount");

            // One partition per tenant on the store's tenant partition scheme, by the tenant_ordinal
            // column Polecat adds, and left alone by later schema migrations
            opts.Storage.PartitionPerTenant(table);

            #endregion
        }));
    }

    private static async Task insertAsync(string tenantId, int amount)
    {
        #region sample_insert_into_a_table_partitioned_per_tenant

        // tenant_ordinal comes from the store's pc_tenant_ordinal() function
        var sql = $"""
            INSERT INTO {Schema}.ledger_entries (id, tenant_id, tenant_ordinal, amount)
            VALUES (NEWID(), @tenant_id, {Schema}.pc_tenant_ordinal(@tenant_id), @amount);
            """;

        #endregion

        await scalarAsync(sql, cmd =>
        {
            cmd.Parameters.AddWithValue("@tenant_id", tenantId);
            cmd.Parameters.AddWithValue("@amount", amount);
        });
    }

    private static async Task<int> partitionCountAsync()
    {
        return (int)(await scalarAsync($"""
            SELECT COUNT(*) FROM sys.partitions
            WHERE object_id = OBJECT_ID('{Schema}.ledger_entries') AND index_id IN (0, 1)
            """))!;
    }

    [Fact]
    public async Task reapplying_the_schema_over_existing_tenants_does_not_rebuild_the_table()
    {
        // First deploy: the table, two tenants (a partition each), and rows in both
        int partitions;
        {
            var store = buildStore();
            await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
            await store.Advanced.AddPolecatManagedTenantsAsync(Ct, "acme", "globex");

            await insertAsync("acme", 1);
            await insertAsync("globex", 2);

            partitions = await partitionCountAsync();
            partitions.ShouldBeGreaterThanOrEqualTo(3);
        }

        // Second deploy, nothing changed: no difference, so no rebuild
        {
            var store = buildStore();
            var migration = await store.Database.CreateMigrationAsync(Ct);
            migration.Difference.ShouldBe(SchemaPatchDifference.None);

            await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

            // A tenant added later gets its partition in this table too
            await store.Advanced.AddPolecatManagedTenantsAsync(Ct, "initech");
            await insertAsync("initech", 3);

            (await partitionCountAsync()).ShouldBe(partitions + 1);
            ((int)(await scalarAsync($"SELECT COUNT(*) FROM {Schema}.ledger_entries"))!).ShouldBe(3);

            // Each tenant's row is in its own partition
            ((int)(await scalarAsync($"""
                SELECT COUNT(DISTINCT $PARTITION.{store.Options.EventGraph.TenantPartitionManager.PartitionFunctionName(ledgerTable())}(tenant_ordinal))
                FROM {Schema}.ledger_entries
                """))!).ShouldBe(3);
        }
    }

    [Fact]
    public void the_table_is_partitioned_like_the_document_tables()
    {
        var table = ledgerTable();

        var store = track(DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionStringFor(Database);
            opts.DatabaseSchemaName = Schema;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement();
            opts.Storage.PartitionPerTenant(table);
        }));

        table.SqlServerPartitioning.ShouldBeSameAs(store.Options.EventGraph.TenantPartitionManager);

        var ordinal = table.ColumnFor("tenant_ordinal").ShouldNotBeNull();
        ordinal.Type.ShouldBe("int");
        ordinal.AllowNulls.ShouldBeFalse();

        // SQL Server requires the partition column in the clustered index
        table.PrimaryKeyColumns.ShouldBe(["id", "tenant_ordinal"], ignoreOrder: true);

        store.Options.ExtendedSchemaObjects.ShouldContain(table);
        store.Options.Storage.ExtendedSchemaObjects.ShouldBeSameAs(store.Options.ExtendedSchemaObjects);
    }

    [Fact]
    public void a_table_with_its_own_tenant_ordinal_column_keeps_it()
    {
        var table = ledgerTable();
        table.AddColumn("tenant_ordinal", "int");

        track(DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionStringFor(Database);
            opts.DatabaseSchemaName = Schema;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Storage.PartitionPerTenant(table);
        }));

        table.Columns.Count(x => x.Name == "tenant_ordinal").ShouldBe(1);
        table.ColumnFor("tenant_ordinal")!.AllowNulls.ShouldBeFalse();
        table.PrimaryKeyColumns.ShouldBe(["id", "tenant_ordinal"], ignoreOrder: true);
    }

    [Fact]
    public void the_same_table_can_be_handed_to_a_second_store()
    {
        var table = ledgerTable();

        DocumentStore build() => track(DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionStringFor(Database);
            opts.DatabaseSchemaName = Schema;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement();
            opts.Storage.PartitionPerTenant(table);
        }));

        build();
        var second = build();

        table.SqlServerPartitioning.ShouldBeSameAs(second.Options.EventGraph.TenantPartitionManager);
        table.Columns.Count(x => x.Name == "tenant_ordinal").ShouldBe(1);
    }

    [Fact]
    public void a_table_partitioned_another_way_is_refused()
    {
        var table = ledgerTable();
        table.AddColumn<DateTimeOffset>("posted").AsPrimaryKey();
        table.PartitionByRange("posted", "datetimeoffset");

        var ex = Should.Throw<InvalidOperationException>(() =>
        {
            track(DocumentStore.For(opts =>
            {
                opts.ConnectionString = ConnectionSource.ConnectionStringFor(Database);
                opts.DatabaseSchemaName = Schema;
                opts.Events.TenancyStyle = TenancyStyle.Conjoined;
                opts.Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement();
                opts.Storage.PartitionPerTenant(table);
            }));
        });

        ex.Message.ShouldContain("only one partition scheme");
    }

    [Fact]
    public void a_store_without_polecat_managed_tenant_partitioning_refuses()
    {
        var ex = Should.Throw<InvalidOperationException>(() =>
        {
            track(DocumentStore.For(opts =>
            {
                opts.ConnectionString = ConnectionSource.ConnectionStringFor(Database);
                opts.DatabaseSchemaName = Schema;
                opts.Storage.PartitionPerTenant(ledgerTable());
            }));
        });

        ex.Message.ShouldContain($"{Schema}.ledger_entries");
        ex.Message.ShouldContain("PartitionMultiTenantedDocumentsUsingPolecatManagement");
    }

    [Fact]
    public async Task the_tenant_ordinal_comes_from_the_store_or_from_sql()
    {
        var store = buildStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // A new tenant is registered, with its partitions, as its first write would
        #region sample_tenant_ordinal_from_the_store
        var ordinal = await store.Advanced.TenantOrdinalAsync("acme", Ct);
        #endregion

        (await store.Advanced.TenantOrdinalAsync("acme", Ct)).ShouldBe(ordinal);

        (await scalarAsync($"SELECT {Schema}.pc_tenant_ordinal(@tenant_id)",
            cmd => cmd.Parameters.AddWithValue("@tenant_id", "acme"))).ShouldBe(ordinal);
        (await scalarAsync($"SELECT ordinal FROM {Schema}.pc_tenant_partitions WHERE tenant_id = 'acme'"))
            .ShouldBe(ordinal);

        // A tenant nobody has registered has no ordinal, so an insert for it fails on NOT NULL
        (await scalarAsync($"SELECT {Schema}.pc_tenant_ordinal(@tenant_id)",
            cmd => cmd.Parameters.AddWithValue("@tenant_id", "nobody"))).ShouldBe(DBNull.Value);
        await Should.ThrowAsync<SqlException>(() => insertAsync("nobody", 1));

        await insertAsync("acme", 5);
        ((int)(await scalarAsync($"SELECT tenant_ordinal FROM {Schema}.ledger_entries"))!).ShouldBe(ordinal);
    }

    [Fact]
    public async Task the_tenant_ordinal_needs_polecat_managed_tenant_partitioning()
    {
        var store = track(DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionStringFor(Database);
            opts.DatabaseSchemaName = Schema;
        }));

        await Should.ThrowAsync<InvalidOperationException>(() => store.Advanced.TenantOrdinalAsync("acme", Ct));
    }
}
