using JasperFx;
using JasperFx.Events;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Weasel.Core;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     #764 / jasperfx#1005 — <c>IEventStore.FindDatabaseForTenantAsync</c> resolves through Polecat's
///     own tenancy. JasperFx's default only answers for a single-database store, so before this a
///     database-per-tenant store sent Bobcat's <c>AfterAsyncDaemonsCatchUpAsync(tenantId)</c> and
///     Wolverine's correlation query to every tenant database.
/// </summary>
public class find_database_for_tenant_tests
{
    // Static tenancy and conjoined tenancy answer from configuration alone, so these stores never
    // connect. Timeout=1 keeps an accidental connection from hiding behind a slow failure.
    private static string OfflineConnectionStringFor(string databaseName) =>
        $"Server=localhost;Database={databaseName};Integrated Security=true;TrustServerCertificate=true;Timeout=1";

    private static DocumentStore BuildOfflineStore(Action<StoreOptions> configure)
    {
        var options = new StoreOptions
        {
            ConnectionString = OfflineConnectionStringFor("find_main"),
            AutoCreateSchemaObjects = AutoCreate.None,
            DatabaseSchemaName = "find_for_tenant"
        };

        configure(options);
        return new DocumentStore(options);
    }

    [Fact]
    public async Task a_statically_registered_tenant_resolves_to_its_own_database()
    {
        await using var store = BuildOfflineStore(opts =>
        {
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("tenant1", OfflineConnectionStringFor("find_tenant1"));
                tenancy.AddTenant("tenant2", OfflineConnectionStringFor("find_tenant2"));
            });
        });

        var database = await ((IEventStore)store).FindDatabaseForTenantAsync("tenant2");

        database.ShouldNotBeNull();
        database.ShouldBeSameAs(store.Options.Tenancy!.GetDatabase("tenant2"));
        database.Identifier.ShouldBe("Polecat_tenant2");
    }

    [Fact]
    public async Task an_unknown_tenant_of_a_static_tenancy_is_null_and_adds_nothing()
    {
        await using var store = BuildOfflineStore(opts =>
        {
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("tenant1", OfflineConnectionStringFor("find_tenant1"));
            });
        });

        // GetDatabase() refuses an unknown tenant with an exception; the lookup answers null instead,
        // which is what sends the caller back to AllDatabases().
        (await ((IEventStore)store).FindDatabaseForTenantAsync("nobody")).ShouldBeNull();

        Should.Throw<UnknownTenantIdException>(() => store.Options.Tenancy!.GetDatabase("nobody"));
        (await ((IEventStore)store).AllDatabases()).Select(x => x.Identifier).ShouldBe(["Polecat_tenant1"]);
    }

    [Fact]
    public async Task every_tenant_of_a_conjoined_store_resolves_to_the_one_database()
    {
        await using var store = BuildOfflineStore(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);

        var eventStore = (IEventStore)store;

        // Including a tenant id nothing has ever written for: under conjoined tenancy a tenant's rows
        // can only be in the one database, registered or not.
        foreach (var tenantId in new[] { "tenant1", "tenant2", "never_seen", StorageConstants.DefaultTenantId })
        {
            (await eventStore.FindDatabaseForTenantAsync(tenantId)).ShouldBeSameAs(store.Database);
        }
    }

    [Fact]
    public async Task a_single_database_store_resolves_any_tenant_to_its_database()
    {
        await using var store = BuildOfflineStore(_ => { });

        (await ((IEventStore)store).FindDatabaseForTenantAsync(StorageConstants.DefaultTenantId))
            .ShouldBeSameAs(store.Database);
    }
}

/// <summary>
///     #764 — the master table tenancy half, which needs SQL Server because the tenant registry lives
///     in the pc_tenants control table.
/// </summary>
public class find_database_for_tenant_master_table_tests : IAsyncLifetime
{
    private const string TenantA = "tenant_a";
    private const string DisabledTenant = "tenant_off";
    private const string UnknownTenant = "tenant_unknown";
    private static readonly string ControlDb = ConnectionSource.Scoped("find_mt_control");
    private static readonly string DbA = ConnectionSource.Scoped("find_mt_tenant_a");

    private static string Db(string name) => ConnectionSource.ConnectionStringFor(name);

    public async ValueTask InitializeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { ControlDb, DbA })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { ControlDb, DbA })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF DB_ID('{db}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{db}];
                END
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static (DocumentStore Store, MasterTableTenancy Tenancy) CreateStore()
    {
        MasterTableTenancy? tenancy = null;
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = Db(ControlDb);
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            tenancy = opts.MultiTenantedMasterTable(Db(ControlDb));
        });

        return (store, tenancy!);
    }

    private static async Task<int> CountAsync(string connectionString, string sql)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (int)(await cmd.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task a_registered_tenant_resolves_to_its_own_database()
    {
        var (store, tenancy) = CreateStore();
        await using (store)
        {
            await tenancy.AddDatabaseRecordAsync(TenantA, Db(DbA), TestContext.Current.CancellationToken);

            var database = await ((IEventStore)store).FindDatabaseForTenantAsync(TenantA,
                TestContext.Current.CancellationToken);

            database.ShouldNotBeNull();
            database.Identifier.ShouldBe($"Polecat_{TenantA}");

            // The same PolecatDatabase a session for the tenant would use, not a lookalike.
            database.ShouldBeSameAs(((ITenancy)tenancy).GetDatabase(TenantA));
        }
    }

    [Fact]
    public async Task an_unknown_tenant_is_null_and_creates_nothing()
    {
        var (store, tenancy) = CreateStore();
        await using (store)
        {
            await tenancy.AddDatabaseRecordAsync(TenantA, Db(DbA), TestContext.Current.CancellationToken);

            var databasesBefore = await CountAsync(ConnectionSource.MasterConnectionString,
                "SELECT COUNT(*) FROM sys.databases");

            (await ((IEventStore)store).FindDatabaseForTenantAsync(UnknownTenant,
                TestContext.Current.CancellationToken)).ShouldBeNull();

            // No database created, no registry row written, nothing cached for the unknown tenant.
            (await CountAsync(ConnectionSource.MasterConnectionString, "SELECT COUNT(*) FROM sys.databases"))
                .ShouldBe(databasesBefore);
            (await CountAsync(Db(ControlDb),
                    $"SELECT COUNT(*) FROM {tenancy.QualifiedTableName} WHERE tenant_id = '{UnknownTenant}'"))
                .ShouldBe(0);
            ((ITenantedSource<string>)tenancy).AllActiveByTenant().Select(x => x.TenantId)
                .ShouldNotContain(UnknownTenant);
        }
    }

    [Fact]
    public async Task a_disabled_tenant_is_null()
    {
        var (store, tenancy) = CreateStore();
        await using (store)
        {
            await tenancy.AddDatabaseRecordAsync(DisabledTenant, Db(DbA), TestContext.Current.CancellationToken);
            await tenancy.DisableTenantAsync(DisabledTenant, TestContext.Current.CancellationToken);

            (await ((IEventStore)store).FindDatabaseForTenantAsync(DisabledTenant,
                TestContext.Current.CancellationToken)).ShouldBeNull();
        }
    }
}
