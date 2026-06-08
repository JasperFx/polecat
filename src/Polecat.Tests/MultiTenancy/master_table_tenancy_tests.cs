using JasperFx;
using JasperFx.Descriptors;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polecat.TestUtils;

namespace Polecat.Tests.MultiTenancy;

public class master_table_tenancy_tests : IAsyncLifetime
{
    private const string TenantA = "tenant_a";
    private const string TenantB = "tenant_b";
    private const string ControlDb = "polecat_mt_control";
    private const string DbA = "polecat_mt_tenant_a";
    private const string DbB = "polecat_mt_tenant_b";

    private static readonly string MasterConnectionString =
        ConnectionSource.ConnectionString.Replace("Initial Catalog=master", "Database=master");

    private static string Db(string name) =>
        ConnectionSource.ConnectionString.Replace("Initial Catalog=master", $"Database={name}");

    public async Task InitializeAsync()
    {
        await using var conn = new SqlConnection(MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { ControlDb, DbA, DbB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public async Task DisposeAsync()
    {
        await using var conn = new SqlConnection(MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { ControlDb, DbA, DbB })
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

    private (DocumentStore Store, MasterTableTenancy Tenancy) CreateStore()
    {
        MasterTableTenancy? tenancy = null;
        var store = DocumentStore.For(opts =>
        {
            // The default connection just needs to be valid; routing goes through the tenancy.
            opts.ConnectionString = Db(ControlDb);
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            tenancy = opts.MultiTenantedMasterTable(Db(ControlDb));
        });

        return (store, tenancy!);
    }

    private static async Task ApplySchemaAsync(MasterTableTenancy tenancy)
    {
        foreach (var db in await tenancy.BuildDatabasesAsync())
        {
            await db.ApplyAllConfiguredChangesToDatabaseAsync();
        }
    }

    [Fact]
    public async Task cardinality_is_dynamic_multiple()
    {
        var (store, _) = CreateStore();
        using (store)
        {
            store.Options.Tenancy!.Cardinality.ShouldBe(DatabaseCardinality.DynamicMultiple);
        }
    }

    [Fact]
    public async Task add_records_and_route_to_separate_databases()
    {
        var (store, tenancy) = CreateStore();
        using (store)
        {
            await tenancy.AddDatabaseRecordAsync(TenantA, Db(DbA));
            await tenancy.AddDatabaseRecordAsync(TenantB, Db(DbB));
            await ApplySchemaAsync(tenancy);

            var docId = Guid.NewGuid();

            await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
            {
                session.Store(new TestDoc { Id = docId, Name = "Tenant A Doc" });
                await session.SaveChangesAsync();
            }

            await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantA }))
            {
                (await query.LoadAsync<TestDoc>(docId))!.Name.ShouldBe("Tenant A Doc");
            }

            // Separate database — tenant B cannot see tenant A's document.
            await using (var query = store.QuerySession(new SessionOptions { TenantId = TenantB }))
            {
                (await query.LoadAsync<TestDoc>(docId)).ShouldBeNull();
            }
        }
    }

    [Fact]
    public async Task unknown_tenant_throws()
    {
        var (store, _) = CreateStore();
        using (store)
        {
            // Touch the tenancy so the master table is created, but never add this tenant.
            await Should.ThrowAsync<UnknownTenantIdException>(async () =>
            {
                await using var _ = store.LightweightSession(new SessionOptions { TenantId = "nonexistent" });
            });
        }
    }

    [Fact]
    public async Task build_databases_returns_only_enabled_tenants()
    {
        var (store, tenancy) = CreateStore();
        using (store)
        {
            await tenancy.AddDatabaseRecordAsync(TenantA, Db(DbA));
            await tenancy.AddDatabaseRecordAsync(TenantB, Db(DbB));

            (await tenancy.BuildDatabasesAsync()).Count.ShouldBe(2);

            await tenancy.DisableTenantAsync(TenantB);

            // A fresh tenancy reads purely from the master table — no stale cache.
            var (store2, tenancy2) = CreateStore();
            using (store2)
            {
                (await tenancy2.BuildDatabasesAsync()).Count.ShouldBe(1);
            }
        }
    }

    [Fact]
    public async Task disable_then_enable_toggles_routing()
    {
        var (store, tenancy) = CreateStore();
        using (store)
        {
            await tenancy.AddDatabaseRecordAsync(TenantA, Db(DbA));
            await ApplySchemaAsync(tenancy);

            // Routable while enabled.
            await using (var _ = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
            {
            }

            await tenancy.DisableTenantAsync(TenantA);

            (await tenancy.AllDisabledAsync()).ShouldContain(TenantA);
            await Should.ThrowAsync<UnknownTenantIdException>(async () =>
            {
                await using var _ = store.LightweightSession(new SessionOptions { TenantId = TenantA });
            });

            await tenancy.EnableTenantAsync(TenantA);

            (await tenancy.AllDisabledAsync()).ShouldNotContain(TenantA);
            // Routable again.
            await using (var _ = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
            {
            }
        }
    }

    [Fact]
    public async Task delete_record_removes_tenant()
    {
        var (store, tenancy) = CreateStore();
        using (store)
        {
            await tenancy.AddDatabaseRecordAsync(TenantA, Db(DbA));
            (await tenancy.BuildDatabasesAsync()).Count.ShouldBe(1);

            await tenancy.DeleteDatabaseRecordAsync(TenantA);

            (await tenancy.BuildDatabasesAsync()).Count.ShouldBe(0);
            await Should.ThrowAsync<UnknownTenantIdException>(async () =>
            {
                await using var _ = store.LightweightSession(new SessionOptions { TenantId = TenantA });
            });
        }
    }

    [Fact]
    public async Task add_is_idempotent_and_reenables_disabled_tenant()
    {
        var (store, tenancy) = CreateStore();
        using (store)
        {
            await tenancy.AddDatabaseRecordAsync(TenantA, Db(DbA));
            await tenancy.DisableTenantAsync(TenantA);
            (await tenancy.AllDisabledAsync()).ShouldContain(TenantA);

            // Re-adding the same tenant is an upsert that clears the disabled flag.
            await tenancy.AddDatabaseRecordAsync(TenantA, Db(DbA));

            (await tenancy.AllDisabledAsync()).ShouldNotContain(TenantA);
            (await tenancy.BuildDatabasesAsync()).Count.ShouldBe(1);
        }
    }

    public class TestDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }
}
