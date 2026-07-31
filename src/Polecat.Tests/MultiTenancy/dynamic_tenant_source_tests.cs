using JasperFx;
using JasperFx.Descriptors;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Storage;
using Polecat.TestUtils;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     #377: MasterTableTenancy declares JasperFx.MultiTenancy.IDynamicTenantSource&lt;string&gt; so
///     store-agnostic admin tooling (CritterWatch) can drive the runtime tenant lifecycle without
///     referencing Polecat's concrete tenancy types. These tests exercise the tenancy *only* through
///     that abstraction — every call goes through the interface reference, never the concrete type.
/// </summary>
public class dynamic_tenant_source_tests : IAsyncLifetime
{
    private const string TenantA = "tenant_a";
    private const string TenantB = "tenant_b";
    private static readonly string ControlDb = ConnectionSource.Scoped("dts_control");
    private static readonly string DbA = ConnectionSource.Scoped("dts_tenant_a");
    private static readonly string DbB = ConnectionSource.Scoped("dts_tenant_b");

    private static readonly string MasterConnectionString = ConnectionSource.MasterConnectionString;

    private static string Db(string name) =>
        ConnectionSource.ConnectionStringFor(name);

    public async ValueTask InitializeAsync()
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

    public async ValueTask DisposeAsync()
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

    private (DocumentStore Store, IDynamicTenantSource<string> Source) CreateStore()
    {
        MasterTableTenancy? tenancy = null;
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = Db(ControlDb);
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            tenancy = opts.MultiTenantedMasterTable(Db(ControlDb));
        });

        // The whole point of #377: the concrete tenancy *is* the abstraction.
        return (store, tenancy!.ShouldBeAssignableTo<IDynamicTenantSource<string>>()!);
    }

    [Fact]
    public void master_table_tenancy_is_a_dynamic_tenant_source()
    {
        var (store, source) = CreateStore();
        using (store)
        {
            store.Options.Tenancy.ShouldBeAssignableTo<IDynamicTenantSource<string>>();
            source.Cardinality.ShouldBe(DatabaseCardinality.DynamicMultiple);
        }
    }

    [Fact]
    public async Task add_tenant_then_find_returns_the_connection_string()
    {
        var (store, source) = CreateStore();
        using (store)
        {
            await source.AddTenantAsync(TenantA, Db(DbA));

            (await source.FindAsync(TenantA)).ShouldBe(Db(DbA));
        }
    }

    [Fact]
    public async Task find_unknown_tenant_throws()
    {
        var (store, source) = CreateStore();
        using (store)
        {
            await Should.ThrowAsync<UnknownTenantIdException>(async () => await source.FindAsync("nope"));
        }
    }

    [Fact]
    public async Task disable_and_enable_round_trip_through_the_abstraction()
    {
        var (store, source) = CreateStore();
        using (store)
        {
            await source.AddTenantAsync(TenantA, Db(DbA));

            await source.DisableTenantAsync(TenantA);

            (await source.AllDisabledAsync()).ShouldContain(TenantA);
            await Should.ThrowAsync<UnknownTenantIdException>(async () => await source.FindAsync(TenantA));

            await source.EnableTenantAsync(TenantA);

            (await source.AllDisabledAsync()).ShouldNotContain(TenantA);
            (await source.FindAsync(TenantA)).ShouldBe(Db(DbA));
        }
    }

    [Fact]
    public async Task remove_tenant_deletes_the_registry_record()
    {
        var (store, source) = CreateStore();
        using (store)
        {
            await source.AddTenantAsync(TenantA, Db(DbA));

            await source.RemoveTenantAsync(TenantA);

            await Should.ThrowAsync<UnknownTenantIdException>(async () => await source.FindAsync(TenantA));
            (await source.AllDisabledAsync()).ShouldNotContain(TenantA);

            // A fresh tenancy reading purely from the master table agrees — the row is gone, not
            // merely evicted from the in-memory cache.
            var (store2, source2) = CreateStore();
            using (store2)
            {
                await Should.ThrowAsync<UnknownTenantIdException>(async () => await source2.FindAsync(TenantA));
            }
        }
    }

    [Fact]
    public async Task refresh_reloads_the_active_tenants()
    {
        var (store, source) = CreateStore();
        using (store)
        {
            await source.AddTenantAsync(TenantA, Db(DbA));
            await source.AddTenantAsync(TenantB, Db(DbB));

            await source.RefreshAsync();

            source.AllActiveByTenant().Select(x => x.TenantId)
                .OrderBy(x => x, StringComparer.Ordinal)
                .ShouldBe([TenantA, TenantB]);
            source.AllActive().Count.ShouldBe(2);

            // A disabled tenant drops out of the active set on the next refresh.
            await source.DisableTenantAsync(TenantB);
            await source.RefreshAsync();

            source.AllActiveByTenant().Select(x => x.TenantId).ShouldBe([TenantA]);
        }
    }

    [Fact]
    public async Task auto_assign_overload_is_not_supported()
    {
        var (store, source) = CreateStore();
        using (store)
        {
            // Database-per-tenant has no pool to assign from, so the jasperfx#413 auto-assign
            // overload keeps its default NotSupportedException. CritterWatch relies on that to
            // require a caller-supplied connection string for this tenancy model.
            await Should.ThrowAsync<NotSupportedException>(async () =>
                await source.AddTenantAsync(TenantA, TestContext.Current.CancellationToken));
        }
    }
}
