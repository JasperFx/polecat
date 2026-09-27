using JasperFx;
using JasperFx.Descriptors;
using JasperFx.Events;
using Weasel.Core;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     polecat#675 — <c>IEventStore.TryCreateUsage()</c> used to hand-build its
///     <see cref="DatabaseUsage" /> with <c>Cardinality = Single</c> and an empty
///     <see cref="DatabaseUsage.Databases" />, so a database-per-tenant store described itself as
///     single-database while <c>IEventStore.AllDatabases()</c> — reading the same tenancy — returned
///     every tenant. Wolverine's <c>EventStoreAgents.SupportedAgentsAsync</c> enumerates
///     event-subscription agents out of <see cref="DatabaseUsage.Databases" />, so nothing ever
///     advertised — or scheduled — a tenant database's async projections.
/// </summary>
/// <remarks>
///     Pure unit tests: a static tenancy describes itself from configuration alone, and
///     <c>PolecatDatabase.Describe()</c> only parses the connection string. Nothing here connects,
///     which is also the point of the last test — the descriptor is a diagnostic that monitoring
///     tools poll, so it has to answer on a store whose databases are unreachable.
/// </remarks>
public class database_usage_descriptor_tests
{
    private static string ConnectionStringFor(string databaseName) =>
        $"Server=localhost;Database={databaseName};Integrated Security=true;TrustServerCertificate=true;Timeout=1";

    private static DocumentStore BuildStore(Action<StoreOptions> configure)
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionStringFor("usage_main"),
            AutoCreateSchemaObjects = AutoCreate.None,
            DatabaseSchemaName = "usage_cardinality"
        };

        configure(options);
        return new DocumentStore(options);
    }

    private static async Task<DatabaseUsage> DatabaseUsageOf(DocumentStore store)
    {
        var usage = await ((IEventStore)store).TryCreateUsage(CancellationToken.None);
        usage.ShouldNotBeNull();
        usage.Database.ShouldNotBeNull();
        return usage.Database;
    }

    [Fact]
    public async Task a_single_database_store_reports_single_with_a_main_database()
    {
        await using var store = BuildStore(_ => { });

        var database = await DatabaseUsageOf(store);

        database.Cardinality.ShouldBe(DatabaseCardinality.Single);
        database.MainDatabase.ShouldNotBeNull();
        database.MainDatabase!.DatabaseName.ShouldBe("usage_main");

        // Asserted explicitly: "not Single" would also pass on a store that reports garbage, so both
        // shapes are pinned rather than only the one this issue changed.
        database.Databases.ShouldBeEmpty();
    }

    [Fact]
    public async Task a_statically_multi_tenanted_store_reports_every_tenant_database()
    {
        await using var store = BuildStore(opts =>
        {
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("tenant1", ConnectionStringFor("usage_tenant1"));
                tenancy.AddTenant("tenant2", ConnectionStringFor("usage_tenant2"));
            });
        });

        var database = await DatabaseUsageOf(store);

        database.Cardinality.ShouldBe(DatabaseCardinality.StaticMultiple);
        database.Databases.Select(x => x.DatabaseName).OrderBy(x => x)
            .ShouldBe(["usage_tenant1", "usage_tenant2"]);

        // The descriptor and AllDatabases() are the two surfaces that disagreed. Pin that they agree.
        var all = await ((IEventStore)store).AllDatabases();
        database.Databases.Count.ShouldBe(all.Count);
    }

    [Fact]
    public async Task each_described_tenant_database_carries_its_tenant_id()
    {
        await using var store = BuildStore(opts =>
        {
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("tenant1", ConnectionStringFor("usage_tenant1"));
                tenancy.AddTenant("tenant2", ConnectionStringFor("usage_tenant2"));
            });
        });

        var database = await DatabaseUsageOf(store);

        database.Databases.Single(x => x.DatabaseName == "usage_tenant1").TenantIds.ShouldBe(["tenant1"]);
        database.Databases.Single(x => x.DatabaseName == "usage_tenant2").TenantIds.ShouldBe(["tenant2"]);
    }

    [Fact]
    public async Task two_tenants_sharing_one_physical_database_are_described_per_tenant()
    {
        await using var store = BuildStore(opts =>
        {
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("tenant1", ConnectionStringFor("usage_shared"));
                tenancy.AddTenant("tenant2", ConnectionStringFor("usage_shared"));
            });
        });

        var database = await DatabaseUsageOf(store);
        var all = await ((IEventStore)store).AllDatabases();

        // Marten collapses these into one descriptor carrying both tenant ids, because there a
        // database is registered directly. Polecat's SeparateDatabaseTenancy builds a PolecatDatabase
        // per REGISTERED TENANT, so two tenants on one physical database are two databases in
        // AllDatabases(), in the resource model and in the daemon's per-database coordination. The
        // descriptor agreeing with those is the whole point of #675, so it reports two as well.
        all.Count.ShouldBe(2);
        database.Databases.Count.ShouldBe(2);
        database.Databases.Select(x => x.DatabaseName).Distinct().ShouldBe(["usage_shared"]);
        database.Databases.SelectMany(x => x.TenantIds).OrderBy(x => x).ShouldBe(["tenant1", "tenant2"]);
    }

    [Fact]
    public async Task an_unreachable_dynamic_tenancy_still_reports_its_cardinality()
    {
        await using var store = BuildStore(opts =>
        {
            // Nothing listens on this port, so reading the master tenant table fails.
            opts.MultiTenantedMasterTable(
                "Server=localhost,11499;Database=usage_master;User Id=sa;Password=nope;Timeout=1;Encrypt=False");
        });

        var database = await DatabaseUsageOf(store);

        // Honest: "a multi-database store whose tenant set I could not read". The old behaviour
        // answered Single, which is a claim about the store rather than about the failed read.
        database.Cardinality.ShouldBe(DatabaseCardinality.DynamicMultiple);
        database.Databases.ShouldBeEmpty();
    }
}
