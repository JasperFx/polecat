using JasperFx;
using JasperFx.MultiTenancy;
using Polecat.Storage;
using Polecat.TestUtils;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     #871 item 3 and the first half of #849. <see cref="MasterTableTenancy" /> only ever added to its
///     cache, so a tenant disabled or removed on another node stayed in this node's until it restarted. Two
///     stores over one <c>pc_tenants</c> table are two nodes. Neither runs a daemon, because the tenancy's
///     own answer is what is being pinned. No tenant database is ever opened.
/// </summary>
public class master_table_eviction_tests
{
    private const string TenantId = "evicted";
    private const string Bystander = "bystander";

    private static string TenantConnection(string database) => ConnectionSource.ConnectionStringFor(database);

    private static DocumentStore BuildNode(string schema)
    {
        return DocumentStore.For(opts =>
        {
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.MultiTenantedMasterTable(ConnectionSource.ConnectionString, schema);
        });
    }

    private static MasterTableTenancy TenancyOf(DocumentStore store) => (MasterTableTenancy)store.Options.Tenancy!;

    private static (DocumentStore Here, DocumentStore Elsewhere) BuildNodes()
    {
        var schema = "evict_" + Guid.NewGuid().ToString("N")[..8];
        return (BuildNode(schema), BuildNode(schema));
    }

    private static async Task RegisterAsync(DocumentStore store)
    {
        var token = TestContext.Current.CancellationToken;
        await TenancyOf(store).AddDatabaseRecordAsync(TenantId, TenantConnection("evicted_db"), token);
        await TenancyOf(store).AddDatabaseRecordAsync(Bystander, TenantConnection("bystander_db"), token);
    }

    private static async Task<string[]> KnownDatabasesAsync(DocumentStore store)
    {
        var built = await TenancyOf(store).BuildDatabasesAsync(TestContext.Current.CancellationToken);
        return built.Select(x => x.Identifier).Order().ToArray();
    }

    [Fact]
    public async Task a_tenant_disabled_elsewhere_leaves_this_nodes_cache_on_the_next_read()
    {
        var token = TestContext.Current.CancellationToken;
        var (here, elsewhere) = BuildNodes();
        using (here)
        using (elsewhere)
        {
            await RegisterAsync(here);
            (await KnownDatabasesAsync(here)).Length.ShouldBe(2);

            await TenancyOf(elsewhere).DisableTenantAsync(TenantId, token);

            // Nothing has told this node yet. This is the part that stays node-local: see the remarks on
            // BuildDatabasesAsync, and the docs.
            ((ITenancy)TenancyOf(here)).GetDatabase(TenantId).ShouldNotBeNull();

            (await KnownDatabasesAsync(here)).ShouldBe([$"Polecat_{Bystander}"]);
            ((ITenancy)TenancyOf(here)).AllDatabases().ShouldHaveSingleItem();

            Should.Throw<DisabledTenantException>(() => ((ITenancy)TenancyOf(here)).GetDatabase(TenantId));
        }
    }

    [Fact]
    public async Task a_tenant_removed_elsewhere_leaves_this_nodes_cache_on_the_next_read()
    {
        var token = TestContext.Current.CancellationToken;
        var (here, elsewhere) = BuildNodes();
        using (here)
        using (elsewhere)
        {
            await RegisterAsync(here);
            (await KnownDatabasesAsync(here)).Length.ShouldBe(2);

            await TenancyOf(elsewhere).DeleteDatabaseRecordAsync(TenantId, token);

            (await KnownDatabasesAsync(here)).ShouldBe([$"Polecat_{Bystander}"]);

            var refused = Should.Throw<UnknownTenantIdException>(
                () => ((ITenancy)TenancyOf(here)).GetDatabase(TenantId));
            refused.ShouldNotBeOfType<DisabledTenantException>();
        }
    }

    [Fact]
    public async Task a_tenant_enabled_again_comes_back()
    {
        var token = TestContext.Current.CancellationToken;
        var (here, elsewhere) = BuildNodes();
        using (here)
        using (elsewhere)
        {
            await RegisterAsync(here);
            await TenancyOf(elsewhere).DisableTenantAsync(TenantId, token);
            (await KnownDatabasesAsync(here)).ShouldBe([$"Polecat_{Bystander}"]);

            await TenancyOf(elsewhere).EnableTenantAsync(TenantId, token);

            (await KnownDatabasesAsync(here)).ShouldBe([$"Polecat_{Bystander}", $"Polecat_{TenantId}"]);
        }
    }

    /// <summary>
    ///     A tenant that stayed keeps its cached database. The coordinator's daemons, and any session
    ///     already holding one, are keyed to that instance.
    /// </summary>
    [Fact]
    public async Task an_untouched_tenant_keeps_its_cached_database()
    {
        var token = TestContext.Current.CancellationToken;
        var (here, elsewhere) = BuildNodes();
        using (here)
        using (elsewhere)
        {
            await RegisterAsync(here);
            var cached = ((ITenancy)TenancyOf(here)).GetDatabase(Bystander);

            await TenancyOf(elsewhere).DisableTenantAsync(TenantId, token);
            await KnownDatabasesAsync(here);

            ((ITenancy)TenancyOf(here)).GetDatabase(Bystander).ShouldBeSameAs(cached);
        }
    }
}
