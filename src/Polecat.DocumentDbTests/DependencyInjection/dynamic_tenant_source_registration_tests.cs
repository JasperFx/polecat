using JasperFx.MultiTenancy;
using Microsoft.Extensions.DependencyInjection;
using Polecat.Storage;
using Polecat.TestUtils;

namespace Polecat.Tests.DependencyInjection;

public interface IBug1144Store : IDocumentStore;

/// <summary>
///     #377: implementing IDynamicTenantSource&lt;string&gt; is only half the story — CritterWatch
///     resolves it with GetServices&lt;IDynamicTenantSource&lt;string&gt;&gt;() and degrades to a
///     read-only tenant list when the collection is empty. So the registration has to exist for a
///     dynamic tenancy and must *not* exist for every other tenancy model.
/// </summary>
public class dynamic_tenant_source_registration_tests
{
    [Fact]
    public void master_table_tenancy_registers_the_dynamic_source()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.MultiTenantedMasterTable(ConnectionSource.ConnectionString);
        });

        using var provider = services.BuildServiceProvider();

        var sources = provider.GetServices<IDynamicTenantSource<string>>().ToList();
        sources.Count.ShouldBe(1);
        var store = (DocumentStore)provider.GetRequiredService<IDocumentStore>();
        sources[0].ShouldBeOfType<MasterTableTenancy>().ShouldBeSameAs(store.Options.Tenancy);
    }

    [Fact]
    public void master_table_tenancy_registers_the_dynamic_source_from_prebuilt_options()
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };
        options.MultiTenantedMasterTable(ConnectionSource.ConnectionString);

        var services = new ServiceCollection();
        services.AddPolecat(options);

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IDynamicTenantSource<string>>().ShouldHaveSingleItem();
    }

    [Fact]
    public void single_database_store_registers_no_dynamic_source()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IDynamicTenantSource<string>>().ShouldBeEmpty();
    }

    [Fact]
    public void static_separate_database_tenancy_registers_no_dynamic_source()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.MultiTenantedDatabases(x => x.AddTenant("one", ConnectionSource.ConnectionString));
        });

        using var provider = services.BuildServiceProvider();

        // Static tenancy has no runtime lifecycle, so the collection stays empty and consumers
        // correctly show a read-only tenant list.
        provider.GetServices<IDynamicTenantSource<string>>().ShouldBeEmpty();
    }

    // #1144 (marten#6190): the forms whose tenancy isn't known when they're registered opt in with
    // ExposeDynamicTenantSource(). A lazy registration for every store can't work instead: the container keeps a
    // null a factory returns in GetServices(), so a store that isn't dynamic would hand consumers a null.

    private static void masterTable(StoreOptions opts)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.MultiTenantedMasterTable(ConnectionSource.ConnectionString);
    }

    [Fact]
    public void the_service_provider_form_exposes_its_dynamic_tenancy_when_asked()
    {
        var services = new ServiceCollection();
        var connectionString = ConnectionSource.ConnectionString;
        var controlPlaneConnectionString = ConnectionSource.ConnectionString;

        #region sample_polecat_expose_dynamic_tenant_source

        services.AddPolecat(sp =>
        {
            var opts = new StoreOptions { ConnectionString = connectionString };
            opts.MultiTenantedMasterTable(controlPlaneConnectionString);
            return opts;
        })
        // Polecat can't see this tenancy until the store is built, so ask for it
        .ExposeDynamicTenantSource();

        #endregion

        using var provider = services.BuildServiceProvider();

        var source = provider.GetServices<IDynamicTenantSource<string>>().ShouldHaveSingleItem();
        source.ShouldBeOfType<MasterTableTenancy>()
            .ShouldBeSameAs(((DocumentStore)provider.GetRequiredService<IDocumentStore>()).Options.Tenancy);
    }

    [Fact]
    public void tenancy_set_through_configure_polecat_is_exposed_when_asked()
    {
        var services = new ServiceCollection();
        services.AddPolecat(ConnectionSource.ConnectionString).ExposeDynamicTenantSource();
        services.ConfigurePolecat(masterTable);

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IDynamicTenantSource<string>>().ShouldHaveSingleItem()
            .ShouldBeOfType<MasterTableTenancy>();
    }

    [Fact]
    public void an_ancillary_store_exposes_its_dynamic_tenancy_when_asked()
    {
        var services = new ServiceCollection();
        services.AddPolecatStore<IBug1144Store>(masterTable).ExposeDynamicTenantSource();

        using var provider = services.BuildServiceProvider();

        var source = provider.GetServices<IDynamicTenantSource<string>>().ShouldHaveSingleItem();
        source.ShouldBeSameAs(((DocumentStore)provider.GetRequiredService<IBug1144Store>()).Options.Tenancy);
    }

    [Fact]
    public void an_ancillary_store_that_does_not_ask_exposes_nothing()
    {
        var services = new ServiceCollection();
        services.AddPolecatStore<IBug1144Store>(masterTable);

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IDynamicTenantSource<string>>().ShouldBeEmpty();
    }

    [Fact]
    public void asking_where_the_store_already_exposes_it_registers_it_once()
    {
        var services = new ServiceCollection();
        services.AddPolecat(masterTable).ExposeDynamicTenantSource().ExposeDynamicTenantSource();

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IDynamicTenantSource<string>>().ShouldHaveSingleItem();
    }

    [Fact]
    public void asking_for_a_store_whose_tenancy_is_not_dynamic_says_so()
    {
        var services = new ServiceCollection();
        services.AddPolecat(ConnectionSource.ConnectionString).ExposeDynamicTenantSource();

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(() =>
            provider.GetServices<IDynamicTenantSource<string>>().ToList());
        ex.Message.ShouldContain("ExposeDynamicTenantSource()");
    }
}
