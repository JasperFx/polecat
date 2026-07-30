using JasperFx.MultiTenancy;
using Microsoft.Extensions.DependencyInjection;
using Polecat.Storage;
using Polecat.TestUtils;

namespace Polecat.Tests.DependencyInjection;

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
}
