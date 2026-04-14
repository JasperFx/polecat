using Microsoft.Extensions.DependencyInjection;
using Polecat.Tests.Harness;

namespace Polecat.Tests.DependencyInjection;

public interface ILazyTestStore : IDocumentStore;

public class lazy_ancillary_store_registration_tests
{
    [Fact]
    public void add_polecat_store_registers_lazy_of_T()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        services.AddPolecatStore<ILazyTestStore>(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "lazy_test_ancillary";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        // Verify the Lazy<T> service descriptor is registered
        var descriptor = services.FirstOrDefault(d =>
            d.ServiceType == typeof(Lazy<ILazyTestStore>));

        descriptor.ShouldNotBeNull();
        descriptor.Lifetime.ShouldBe(ServiceLifetime.Singleton);
    }

    [Fact]
    public void add_polecat_store_registers_lazy_for_each_ancillary_store()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        services.AddPolecatStore<ILazyTestStore>(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "lazy_test_store1";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        services.AddPolecatStore<IAnotherTestStore>(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "lazy_test_store2";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        services.Any(d => d.ServiceType == typeof(Lazy<ILazyTestStore>)).ShouldBeTrue();
        services.Any(d => d.ServiceType == typeof(Lazy<IAnotherTestStore>)).ShouldBeTrue();
    }
}

public interface IAnotherTestStore : IDocumentStore;
