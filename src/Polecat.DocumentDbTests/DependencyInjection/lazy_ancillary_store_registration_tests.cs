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

    [Fact]
    public void resolves_the_marker_interface_to_a_working_document_store()
    {
        // Regression: AddPolecatStore<T> used to hard-cast a bare DocumentStore to the marker
        // interface T (`(T)(IDocumentStore)store`), which threw InvalidCastException the moment T
        // was actually resolved. The previous tests only asserted the Lazy<T> descriptor existed,
        // so they never resolved T and never caught it. A runtime proxy subclass now implements T.
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        services.AddPolecatStore<ILazyTestStore>(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "resolves_marker_ancillary";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<ILazyTestStore>();
        store.ShouldNotBeNull();
        store.ShouldBeAssignableTo<IDocumentStore>();

        // The same singleton instance flows through the Lazy<T> registration.
        provider.GetRequiredService<Lazy<ILazyTestStore>>().Value.ShouldBeSameAs(store);
    }
}

public interface IAnotherTestStore : IDocumentStore;
