using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Internal;
using Polecat.Tests.Harness;

namespace Polecat.Tests.DependencyInjection;

public class service_registration_tests
{
    private static ServiceProvider BuildProvider(Action<PolecatConfigurationExpression>? configure = null)
    {
        var services = new ServiceCollection();
        var expression = services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
        });
        configure?.Invoke(expression);
        return services.BuildServiceProvider();
    }

    [Fact]
    public void registers_store_options_as_singleton()
    {
        using var provider = BuildProvider();
        var options1 = provider.GetRequiredService<StoreOptions>();
        var options2 = provider.GetRequiredService<StoreOptions>();
        options1.ShouldBeSameAs(options2);
    }

    [Fact]
    public void registers_document_store_as_singleton()
    {
        using var provider = BuildProvider();
        var store1 = provider.GetRequiredService<IDocumentStore>();
        var store2 = provider.GetRequiredService<IDocumentStore>();
        store1.ShouldBeSameAs(store2);
        store1.ShouldBeOfType<DocumentStore>();
    }

    [Fact]
    public void registers_session_factory_as_singleton()
    {
        using var provider = BuildProvider();
        var factory1 = provider.GetRequiredService<ISessionFactory>();
        var factory2 = provider.GetRequiredService<ISessionFactory>();
        factory1.ShouldBeSameAs(factory2);
    }

    [Fact]
    public void default_session_factory_is_lightweight()
    {
        using var provider = BuildProvider();
        var factory = provider.GetRequiredService<ISessionFactory>();
        factory.ShouldBeOfType<DefaultSessionFactory>();

        // Default sessions should be lightweight (no identity map)
        var session = factory.OpenSession();
        session.ShouldBeOfType<LightweightSession>();
    }

    [Fact]
    public async Task resolves_document_session_as_scoped()
    {
        await using var provider = BuildProvider();
        await using var scope1 = provider.CreateAsyncScope();
        await using var scope2 = provider.CreateAsyncScope();

        var session1a = scope1.ServiceProvider.GetRequiredService<IDocumentSession>();
        var session1b = scope1.ServiceProvider.GetRequiredService<IDocumentSession>();
        var session2 = scope2.ServiceProvider.GetRequiredService<IDocumentSession>();

        // Same within scope
        session1a.ShouldBeSameAs(session1b);
        // Different across scopes
        session1a.ShouldNotBeSameAs(session2);
    }

    [Fact]
    public async Task resolves_query_session_as_scoped()
    {
        await using var provider = BuildProvider();
        await using var scope1 = provider.CreateAsyncScope();
        await using var scope2 = provider.CreateAsyncScope();

        var session1a = scope1.ServiceProvider.GetRequiredService<IQuerySession>();
        var session1b = scope1.ServiceProvider.GetRequiredService<IQuerySession>();
        var session2 = scope2.ServiceProvider.GetRequiredService<IQuerySession>();

        // Same within scope
        session1a.ShouldBeSameAs(session1b);
        // Different across scopes
        session1a.ShouldNotBeSameAs(session2);
    }

    [Fact]
    public void use_lightweight_sessions_explicitly()
    {
        using var provider = BuildProvider(expr => expr.UseLightweightSessions());
        var factory = provider.GetRequiredService<ISessionFactory>();

        var session = factory.OpenSession();
        session.ShouldBeOfType<LightweightSession>();
    }

    [Fact]
    public void use_identity_sessions()
    {
        using var provider = BuildProvider(expr => expr.UseIdentitySessions());
        var factory = provider.GetRequiredService<ISessionFactory>();

        var session = factory.OpenSession();
        session.ShouldBeOfType<IdentityMapDocumentSession>();
    }

    [Fact]
    public void add_polecat_with_connection_string()
    {
        var services = new ServiceCollection();
        services.AddPolecat(ConnectionSource.ConnectionString);

        using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<IDocumentStore>();
        store.Options.ConnectionString.ShouldBe(ConnectionSource.ConnectionString);
    }

    [Fact]
    public void add_polecat_with_pre_built_options()
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionSource.ConnectionString,
            DatabaseSchemaName = "custom"
        };

        var services = new ServiceCollection();
        services.AddPolecat(options);

        using var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<StoreOptions>();
        resolved.ShouldBeSameAs(options);
        resolved.DatabaseSchemaName.ShouldBe("custom");
    }

    [Fact]
    public void add_polecat_with_service_provider_factory()
    {
        var services = new ServiceCollection();
        services.AddSingleton("test-connection-marker");
        services.AddPolecat(sp =>
        {
            // Prove that the IServiceProvider is available
            sp.GetRequiredService<string>().ShouldBe("test-connection-marker");
            return new StoreOptions
            {
                ConnectionString = ConnectionSource.ConnectionString
            };
        });

        using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<IDocumentStore>();
        store.ShouldNotBeNull();
    }

    [Fact]
    public void configure_polecat_applies_post_configuration()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
        });

        services.ConfigurePolecat(opts =>
        {
            opts.CommandTimeout = 120;
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<StoreOptions>();
        options.CommandTimeout.ShouldBe(120);
    }

    [Fact]
    public void iconfigure_polecat_is_applied()
    {
        var services = new ServiceCollection();
        services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
        });
        services.AddSingleton<IConfigurePolecat>(new TestConfigurePolecat());

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<StoreOptions>();
        options.DatabaseSchemaName.ShouldBe("configured_by_test");
    }

    [Fact]
    public void apply_all_database_changes_registers_hosted_service()
    {
        using var provider = BuildProvider(expr => expr.ApplyAllDatabaseChangesOnStartup());

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        hostedServices.OfType<PolecatActivator>().ShouldHaveSingleItem();
    }

    [Fact]
    public void apply_all_database_changes_sets_flag_on_options()
    {
        using var provider = BuildProvider(expr => expr.ApplyAllDatabaseChangesOnStartup());

        var options = provider.GetRequiredService<StoreOptions>();
        options.ShouldApplyChangesOnStartup.ShouldBeTrue();
    }

    [Fact]
    public void calling_apply_twice_registers_only_one_hosted_service()
    {
        using var provider = BuildProvider(expr =>
        {
            expr.ApplyAllDatabaseChangesOnStartup();
            expr.ApplyAllDatabaseChangesOnStartup();
        });

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        hostedServices.OfType<PolecatActivator>().Count().ShouldBe(1);
    }

    private class TestConfigurePolecat : IConfigurePolecat
    {
        public void Configure(IServiceProvider serviceProvider, StoreOptions options)
        {
            options.DatabaseSchemaName = "configured_by_test";
        }
    }
}
