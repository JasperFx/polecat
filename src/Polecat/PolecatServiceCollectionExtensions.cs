using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Polecat.Internal;

namespace Polecat;

/// <summary>
///     Extension methods for registering Polecat in an IServiceCollection.
/// </summary>
public static class PolecatServiceCollectionExtensions
{
    /// <summary>
    ///     Add Polecat with inline configuration.
    /// </summary>
    public static PolecatConfigurationExpression AddPolecat(
        this IServiceCollection services, Action<StoreOptions> configure)
    {
        return services.AddPolecat(sp =>
        {
            var options = new StoreOptions();
            configure(options);
            return options;
        });
    }

    /// <summary>
    ///     Add Polecat with just a connection string.
    /// </summary>
    public static PolecatConfigurationExpression AddPolecat(
        this IServiceCollection services, string connectionString)
    {
        return services.AddPolecat(opts => opts.ConnectionString = connectionString);
    }

    /// <summary>
    ///     Add Polecat with a pre-built StoreOptions.
    /// </summary>
    public static PolecatConfigurationExpression AddPolecat(
        this IServiceCollection services, StoreOptions options)
    {
        return services.AddPolecat(_ => options);
    }

    /// <summary>
    ///     Add Polecat with a factory function that receives the IServiceProvider.
    /// </summary>
    public static PolecatConfigurationExpression AddPolecat(
        this IServiceCollection services, Func<IServiceProvider, StoreOptions> optionSource)
    {
        services.AddSingleton(sp =>
        {
            var options = optionSource(sp);
            var configures = sp.GetServices<IConfigurePolecat>();
            foreach (var configure in configures)
            {
                configure.Configure(sp, options);
            }

            return options;
        });

        services.AddSingleton<IDocumentStore>(sp =>
        {
            var options = sp.GetRequiredService<StoreOptions>();
            return new DocumentStore(options);
        });

        // Default session factory: lightweight sessions
        services.TryAddSingleton<ISessionFactory>(sp =>
            new DefaultSessionFactory(sp.GetRequiredService<IDocumentStore>()));

        // Scoped sessions resolved through the factory
        services.AddScoped(sp => sp.GetRequiredService<ISessionFactory>().OpenSession());
        services.AddScoped(sp => sp.GetRequiredService<ISessionFactory>().QuerySession());

        return new PolecatConfigurationExpression(services);
    }
}
