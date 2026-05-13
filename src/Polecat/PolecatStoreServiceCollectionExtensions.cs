using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Polecat.Internal;

namespace Polecat;

/// <summary>
///     Extension methods for registering secondary/ancillary Polecat document stores.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2091:DynamicallyAccessedMembers",
    Justification = "Class-level: AddPolecatStore<T> threads T through Microsoft.Extensions.DependencyInjection registration, which has its own DAM annotations. T is constrained to interface IDocumentStore and supplied directly by caller code at registration time, so the trimmer sees and preserves it.")]
public static class PolecatStoreServiceCollectionExtensions
{
    /// <summary>
    ///     Add a secondary IDocumentStore service to the container using only
    ///     an interface "T" that should directly inherit from IDocumentStore.
    /// </summary>
    public static PolecatStoreExpression<T> AddPolecatStore<T>(
        this IServiceCollection services, Action<StoreOptions> configure)
        where T : class, IDocumentStore
    {
        return services.AddPolecatStore<T>(sp =>
        {
            var options = new StoreOptions();
            configure(options);
            return options;
        });
    }

    /// <summary>
    ///     Add a secondary IDocumentStore service to the container using only
    ///     an interface "T" that should directly inherit from IDocumentStore.
    /// </summary>
    public static PolecatStoreExpression<T> AddPolecatStore<T>(
        this IServiceCollection services, Func<IServiceProvider, StoreOptions> optionSource)
        where T : class, IDocumentStore
    {
        services.AddSingleton<T>(sp =>
        {
            var options = optionSource(sp);

            var configures = sp.GetServices<IConfigurePolecat<T>>();
            foreach (var configure in configures)
            {
                configure.Configure(sp, options);
            }

            var store = new DocumentStore(options);
            return (T)(IDocumentStore)store;
        });

        services.AddSingleton<Lazy<T>>(sp => new Lazy<T>(() => sp.GetRequiredService<T>()));

        // Bridge so monitoring tools discover the ancillary store via
        // GetServices<IDocumentStoreUsageSource>(). Mirrors the bridge in
        // PolecatServiceCollectionExtensions.AddPolecat for the primary store.
        services.AddSingleton<JasperFx.Events.IDocumentStoreUsageSource>(sp =>
            sp.GetRequiredService<T>());

        return new PolecatStoreExpression<T>(services);
    }

    /// <summary>
    ///     Register a post-configuration action for StoreOptions on a specific store type.
    /// </summary>
    public static IServiceCollection ConfigurePolecat<T>(
        this IServiceCollection services, Action<StoreOptions> configure)
        where T : IDocumentStore
    {
        services.AddSingleton<IConfigurePolecat<T>>(
            new LambdaConfigurePolecat<T>((_, opts) => configure(opts)));
        return services;
    }

    /// <summary>
    ///     Register a post-configuration action for StoreOptions on a specific store type
    ///     that has access to the service provider.
    /// </summary>
    public static IServiceCollection ConfigurePolecat<T>(
        this IServiceCollection services, Action<IServiceProvider, StoreOptions> configure)
        where T : IDocumentStore
    {
        services.AddSingleton<IConfigurePolecat<T>>(new LambdaConfigurePolecat<T>(configure));
        return services;
    }
}

/// <summary>
///     Fluent builder returned by AddPolecatStore&lt;T&gt;() for further configuration.
/// </summary>
public class PolecatStoreExpression<T> where T : class, IDocumentStore
{
    public PolecatStoreExpression(IServiceCollection services)
    {
        Services = services;
    }

    public IServiceCollection Services { get; }
}

internal class LambdaConfigurePolecat<T> : IConfigurePolecat<T> where T : IDocumentStore
{
    private readonly Action<IServiceProvider, StoreOptions> _configure;

    public LambdaConfigurePolecat(Action<IServiceProvider, StoreOptions> configure)
    {
        _configure = configure;
    }

    public void Configure(IServiceProvider serviceProvider, StoreOptions options)
    {
        _configure(serviceProvider, options);
    }
}
