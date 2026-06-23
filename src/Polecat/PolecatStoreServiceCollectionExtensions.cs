using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
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
        // #177: register IEventStoreInstrumentation alongside the per-T IConfigurePolecat<T>
        // chain so external tooling can resolve every store's instrumentation through
        // GetServices<IEventStoreInstrumentation>() and toggle them in one pass. Mirrors
        // SetEventStoreInstrumentation<T> wiring in Marten's AddMartenStore<T>.
        var instrument = new SetEventStoreInstrumentation<T>();
        services.AddSingleton<IConfigurePolecat<T>>(instrument);
        services.AddSingleton<IEventStoreInstrumentation>(instrument);

        services.AddSingleton<T>(sp =>
        {
            var options = optionSource(sp);

            // polecat#207: give this ancillary store a distinct logical StoreName (its marker type) so its
            // IEventStore.Identity / usage descriptor are distinguishable from the primary and other
            // ancillary stores. Mirrors Marten's SecondaryStoreConfig (options.StoreName = typeof(T).Name).
            // Set before the IConfigurePolecat<T> chain so a user override still wins.
            options.StoreName = typeof(T).Name;

            var configures = sp.GetServices<IConfigurePolecat<T>>();
            foreach (var configure in configures)
            {
                configure.Configure(sp, options);
            }

            // Apply host-level JasperFxOptions (e.g. EnableAdvancedTracking →
            // Events.EnableExtendedProgressionTracking) before the ancillary store is
            // constructed. Runs after the IConfigurePolecat<T> chain so the host opt-in
            // is not clobbered by per-store instrumentation defaults. Mirrors Marten PR #4741.
            options.ReadJasperFxOptions(sp.GetService<JasperFx.JasperFxOptions>());

            // T is a user-supplied marker interface (T : IDocumentStore) that the concrete
            // DocumentStore does NOT implement, so a direct (T)store cast would throw
            // InvalidCastException. Build a thin runtime subclass `class TImplementation :
            // DocumentStore, T` and instantiate that instead. Mirrors Marten's
            // SecondaryStoreConfig<T>.Build / SecondaryStoreProxyFactory.
            var storeType = SecondaryStoreProxyFactory.GetOrCreate(typeof(T));
            return (T)Activator.CreateInstance(storeType, options)!;
        });

        services.AddSingleton<Lazy<T>>(sp => new Lazy<T>(() => sp.GetRequiredService<T>()));

        // #187: contribute this ancillary store's database(s) to JasperFx's resource
        // model so AddResourceSetupOnStartup() / the "resources" CLI provision its
        // schema. Mirrors AddMartenStore<T>'s MartenSystemPart<T> registration.
        services.AddSingleton<JasperFx.CommandLine.Descriptions.ISystemPart>(sp =>
            new PolecatSystemPart<T>(sp.GetRequiredService<T>()));

        // Bridge so monitoring tools discover the ancillary store via
        // GetServices<IDocumentStoreUsageSource>(). Mirrors the bridge in
        // PolecatServiceCollectionExtensions.AddPolecat for the primary store.
        services.AddSingleton<JasperFx.Events.IDocumentStoreUsageSource>(sp =>
            sp.GetRequiredService<T>());
        services.AddSingleton<JasperFx.Documents.IDocumentStoreDiagnostics>(sp =>
            (JasperFx.Documents.IDocumentStoreDiagnostics)sp.GetRequiredService<T>());

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
