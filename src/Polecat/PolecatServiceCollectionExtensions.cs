using JasperFx.CommandLine.Descriptions;
using JasperFx.Events;
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
        // #177: register IEventStoreInstrumentation so external tooling (CritterWatch)
        // can resolve it from the container, flip ExtendedProgressionEnabled BEFORE
        // first IDocumentStore resolution, and have the toggle land on the store. The
        // same instance also enters the IConfigurePolecat chain below, which copies
        // the toggle into options.Events.EnableExtendedProgressionTracking on build.
        // Mirrors Marten's SetEventStoreInstrumentation wiring (jasperfx#424).
        var instrument = new SetEventStoreInstrumentation();
        services.AddSingleton<IConfigurePolecat>(instrument);
        services.AddSingleton<IEventStoreInstrumentation>(instrument);

        services.AddSingleton(sp =>
        {
            var options = optionSource(sp);

            var configures = sp.GetServices<IConfigurePolecat>();
            foreach (var configure in configures)
            {
                configure.Configure(sp, options);
            }

            // Apply host-level JasperFxOptions (e.g. EnableAdvancedTracking →
            // Events.EnableExtendedProgressionTracking) before the IDocumentStore
            // singleton is constructed. Runs after the IConfigurePolecat chain so the
            // host opt-in is not clobbered by per-store instrumentation defaults.
            // Mirrors Marten PR #4741.
            options.ReadJasperFxOptions(sp.GetService<JasperFx.JasperFxOptions>());

            return options;
        });

        services.AddSingleton<IDocumentStore>(sp =>
        {
            var options = sp.GetRequiredService<StoreOptions>();
            return new DocumentStore(options);
        });

        // #187: expose the Polecat database(s) to JasperFx's resource model so the
        // idiomatic services.AddResourceSetupOnStartup() provisions the Polecat
        // schema with no Polecat-specific call (matching Marten's MartenSystemPart).
        // The "resources" CLI commands also discover the Polecat database through this.
        services.AddSingleton<ISystemPart>(sp =>
            new PolecatSystemPart(sp.GetRequiredService<IDocumentStore>()));

        // Bridge so monitoring tools (CritterWatch / Wolverine
        // ServiceCapabilities.readDocumentStores) can discover this store via
        // GetServices<IDocumentStoreUsageSource>(). Mirrors Marten's parallel
        // bridge for IEventStore.
        services.AddSingleton<JasperFx.Events.IDocumentStoreUsageSource>(sp =>
            sp.GetRequiredService<IDocumentStore>());

        // wolverine#3219 — and the event-store side of the same bridge. Wolverine's
        // ServiceCapabilities.readEventStores discovers stores via
        // GetServices<JasperFx.Events.IEventStore>(); without this the Polecat store
        // (which implements IEventStore) is invisible to CritterWatch's event-store
        // surfaces, so its event metadata / projections never reach the dashboard.
        // Marten registers IEventStore in its core DI; this brings Polecat to parity.
        services.AddSingleton<JasperFx.Events.IEventStore>(sp =>
            (JasperFx.Events.IEventStore)sp.GetRequiredService<IDocumentStore>());
        services.AddSingleton<JasperFx.Documents.IDocumentStoreDiagnostics>(sp =>
            (JasperFx.Documents.IDocumentStoreDiagnostics)sp.GetRequiredService<IDocumentStore>());

        // Default session factory: lightweight sessions
        services.TryAddSingleton<ISessionFactory>(sp =>
            new DefaultSessionFactory(sp.GetRequiredService<IDocumentStore>()));

        // Scoped sessions resolved through the factory
        services.AddScoped(sp => sp.GetRequiredService<ISessionFactory>().OpenSession());
        services.AddScoped(sp => sp.GetRequiredService<ISessionFactory>().QuerySession());

        // #219: register the activator unconditionally so InitialData seeders run on host startup
        // even without ApplyAllDatabaseChangesOnStartup. #345: it is also Polecat's always-on emit
        // point for the JasperFx application-assembly-reuse warning. StartAsync only logs that warning
        // (when present) and is otherwise a no-op when there is no InitialData and
        // ShouldApplyChangesOnStartup is false, so this is safe for every app.
        PolecatConfigurationExpression.EnsureActivatorIsRegistered(services);

        return new PolecatConfigurationExpression(services);
    }
}

// #177: paired (IConfigurePolecat + IEventStoreInstrumentation) connector classes
// — co-located here per Marten's pattern (SetEventStoreInstrumentation lives next
// to AddMarten / AddMartenStore<T> in MartenServiceCollectionExtensions). External
// tooling resolves IEventStoreInstrumentation from the container and flips
// ExtendedProgressionEnabled; on first store resolution the IConfigurePolecat[<T>]
// chain runs Configure(), which copies the flag onto options.Events.

internal class SetEventStoreInstrumentation : IConfigurePolecat, IEventStoreInstrumentation
{
    public void Configure(IServiceProvider serviceProvider, StoreOptions options)
    {
        options.Events.EnableExtendedProgressionTracking = ExtendedProgressionEnabled;
    }

    public bool ExtendedProgressionEnabled { get; set; }
}

internal class SetEventStoreInstrumentation<T> : IConfigurePolecat<T>, IEventStoreInstrumentation
    where T : IDocumentStore
{
    public void Configure(IServiceProvider serviceProvider, StoreOptions options)
    {
        options.Events.EnableExtendedProgressionTracking = ExtendedProgressionEnabled;
    }

    public bool ExtendedProgressionEnabled { get; set; }
}
