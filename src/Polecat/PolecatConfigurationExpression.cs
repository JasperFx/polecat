using System.Diagnostics.CodeAnalysis;
using JasperFx.Events.Daemon;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polecat.Internal;

namespace Polecat;

/// <summary>
///     Fluent builder returned by AddPolecat() for further configuration.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2087:DynamicallyAccessedMembers",
    Justification = "Class-level: generic type-argument flow on AddPolecat()'s subsequent fluent calls — TStore / TQuerySession / TDocumentSession etc. flow in from caller registration code and are preserved by the trimmer at that boundary per the AOT publishing guide.")]
public class PolecatConfigurationExpression
{
    public PolecatConfigurationExpression(IServiceCollection services)
    {
        Services = services;
    }

    public IServiceCollection Services { get; }

    /// <summary>
    ///     Take the store's connection string from the <see cref="Microsoft.Data.SqlClient.SqlConnection" />
    ///     registered in the container, which is what Aspire's <c>AddSqlServerClient</c> (or
    ///     <c>AddKeyedSqlServerClient</c>, with <paramref name="serviceKey" />) provides. Runs when the store
    ///     is first built, after any <c>ConnectionString</c> set in the configuration lambda, and replaces it.
    ///     The connection is resolved in a scope, only read and never opened.
    /// </summary>
    /// <remarks>
    ///     SqlClient has no public <c>DbDataSource</c>, so the connection string is the shareable part.
    /// </remarks>
    public PolecatConfigurationExpression UseSqlConnectionFromServices(object? serviceKey = null)
    {
        Services.AddSingleton<IConfigurePolecat>(new SqlConnectionFromServices(serviceKey));
        return this;
    }

    private sealed class SqlConnectionFromServices(object? serviceKey) : IConfigurePolecat
    {
        public void Configure(IServiceProvider services, StoreOptions options)
        {
            using var scope = services.CreateScope();
            var connection = serviceKey == null
                ? scope.ServiceProvider.GetRequiredService<Microsoft.Data.SqlClient.SqlConnection>()
                : scope.ServiceProvider.GetRequiredKeyedService<Microsoft.Data.SqlClient.SqlConnection>(serviceKey);

            options.Connection(connection);
        }
    }

    /// <summary>
    ///     Use lightweight sessions (no identity map) as the default.
    ///     This is already the default behavior.
    /// </summary>
    public PolecatConfigurationExpression UseLightweightSessions()
    {
        return BuildSessionsWith<LightweightSessionFactory>();
    }

    /// <summary>
    ///     Use identity map sessions as the default.
    /// </summary>
    public PolecatConfigurationExpression UseIdentitySessions()
    {
        return BuildSessionsWith<IdentitySessionFactory>();
    }

    /// <summary>
    ///     Register a custom session factory.
    /// </summary>
    public PolecatConfigurationExpression BuildSessionsWith<T>(
        ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where T : class, ISessionFactory
    {
        Services.Add(new ServiceDescriptor(typeof(ISessionFactory), typeof(T), lifetime));
        return this;
    }

    /// <summary>
    ///     Run the async projection daemon in this host, through the projection coordinator. Mirrors
    ///     Marten's <c>AddAsyncDaemon</c>.
    /// </summary>
    /// <remarks>
    ///     <see cref="DaemonMode.Solo" /> runs every shard here and takes no lock.
    ///     <see cref="DaemonMode.HotCold" /> runs a shard on one node at a time, with failover. In both, a
    ///     paused shard is restarted and a tenant added to, disabled in or removed from a master table is
    ///     picked up on the leadership cycle. <see cref="DaemonMode.Disabled" /> and
    ///     <see cref="DaemonMode.ExternallyManaged" /> host nothing.
    /// </remarks>
    public PolecatConfigurationExpression AddAsyncDaemon(DaemonMode mode)
    {
        Services.ConfigurePolecat(opts => opts.DaemonSettings.AsyncMode = mode);
        EnsureActivatorIsRegistered();

        if (mode is not (DaemonMode.Solo or DaemonMode.HotCold))
        {
            return this;
        }

        registerCoordinator(soloTakesNoLocks: true);

        // #847: kept for Daemon and Daemons. The coordinator is the hosted service, so this isn't one.
        if (Services.All(x => x.ServiceType != typeof(PolecatDaemonHostedService)))
        {
            Services.AddSingleton<PolecatDaemonHostedService>(sp => new PolecatDaemonHostedService(
                (Polecat.Events.Daemon.Coordination.ProjectionCoordinator)sp
                    .GetRequiredService<Polecat.Events.Daemon.Coordination.IProjectionCoordinator>()));
        }

        return this;
    }

    /// <summary>
    ///     Enable the multi-node-aware projection coordinator. Each Polecat node
    ///     races for SQL Server application locks (<c>sp_getapplock</c>) per shard
    ///     (single-tenant) or per database (multi-tenant) so a given shard runs
    ///     on exactly one node at a time. Equivalent to Marten's hot-cold daemon
    ///     coordination.
    /// </summary>
    /// <remarks>
    ///     <see cref="AddAsyncDaemon"/> registers the same coordinator, so calling both still hosts one.
    ///     The difference is <see cref="DaemonMode.Solo" />, where this takes the locks anyway and
    ///     <see cref="AddAsyncDaemon"/> takes none. Whichever is called first decides that.
    /// </remarks>
    public PolecatConfigurationExpression AddProjectionCoordinator(DaemonMode mode)
    {
        Services.ConfigurePolecat(opts => opts.DaemonSettings.AsyncMode = mode);
        EnsureActivatorIsRegistered();
        registerCoordinator(soloTakesNoLocks: false);
        return this;
    }

    // marten#5056: a second IHostedService forwarding to the one coordinator would have the host stop it
    // twice at shutdown, so a repeated registration, by either method, adds nothing.
    private void registerCoordinator(bool soloTakesNoLocks)
    {
        if (Services.Any(x => x.ServiceType == typeof(Polecat.Events.Daemon.Coordination.IProjectionCoordinator)))
        {
            return;
        }

        Services.AddSingleton<Polecat.Events.Daemon.Coordination.IProjectionCoordinator>(sp =>
        {
            var store = (DocumentStore)sp.GetRequiredService<IDocumentStore>();
            return new Polecat.Events.Daemon.Coordination.ProjectionCoordinator(
                store, sp.GetRequiredService<ILoggerFactory>(), soloTakesNoLocks);
        });
        Services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<Polecat.Events.Daemon.Coordination.IProjectionCoordinator>());
        // jasperfx#430 — also resolve the JasperFx.Events base interface, so a stock host can
        // GetService<JasperFx.Events.Daemon.IProjectionCoordinator>() directly instead of walking
        // the registered IHostedServices for one.
        Services.AddSingleton<JasperFx.Events.Daemon.IProjectionCoordinator>(sp =>
            sp.GetRequiredService<Polecat.Events.Daemon.Coordination.IProjectionCoordinator>());
    }

    /// <summary>
    ///     Marker-typed variant of <see cref="AddProjectionCoordinator"/> for
    ///     ancillary store registrations (multi-store apps where each store
    ///     gets its own coordinator).
    /// </summary>
    public PolecatConfigurationExpression AddProjectionCoordinator<T>(DaemonMode mode)
        where T : class, IDocumentStore
    {
        EnsureActivatorIsRegistered();
        new PolecatStoreExpression<T>(Services).AddProjectionCoordinator(mode);
        return this;
    }

    /// <summary>
    ///     Apply all database schema changes on application startup.
    ///     Registers an IHostedService that runs Weasel schema migration.
    /// </summary>
    public PolecatConfigurationExpression ApplyAllDatabaseChangesOnStartup()
    {
        EnsureActivatorIsRegistered();
        Services.ConfigurePolecat(opts => opts.ShouldApplyChangesOnStartup = true);
        return this;
    }

    private void EnsureActivatorIsRegistered() => EnsureActivatorIsRegistered(Services);

    /// <summary>
    ///     Registers the PolecatActivator hosted service exactly once. The activator applies schema
    ///     changes on startup (when ApplyAllDatabaseChangesOnStartup is set) and ALWAYS runs the
    ///     configured InitialData seeders — so #219: it is registered unconditionally by AddPolecat
    ///     so that InitialData runs even when the app never calls ApplyAllDatabaseChangesOnStartup /
    ///     AddAsyncDaemon / AddProjectionCoordinator.
    /// </summary>
    internal static void EnsureActivatorIsRegistered(IServiceCollection services)
    {
        if (services.Any(x =>
                x.ServiceType == typeof(IHostedService) &&
                x.ImplementationType == typeof(PolecatActivator)))
        {
            return;
        }

        services.Insert(0,
            new ServiceDescriptor(typeof(IHostedService), typeof(PolecatActivator),
                ServiceLifetime.Singleton));
    }
}

/// <summary>
///     Extension methods for IServiceCollection to configure Polecat options after registration.
/// </summary>
public static class PolecatServiceConfigurationExtensions
{
    /// <summary>
    ///     Register a post-configuration action for StoreOptions.
    /// </summary>
    public static IServiceCollection ConfigurePolecat(
        this IServiceCollection services, Action<StoreOptions> configure)
    {
        services.AddSingleton<IConfigurePolecat>(new LambdaConfigurePolecat((_, opts) => configure(opts)));
        return services;
    }

    /// <summary>
    ///     Register a post-configuration action for StoreOptions that has access to the
    ///     service provider. Use this when the configuration depends on other services --
    ///     options bound from configuration, a logger, or a running host component.
    /// </summary>
    public static IServiceCollection ConfigurePolecat(
        this IServiceCollection services, Action<IServiceProvider, StoreOptions> configure)
    {
        services.AddSingleton<IConfigurePolecat>(new LambdaConfigurePolecat(configure));
        return services;
    }
}

internal class LambdaConfigurePolecat : IConfigurePolecat
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
