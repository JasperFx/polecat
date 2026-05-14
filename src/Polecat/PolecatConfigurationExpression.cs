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
    ///     Enable the async projection daemon as a hosted service.
    ///     This starts background processing of projections registered with Async lifecycle.
    /// </summary>
    public PolecatConfigurationExpression AddAsyncDaemon(DaemonMode mode)
    {
        Services.ConfigurePolecat(opts => opts.DaemonSettings.AsyncMode = mode);
        EnsureActivatorIsRegistered();
        Services.AddSingleton<PolecatDaemonHostedService>(sp =>
        {
            var store = (DocumentStore)sp.GetRequiredService<IDocumentStore>();
            return new PolecatDaemonHostedService(store, sp.GetRequiredService<ILoggerFactory>());
        });
        Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<PolecatDaemonHostedService>());
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
    ///     Mutually exclusive with <see cref="AddAsyncDaemon"/> — pick one. The
    ///     coordinator subsumes the simple hosted-service path: it owns the
    ///     daemon lifecycle, leader election, and per-shard agent start/stop.
    /// </remarks>
    public PolecatConfigurationExpression AddProjectionCoordinator(DaemonMode mode)
    {
        Services.ConfigurePolecat(opts => opts.DaemonSettings.AsyncMode = mode);
        EnsureActivatorIsRegistered();

        Services.AddSingleton<Polecat.Events.Daemon.Coordination.IProjectionCoordinator>(sp =>
        {
            var store = (DocumentStore)sp.GetRequiredService<IDocumentStore>();
            return new Polecat.Events.Daemon.Coordination.ProjectionCoordinator(
                store, sp.GetRequiredService<ILoggerFactory>());
        });
        Services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<Polecat.Events.Daemon.Coordination.IProjectionCoordinator>());
        return this;
    }

    /// <summary>
    ///     Marker-typed variant of <see cref="AddProjectionCoordinator"/> for
    ///     ancillary store registrations (multi-store apps where each store
    ///     gets its own coordinator).
    /// </summary>
    public PolecatConfigurationExpression AddProjectionCoordinator<T>(DaemonMode mode)
        where T : class, IDocumentStore
    {
        Services.ConfigurePolecat(opts => opts.DaemonSettings.AsyncMode = mode);
        EnsureActivatorIsRegistered();

        Services.AddSingleton<Polecat.Events.Daemon.Coordination.IProjectionCoordinator<T>>(sp =>
        {
            var store = (DocumentStore)(IDocumentStore)sp.GetRequiredService<T>();
            return new Polecat.Events.Daemon.Coordination.ProjectionCoordinator<T>(
                store, sp.GetRequiredService<ILoggerFactory>());
        });
        Services.AddSingleton<IHostedService>(sp =>
            sp.GetRequiredService<Polecat.Events.Daemon.Coordination.IProjectionCoordinator<T>>());
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

    private void EnsureActivatorIsRegistered()
    {
        if (Services.Any(x =>
                x.ServiceType == typeof(IHostedService) &&
                x.ImplementationType == typeof(PolecatActivator)))
        {
            return;
        }

        Services.Insert(0,
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
        services.AddSingleton<IConfigurePolecat>(new LambdaConfigurePolecat(configure));
        return services;
    }
}

internal class LambdaConfigurePolecat : IConfigurePolecat
{
    private readonly Action<StoreOptions> _configure;

    public LambdaConfigurePolecat(Action<StoreOptions> configure)
    {
        _configure = configure;
    }

    public void Configure(IServiceProvider serviceProvider, StoreOptions options)
    {
        _configure(options);
    }
}
