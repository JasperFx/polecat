using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Internal;

namespace Polecat;

/// <summary>
///     Fluent builder returned by AddPolecat() for further configuration.
/// </summary>
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
