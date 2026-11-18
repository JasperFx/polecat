using System.Reflection;
using JasperFx.Core;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon.Coordination;
using Shouldly;
using Weasel.SqlServer;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #749: <c>opts.Events.ConfigureAdvisoryLock</c> reaches the <see cref="AdvisoryLock" /> the
///     projection coordinator builds, and leaving it unset is exactly Weasel 9.42.0's behaviour.
///     Constructing an AdvisoryLock never opens its connection, so no database is touched.
/// </summary>
public class advisory_lock_options_tests
{
    private static DocumentStore Store(Action<StoreOptions>? configure = null)
        => DocumentStore.For(opts =>
        {
            // Never opened: building a lock does not connect, and a monitor with no locks held
            // short-circuits before it would.
            opts.ConnectionString =
                "Server=unopened;Database=none;User Id=sa;Password=irrelevant;TrustServerCertificate=True";
            configure?.Invoke(opts);
        });

    [Fact]
    public void unset_is_exactly_the_three_argument_constructors_settings()
    {
        var built = new StoreOptions().Events.BuildAdvisoryLockOptions();
        var weaselDefault = new SqlServerAdvisoryLockOptions();

        built.MonitoringInterval.ShouldBe(weaselDefault.MonitoringInterval);
        built.ProbeTimeout.ShouldBe(weaselDefault.ProbeTimeout);
    }

    [Fact]
    public void the_configure_action_is_applied()
    {
        var options = new StoreOptions();
        options.Events.ConfigureAdvisoryLock = x =>
        {
            x.MonitoringInterval = 10.Seconds();
            x.ProbeTimeout = 3.Seconds();
        };

        var built = options.Events.BuildAdvisoryLockOptions();

        built.MonitoringInterval.ShouldBe(10.Seconds());
        built.ProbeTimeout.ShouldBe(3.Seconds());
    }

    [Fact]
    public void the_action_runs_once_per_coordinator_not_once_per_lock()
    {
        var invocations = 0;
        using var store = Store(opts => opts.Events.ConfigureAdvisoryLock = _ => invocations++);
        var database = (IProjectionDatabase)store.Options.Tenancy!.AllDatabases().Single();

        var factory = ProjectionCoordinator.BuildLockFactory(store.Options, NullLoggerFactory.Instance);
        invocations.ShouldBe(1);

        _ = factory(database);
        _ = factory(database);
        invocations.ShouldBe(1);
    }

    // ⚠️ The end-to-end half, and why it reads a Weasel private field. AdvisoryLock exposes none of its
    // settings, and the one observable effect of the opt-out -- no PeriodicTimer -- is internal to it.
    // Without this, a coordinator that computed the options and then called the THREE-argument
    // constructor would pass every other test here. The field lookup fails loudly rather than going
    // green if Weasel ever renames it.
    [Fact]
    public async Task an_infinite_interval_reaches_the_lock_and_starts_no_monitor()
    {
        using var store = Store(opts =>
            opts.Events.ConfigureAdvisoryLock = x => x.MonitoringInterval = Timeout.InfiniteTimeSpan);
        var database = (IProjectionDatabase)store.Options.Tenancy!.AllDatabases().Single();

        var advisoryLock = ProjectionCoordinator.BuildLockFactory(store.Options, NullLoggerFactory.Instance)(database);
        try
        {
            MonitorTimerOf(advisoryLock).ShouldBeNull();
        }
        finally
        {
            await advisoryLock.DisposeAsync();
        }
    }

    [Fact]
    public async Task by_default_the_lock_runs_its_monitor()
    {
        using var store = Store();
        var database = (IProjectionDatabase)store.Options.Tenancy!.AllDatabases().Single();

        var advisoryLock = ProjectionCoordinator.BuildLockFactory(store.Options, NullLoggerFactory.Instance)(database);
        try
        {
            MonitorTimerOf(advisoryLock).ShouldNotBeNull();
        }
        finally
        {
            await advisoryLock.DisposeAsync();
        }
    }

    private static object? MonitorTimerOf(IAdvisoryLock advisoryLock)
    {
        var field = typeof(AdvisoryLock).GetField("_monitorTimer", BindingFlags.Instance | BindingFlags.NonPublic);
        field.ShouldNotBeNull("Weasel.SqlServer.AdvisoryLock no longer has a _monitorTimer field; re-point this probe");
        return field.GetValue(advisoryLock);
    }
}
