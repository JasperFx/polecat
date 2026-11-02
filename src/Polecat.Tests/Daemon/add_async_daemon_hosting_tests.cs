using JasperFx;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat.Internal;
using Polecat.Projections;
using Polecat.TestUtils;
using PolecatCoordinator = Polecat.Events.Daemon.Coordination.IProjectionCoordinator;

namespace Polecat.Tests.Daemon;

public record PausingTick;

public class PausingTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

public partial class PausingTallyProjection : SingleStreamProjection<PausingTally, Guid>
{
    // Set while the "outage" lasts. Nothing else in this suite registers the projection.
    public static volatile bool Failing;

    public override PausingTally? Evolve(PausingTally? snapshot, Guid id, IEvent e)
    {
        if (Failing) throw new InvalidOperationException("The database went away");

        snapshot ??= new PausingTally { Id = id };
        if (e.Data is PausingTick) snapshot.Count++;
        return snapshot;
    }
}

/// <summary>
///     #847: <c>AddAsyncDaemon</c> used to call <c>StartAllAsync</c> from a hosted service of its own, which
///     takes no lock and runs no loop. So <see cref="DaemonMode.HotCold" /> had no leader election, a paused
///     shard stayed paused until the process restarted, and a tenant added to a master table was never
///     found. It now registers the projection coordinator, the way Marten's does.
/// </summary>
public class add_async_daemon_hosting_tests
{
    private const string CoordinatedShard = "CoordinatedTally:All";
    private const string AgentShard = "AgentTally:All";

    private static readonly TimeSpan Timeout = 20.Seconds();

    private static string NewSchema() => "async_dmn_" + Guid.NewGuid().ToString("N")[..8];

    private static void Configure(StoreOptions opts, string schema)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.DaemonSettings.LeadershipPollingTime = 100;
    }

    private static IHost BuildNode(string schema, DaemonMode mode)
    {
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services => services.AddPolecat(opts =>
            {
                Configure(opts, schema);
                opts.Projections.Add<CoordinatedTallyProjection>(ProjectionLifecycle.Async);
                opts.Projections.Add<AgentTallyProjection>(ProjectionLifecycle.Async);
            }).AddAsyncDaemon(mode))
            .Build();
    }

    private static bool IsRunning(IHost node, string shard)
    {
        return node.Services.GetRequiredService<PolecatDaemonHostedService>().Daemons
            .SelectMany(x => x.CurrentAgents())
            .Any(x => x.Name.Identity == shard && x.Status == AgentStatus.Running);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string description)
    {
        var deadline = DateTimeOffset.UtcNow.Add(Timeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException($"Timed out waiting until {description}");
    }

    private static async Task AppendTickAsync(IHost node, Guid id)
    {
        var store = node.Services.GetRequiredService<IDocumentStore>();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id, new CoordinatedTick());
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private static async Task WaitForTallyAsync(IHost node, Guid id)
    {
        var store = node.Services.GetRequiredService<IDocumentStore>();
        var deadline = DateTimeOffset.UtcNow.Add(Timeout);
        while (DateTimeOffset.UtcNow < deadline)
        {
            await using var query = store.QuerySession();
            if (await query.LoadAsync<CoordinatedTally>(id, TestContext.Current.CancellationToken) != null) return;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException("The event was never projected");
    }

    [Fact]
    public async Task a_second_hot_cold_node_runs_nothing_until_the_first_one_goes_away()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();

        using var first = BuildNode(schema, DaemonMode.HotCold);
        using var second = BuildNode(schema, DaemonMode.HotCold);

        await first.StartAsync(token);
        await WaitUntilAsync(() => IsRunning(first, CoordinatedShard) && IsRunning(first, AgentShard),
            "the first node ran both shards");

        await second.StartAsync(token);

        // Ten leadership cycles: plenty for a node that was going to start something to have started it.
        await Task.Delay(1.Seconds(), token);
        IsRunning(second, CoordinatedShard).ShouldBeFalse();
        IsRunning(second, AgentShard).ShouldBeFalse();

        await first.StopAsync(token);
        await WaitUntilAsync(() => IsRunning(second, CoordinatedShard) && IsRunning(second, AgentShard),
            "the second node took over both shards");

        var id = Guid.NewGuid();
        await AppendTickAsync(second, id);
        await WaitForTallyAsync(second, id);

        await second.StopAsync(token);
    }

    /// <summary>
    ///     marten#5754. A failure past the Polly retries pauses the agent, and only the coordinator's loop
    ///     restarts a paused one. After a database restart the shard would stay paused until the process did.
    /// </summary>
    [Fact]
    public async Task a_paused_shard_resumes_on_its_own()
    {
        var token = TestContext.Current.CancellationToken;
        var schema = NewSchema();
        const string shard = "PausingTally:All";

        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services => services.AddPolecat(opts =>
            {
                Configure(opts, schema);
                opts.DaemonSettings.HealthCheckPollingTime = 250.Milliseconds();
                opts.DaemonSettings.AgentPauseTime = 100.Milliseconds();

                // An application failure pauses the agent exactly as a failed load does: both end in
                // ReportCriticalFailureAsync. The default skips it into a dead letter instead.
                opts.Projections.Errors.SkipApplyErrors = false;
                opts.Projections.Add<PausingTallyProjection>(ProjectionLifecycle.Async);
            }).AddAsyncDaemon(DaemonMode.Solo))
            .Build();

        var store = host.Services.GetRequiredService<IDocumentStore>();
        var daemons = host.Services.GetRequiredService<PolecatDaemonHostedService>();

        AgentStatus? status() => daemons.Daemons.SelectMany(x => x.CurrentAgents())
            .FirstOrDefault(x => x.Name.Identity == shard)?.Status;

        PausingTallyProjection.Failing = true;
        try
        {
            await host.StartAsync(token);

            var id = Guid.NewGuid();
            await using (var session = store.LightweightSession())
            {
                session.Events.StartStream(id, new PausingTick());
                await session.SaveChangesAsync(token);
            }

            await WaitUntilAsync(() => status() == AgentStatus.Paused, "the shard paused");

            PausingTallyProjection.Failing = false;

            var deadline = DateTimeOffset.UtcNow.Add(Timeout);
            while (DateTimeOffset.UtcNow < deadline)
            {
                await using var query = store.QuerySession();
                if (await query.LoadAsync<PausingTally>(id, token) != null)
                {
                    status().ShouldBe(AgentStatus.Running);
                    await host.StopAsync(token);
                    return;
                }

                await Task.Delay(100, token);
            }

            throw new TimeoutException($"The shard never resumed. Its status is still {status()}.");
        }
        finally
        {
            PausingTallyProjection.Failing = false;
        }
    }

    /// <summary>
    ///     The old hosted service never read the mode at all, so <see cref="DaemonMode.Disabled" /> ran every
    ///     shard. <see cref="DaemonMode.ExternallyManaged" /> means another system runs them.
    /// </summary>
    [Theory]
    [InlineData(DaemonMode.Disabled)]
    [InlineData(DaemonMode.ExternallyManaged)]
    public async Task a_mode_that_hosts_nothing_starts_nothing(DaemonMode mode)
    {
        var token = TestContext.Current.CancellationToken;

        using var host = BuildNode(NewSchema(), mode);
        await host.StartAsync(token);

        var hosted = host.Services.GetServices<IHostedService>().ToArray();
        hosted.OfType<PolecatDaemonHostedService>().ShouldBeEmpty();
        hosted.OfType<JasperFx.Events.Daemon.IProjectionCoordinator>().ShouldBeEmpty();

        var id = Guid.NewGuid();
        await AppendTickAsync(host, id);
        await Task.Delay(1.Seconds(), token);

        await using var query = host.Services.GetRequiredService<IDocumentStore>().QuerySession();
        (await query.LoadAsync<CoordinatedTally>(id, token)).ShouldBeNull();

        await host.StopAsync(token);
    }

    [Fact]
    public void the_coordinator_is_what_add_async_daemon_registers()
    {
        using var host = BuildNode(NewSchema(), DaemonMode.Solo);

        var coordinator = host.Services.GetRequiredService<PolecatCoordinator>();

        host.Services.GetRequiredService<JasperFx.Events.Daemon.IProjectionCoordinator>().ShouldBeSameAs(coordinator);
        host.Services.GetServices<IHostedService>().OfType<PolecatCoordinator>().ShouldHaveSingleItem()
            .ShouldBeSameAs(coordinator);
    }

    /// <summary>
    ///     Both used to register their own hosted service, so a host calling both ran every shard twice. A
    ///     repeated registration would also have the host stop one coordinator twice (marten#5056).
    /// </summary>
    [Fact]
    public void registering_twice_still_hosts_one_coordinator()
    {
        var schema = NewSchema();

        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services => services.AddPolecat(opts => Configure(opts, schema))
                .AddAsyncDaemon(DaemonMode.Solo)
                .AddProjectionCoordinator(DaemonMode.Solo)
                .AddAsyncDaemon(DaemonMode.Solo))
            .Build();

        host.Services.GetServices<IHostedService>().OfType<PolecatCoordinator>().ShouldHaveSingleItem();
        host.Services.GetServices<PolecatCoordinator>().ShouldHaveSingleItem();
        host.Services.GetServices<PolecatDaemonHostedService>().ShouldHaveSingleItem();
    }

    /// <summary>
    ///     <c>Daemon</c> and <c>Daemons</c> stay public for existing callers, Wolverine's tests among them,
    ///     and have to answer as soon as the host has started, before the leadership loop's first cycle.
    /// </summary>
    [Fact]
    public async Task the_old_daemon_accessors_hand_out_the_coordinators_daemon()
    {
        var token = TestContext.Current.CancellationToken;

        using var host = BuildNode(NewSchema(), DaemonMode.Solo);
        await host.StartAsync(token);

        var facade = host.Services.GetRequiredService<PolecatDaemonHostedService>();
        var coordinator = host.Services.GetRequiredService<PolecatCoordinator>();

        facade.Daemon.ShouldNotBeNull().ShouldBeSameAs(coordinator.DaemonForMainDatabase());
        facade.Daemons.ShouldHaveSingleItem().ShouldBeSameAs(facade.Daemon);

        await WaitUntilAsync(() => IsRunning(host, CoordinatedShard), "the shard ran");
        await host.StopAsync(token);
    }

    /// <summary>
    ///     Solo says this node is the only one, so <c>AddAsyncDaemon</c> keeps taking no lock, as it always
    ///     has. A lock here would also make a shard depend on a held connection for no benefit.
    /// </summary>
    [Fact]
    public void solo_takes_no_locks_and_hot_cold_does()
    {
        using var solo = BuildNode(NewSchema(), DaemonMode.Solo);
        ((ProjectionCoordinatorBase)solo.Services.GetRequiredService<PolecatCoordinator>())
            .Distributor.ShouldBeOfType<SoloProjectionDistributor>();

        using var hotCold = BuildNode(NewSchema(), DaemonMode.HotCold);
        ((ProjectionCoordinatorBase)hotCold.Services.GetRequiredService<PolecatCoordinator>())
            .Distributor.ShouldBeOfType<SingleTenantProjectionDistributor>();
    }

    /// <summary>
    ///     Nothing here runs <c>ApplyAllDatabaseChangesOnStartup</c>, so the agents have to provision the
    ///     event store themselves. <c>BuildProjectionDaemonAsync</c> used to do it for this path.
    /// </summary>
    [Fact]
    public async Task an_empty_schema_is_provisioned_without_anything_else_touching_it()
    {
        var token = TestContext.Current.CancellationToken;

        using var host = BuildNode(NewSchema(), DaemonMode.Solo);
        await host.StartAsync(token);
        await WaitUntilAsync(() => IsRunning(host, CoordinatedShard), "the shard ran");

        var id = Guid.NewGuid();
        await AppendTickAsync(host, id);
        await WaitForTallyAsync(host, id);

        await host.StopAsync(token);
    }
}
