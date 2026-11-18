using JasperFx;
using JasperFx.Core;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.TestUtils;
using Weasel.SqlServer;
using PolecatCoordinator = Polecat.Events.Daemon.Coordination.IProjectionCoordinator;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #871 item 5, marten#4915. <see cref="IHostedService" /> only promises a StopAsync on a graceful
///     shutdown and <c>IHost.Dispose()</c> does not stop hosted services, so the common
///     <c>using var host</c> shape tears the container down under a running coordinator. Its loop then kept
///     polling and its session kept the shard locks, against every node that needed them next.
/// </summary>
public class coordinator_disposal_tests
{
    private static IHost BuildNode(string schema)
    {
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services => services.AddPolecat(opts =>
            {
                opts.ConnectionString = ConnectionSource.ConnectionString;
                opts.DatabaseSchemaName = schema;
                opts.AutoCreateSchemaObjects = AutoCreate.All;
                opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
                opts.DaemonSettings.LeadershipPollingTime = 100;
                opts.Projections.Add<CoordinatedTallyProjection>(ProjectionLifecycle.Async);
            }).AddProjectionCoordinator(DaemonMode.HotCold))
            .Build();
    }

    private static async Task<int> WaitForTheShardLockAsync(IHost host)
    {
        var coordinator = (ProjectionCoordinatorBase)host.Services.GetRequiredService<PolecatCoordinator>();
        var set = (await coordinator.Distributor!.BuildDistributionAsync()).ShouldHaveSingleItem();

        var deadline = DateTimeOffset.UtcNow.Add(15.Seconds());
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (coordinator.Distributor.HasLock(set)) return set.LockId;
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException("The coordinator never took the shard lock");
    }

    private static AdvisoryLock BuildProbe()
    {
        return new AdvisoryLock(() => new SqlConnection(ConnectionSource.ConnectionString),
            NullLogger.Instance, "probe");
    }

    [Fact]
    public async Task disposing_a_running_host_gives_the_shard_lock_back()
    {
        var token = TestContext.Current.CancellationToken;
        var host = BuildNode("coord_disp_" + Guid.NewGuid().ToString("N")[..8]);

        await host.StartAsync(token);
        var lockId = await WaitForTheShardLockAsync(host);

        await using var probe = BuildProbe();
        (await probe.TryAttainLockAsync(lockId, token)).ShouldBeFalse();

        host.Dispose();

        (await probe.TryAttainLockAsync(lockId, token)).ShouldBeTrue();
    }

    [Fact]
    public async Task disposing_a_stopped_host_is_uneventful()
    {
        var token = TestContext.Current.CancellationToken;
        var host = BuildNode("coord_disp_" + Guid.NewGuid().ToString("N")[..8]);

        await host.StartAsync(token);
        var lockId = await WaitForTheShardLockAsync(host);
        await host.StopAsync(token);

        await using var probe = BuildProbe();
        (await probe.TryAttainLockAsync(lockId, token)).ShouldBeTrue();

        Should.NotThrow(() => host.Dispose());
    }
}
