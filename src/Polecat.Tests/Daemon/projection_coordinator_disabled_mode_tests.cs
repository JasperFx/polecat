using JasperFx.Events.Daemon;
using Polecat;
using Polecat.Events.Daemon.Coordination;
using Shouldly;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Polecat#141 (jasperfx#352): in <see cref="DaemonMode.Disabled"/> there is
///     nothing to coordinate, so <c>ProjectionCoordinator.BuildDistributor</c>
///     returns null and the lifted <c>ProjectionCoordinatorBase</c> tolerates it —
///     the coordinator constructs without throwing and Start/Stop no-op. No
///     database is touched in this mode, so these are pure unit tests.
/// </summary>
public class projection_coordinator_disabled_mode_tests
{
    private static DocumentStore DisabledStore()
        => DocumentStore.For(opts =>
        {
            // Never opened — Disabled mode builds no distributor and starts no loop.
            opts.ConnectionString =
                "Server=disabled;Database=none;User Id=sa;Password=irrelevant;TrustServerCertificate=True";
            // opts.DaemonSettings.AsyncMode defaults to DaemonMode.Disabled
        });

    [Fact]
    public void coordinator_constructs_with_null_distributor_in_disabled_mode()
    {
        using var store = DisabledStore();
        store.Options.DaemonSettings.AsyncMode.ShouldBe(DaemonMode.Disabled);

        var coordinator = new ProjectionCoordinator(store);

        coordinator.Distributor.ShouldBeNull();
    }

    [Fact]
    public async Task start_then_stop_no_op_in_disabled_mode()
    {
        using var store = DisabledStore();
        var coordinator = new ProjectionCoordinator(store);

        // StartAsync no-ops (no distributor → no leadership loop); StopAsync
        // guards ReleaseAllLocks. Neither should throw or contact the database.
        await Should.NotThrowAsync(async () =>
        {
            await coordinator.StartAsync(CancellationToken.None);
            await coordinator.StopAsync(CancellationToken.None);
        });
    }
}
