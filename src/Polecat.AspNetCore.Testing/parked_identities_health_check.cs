using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Polecat;
using Polecat.Events.Daemon;
using Polecat.Projections;
using Polecat.TestUtils;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

public record WeighInStarted;

public record Weighed(bool Poison);

public partial class WeighIn
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

/// <summary>Throws on a poisoned <see cref="Weighed" />, so the daemon parks the stream.</summary>
public partial class WeighInProjection: SingleStreamProjection<WeighIn, Guid>
{
    public static WeighIn Create(IEvent<WeighInStarted> e) => new() { Id = e.StreamId };

    public void Apply(Weighed e, WeighIn weighIn)
    {
        if (e.Poison) throw new InvalidOperationException("Can't weigh this one");
        weighIn.Count++;
    }
}

public interface IParkedHealthStore: IDocumentStore;

/// <summary>
///     #1105 (marten#6122, fisher#653): <c>AddPolecatParkedIdentitiesHealthCheck()</c> is Degraded while an identity
///     is parked, Healthy again once it is discarded, and Unhealthy while a shard is paused at the parking cap, both on
///     the node that runs the shard and, through the extended progression columns, on one that doesn't.
/// </summary>
public class parked_identities_health_check
{
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(60);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string UniqueSchema() => "parked_hc_" + Guid.NewGuid().ToString("N")[..8];

    private static void configure(StoreOptions opts, string schema)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Projections.Add<WeighInProjection>(ProjectionLifecycle.Async);
    }

    private static async Task<Guid> weighInAsync(IDocumentStore store, bool poison)
    {
        var stream = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream<WeighIn>(stream, new WeighInStarted(), new Weighed(poison));
        await session.SaveChangesAsync(Ct);
        return stream;
    }

    private static async Task<HealthReportEntry> probeAsync(IHost host, string name)
    {
        var report = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync(Ct);
        return report.Entries[name];
    }

    private static async Task waitForPausedAsync(Func<AgentStatus> status)
    {
        var deadline = DateTimeOffset.UtcNow.Add(Generous);
        while (status() != AgentStatus.Paused)
        {
            DateTimeOffset.UtcNow.ShouldBeLessThan(deadline, "the shard never paused at the parking cap");
            await Task.Delay(100, Ct);
        }
    }

    [Fact]
    public async Task degraded_while_an_identity_is_parked_and_healthy_once_it_is_discarded()
    {
        using var host = await Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecat(opts => configure(opts, UniqueSchema())).AddAsyncDaemon(DaemonMode.Solo);
                services.AddHealthChecks().AddPolecatParkedIdentitiesHealthCheck();
            })
            .StartAsync(Ct);

        var store = (DocumentStore)host.Services.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var name = nameof(ParkedIdentitiesHealthCheckExtensions.ParkedIdentitiesHealthCheck);
        (await probeAsync(host, name)).Status.ShouldBe(HealthStatus.Healthy);

        await weighInAsync(store, poison: false);
        var poisoned = await weighInAsync(store, poison: true);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        var degraded = await probeAsync(host, name);
        degraded.Status.ShouldBe(HealthStatus.Degraded);
        degraded.Description.ShouldNotBeNull().ShouldContain("1 projection identity is parked");
        degraded.Description.ShouldContain(nameof(WeighIn));
        degraded.Tags.ShouldContain("Parked");

        var daemon = host.Services.GetRequiredService<IProjectionCoordinator>().DaemonForMainDatabase();
        (await daemon.DiscardParkedIdentityAsync(nameof(WeighIn), null, poisoned, Ct)).Outcome
            .ShouldBe(ParkedReplayOutcome.Discarded);

        (await probeAsync(host, name)).Status.ShouldBe(HealthStatus.Healthy);

        await host.StopAsync(Ct);
    }

    [Fact]
    public async Task unhealthy_while_the_shard_is_paused_at_the_parking_cap_on_the_node_that_runs_it()
    {
        using var host = await Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecat(opts =>
                {
                    configure(opts, UniqueSchema());
                    opts.Projections.Errors.MaxParkedIdentitiesPerShard = 1;
                }).AddAsyncDaemon(DaemonMode.Solo);
                services.AddHealthChecks().AddPolecatParkedIdentitiesHealthCheck();
            })
            .StartAsync(Ct);

        var store = (DocumentStore)host.Services.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await weighInAsync(store, poison: true);
        await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

        var name = nameof(ParkedIdentitiesHealthCheckExtensions.ParkedIdentitiesHealthCheck);
        (await probeAsync(host, name)).Status.ShouldBe(HealthStatus.Degraded);

        // The second park would pass the cap of one, so the shard pauses instead
        await weighInAsync(store, poison: true);

        var daemon = host.Services.GetRequiredService<IProjectionCoordinator>().DaemonForMainDatabase();
        var shard = ((IEventStore)store).RegisteredShardNames().Single().Identity;
        await waitForPausedAsync(() => daemon.StatusFor(shard));

        var unhealthy = await probeAsync(host, name);
        unhealthy.Status.ShouldBe(HealthStatus.Unhealthy);
        unhealthy.Description.ShouldNotBeNull().ShouldContain("1 projection shard is paused at the parking cap");
        unhealthy.Description.ShouldContain(shard);
        unhealthy.Description.ShouldContain("1 projection identity is parked");

        await host.StopAsync(Ct);
    }

    [Fact]
    public async Task unhealthy_from_the_progression_row_on_a_node_that_does_not_run_the_shard()
    {
        await using var store = DocumentStore.For(opts =>
        {
            configure(opts, UniqueSchema());
            opts.Events.EnableExtendedProgressionTracking = true;
            opts.Projections.Errors.MaxParkedIdentitiesPerShard = 1;
        });
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        using var daemon = await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        try
        {
            // The first park commits with the shard's progression row. The failure columns are only ever updated on
            // that row, so a shard that trips on its very first batch has nothing in pc_event_progression to say so.
            await weighInAsync(store, poison: true);
            await store.Database.WaitForNonStaleProjectionDataAsync(Generous);

            await weighInAsync(store, poison: true);

            var shard = ((IEventStore)store).RegisteredShardNames().Single().Identity;
            await waitForPausedAsync(() => daemon.StatusFor(shard));

            // No coordinator: this is a node that doesn't run the daemon, so only pc_event_progression can say
            var check = new ParkedIdentitiesHealthCheckExtensions.ParkedIdentitiesHealthCheck(store);

            var deadline = DateTimeOffset.UtcNow.Add(Generous);
            HealthCheckResult result;
            while ((result = await check.CheckHealthAsync(new HealthCheckContext(), Ct)).Status != HealthStatus.Unhealthy)
            {
                DateTimeOffset.UtcNow.ShouldBeLessThan(deadline,
                    $"the paused shard never reached pc_event_progression: {result.Status} {result.Description}");
                await Task.Delay(100, Ct);
            }

            result.Description.ShouldNotBeNull().ShouldContain(shard);
            result.Exception.ShouldBeNull();
        }
        finally
        {
            await daemon.StopAllAsync();
        }
    }

    [Fact]
    public async Task an_ancillary_store_gets_a_check_of_its_own()
    {
        using var host = await Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddPolecatStore<IParkedHealthStore>(opts => configure(opts, UniqueSchema()))
                    .AddAsyncDaemon(DaemonMode.Solo);
                services.AddHealthChecks().AddPolecatParkedIdentitiesHealthCheck<IParkedHealthStore>();
            })
            .StartAsync(Ct);

        var store = host.Services.GetRequiredService<IParkedHealthStore>();
        var database = ((DocumentStore)(object)store).Database;
        await database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var name = ParkedIdentitiesHealthCheckExtensions.ParkedIdentitiesHealthCheckNameFor<IParkedHealthStore>();
        name.ShouldBe("ParkedIdentitiesHealthCheck:IParkedHealthStore");

        var healthy = await probeAsync(host, name);
        healthy.Status.ShouldBe(HealthStatus.Healthy);
        healthy.Tags.ShouldContain(nameof(IParkedHealthStore));

        await weighInAsync(store, poison: true);
        await database.WaitForNonStaleProjectionDataAsync(Generous);

        (await probeAsync(host, name)).Status.ShouldBe(HealthStatus.Degraded);

        await host.StopAsync(Ct);
    }
}
