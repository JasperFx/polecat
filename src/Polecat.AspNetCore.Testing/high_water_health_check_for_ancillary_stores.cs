using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Polecat;
using Polecat.Events.Daemon;
using Polecat.TestUtils;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

public interface IHwAncillaryStore: IDocumentStore;

/// <summary>
///     #847 / marten#5705: <c>AddPolecatHighWaterHealthCheck()</c> only knew the default store. An application
///     that registered its stores only through <c>AddPolecatStore&lt;TStore&gt;()</c> got a check that threw on
///     its first probe, because nothing registers <see cref="IDocumentStore" /> there, and beside a default
///     store the ancillary one was silently unchecked. <c>autoRestart</c> only ever asked the unmarked
///     coordinator.
///     <para>
///         The hosts here are built and never started, so no daemon runs and a seeded high-water row stays
///         where it was put. That is the stalled agent the check exists to report.
///     </para>
/// </summary>
public class high_water_health_check_for_ancillary_stores
{
    private static readonly TimeSpan Threshold = TimeSpan.FromMilliseconds(200);

    private static string UniqueSchema() => "hw_anc_" + Guid.NewGuid().ToString("N")[..8];

    private static void configure(StoreOptions opts, string schema)
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.DaemonSettings.AsyncMode = DaemonMode.Solo;
        opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
    }

    private static void addAncillaryStore(IServiceCollection services) =>
        services.AddPolecatStore<IHwAncillaryStore>(opts => configure(opts, UniqueSchema()));

    private static void addDefaultStore(IServiceCollection services) =>
        services.AddPolecat(opts => configure(opts, UniqueSchema()));

    private static IHost buildHost(Action<IServiceCollection> configureServices) =>
        Host.CreateDefaultBuilder().ConfigureServices(configureServices).Build();

    // Puts a stalled agent on the store: events past a high-water row that will not move
    private static async Task stallAsync(IDocumentStore store, long mark, int events = 5)
    {
        var token = TestContext.Current.CancellationToken;
        var documentStore = (DocumentStore)store;

        await documentStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await using (var session = store.LightweightSession())
        {
            for (var i = 0; i < events; i++)
            {
                session.Events.StartStream(Guid.NewGuid(), new HwFakeEvent());
            }

            await session.SaveChangesAsync(token);
        }

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            MERGE {store.Options.EventGraph.ProgressionTableName} AS t
            USING (SELECT 'HighWaterMark' AS name, CAST({mark} AS bigint) AS last_seq_id) AS s
            ON t.name = s.name
            WHEN MATCHED THEN UPDATE SET last_seq_id = s.last_seq_id
            WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                VALUES (s.name, s.last_seq_id, SYSDATETIMEOFFSET());
            """;
        await cmd.ExecuteNonQueryAsync(token);
    }

    // The gap heuristic needs two readings a threshold apart: the first notes the stuck mark, the second
    // finds it still stuck
    private static async Task<HealthReport> probeTwiceAsync(IHost host)
    {
        var token = TestContext.Current.CancellationToken;
        var health = host.Services.GetRequiredService<HealthCheckService>();

        await health.CheckHealthAsync(token);
        await Task.Delay(Threshold + TimeSpan.FromMilliseconds(100), token);

        return await health.CheckHealthAsync(token);
    }

    private static HealthReportEntry entryFor(HealthReport report) =>
        report.Entries[HighWaterHealthCheckExtensions.HighWaterHealthCheckNameFor<IHwAncillaryStore>()];

    [Fact]
    public async Task a_store_registered_only_through_AddPolecatStore_can_be_probed()
    {
        using var host = buildHost(services =>
        {
            addAncillaryStore(services);
            services.AddHealthChecks().AddPolecatHighWaterHealthCheck<IHwAncillaryStore>();
        });

        var report = await host.Services.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(TestContext.Current.CancellationToken);

        var entry = entryFor(report);
        entry.Exception.ShouldBeNull();
        entry.Status.ShouldBe(HealthStatus.Healthy);
        entry.Tags.ShouldContain(nameof(IHwAncillaryStore));
    }

    /// <summary>
    ///     The report, kept as the behaviour of the store-less overload: it still means the default store.
    /// </summary>
    [Fact]
    public async Task the_default_check_still_needs_the_default_store()
    {
        using var host = buildHost(services =>
        {
            addAncillaryStore(services);
            services.AddHealthChecks().AddPolecatHighWaterHealthCheck();
        });

        // The health check service builds each check before running it, and a check it cannot build throws
        // out of the probe rather than reporting Unhealthy
        var failure = await Should.ThrowAsync<InvalidOperationException>(() =>
            host.Services.GetRequiredService<HealthCheckService>()
                .CheckHealthAsync(TestContext.Current.CancellationToken));
        failure.Message.ShouldContain(typeof(IDocumentStore).FullName!);
    }

    [Fact]
    public async Task a_stalled_mark_on_the_ancillary_store_is_reported()
    {
        using var host = buildHost(services =>
        {
            addAncillaryStore(services);
            services.AddHealthChecks().AddPolecatHighWaterHealthCheck<IHwAncillaryStore>(staleThreshold: Threshold);
        });

        await stallAsync(host.Services.GetRequiredService<IHwAncillaryStore>(), mark: 1);

        var entry = entryFor(await probeTwiceAsync(host));

        entry.Status.ShouldBe(HealthStatus.Unhealthy);
        entry.Description.ShouldNotBeNull().ShouldContain("stuck at 1");
    }

    [Fact]
    public async Task auto_restart_goes_through_the_ancillary_stores_own_coordinator()
    {
        var coordinator = new RecordingCoordinator();

        using var host = buildHost(services =>
        {
            addAncillaryStore(services);

            services.AddSingleton<Polecat.Events.Daemon.Coordination.IProjectionCoordinator<IHwAncillaryStore>>(
                coordinator);

            services.AddHealthChecks()
                .AddPolecatHighWaterHealthCheck<IHwAncillaryStore>(staleThreshold: Threshold, autoRestart: true);
        });

        var store = host.Services.GetRequiredService<IHwAncillaryStore>();
        await stallAsync(store, mark: 1);

        var entry = entryFor(await probeTwiceAsync(host));

        entry.Status.ShouldBe(HealthStatus.Unhealthy);
        coordinator.AskedFor.ShouldBe([((DocumentStore)store).Database.Identifier]);
    }

    /// <summary>
    ///     Two stores in one database, stalled at different marks. Each check reads its own store, and each
    ///     keeps its own clock. PolecatDatabase's identifier is "Polecat" for both, so with a shared tracker
    ///     the second check to run would read the first's mark as its own having moved, reset, and report
    ///     Healthy for as long as the two alternated.
    /// </summary>
    [Fact]
    public async Task each_store_keeps_its_own_settings_and_tracking()
    {
        using var host = buildHost(services =>
        {
            addDefaultStore(services);
            addAncillaryStore(services);

            services.AddHealthChecks()
                .AddPolecatHighWaterHealthCheck(staleThreshold: Threshold)
                .AddPolecatHighWaterHealthCheck<IHwAncillaryStore>(staleThreshold: Threshold);
        });

        await stallAsync(host.Services.GetRequiredService<IDocumentStore>(), mark: 1);
        await stallAsync(host.Services.GetRequiredService<IHwAncillaryStore>(), mark: 2);

        var report = await probeTwiceAsync(host);

        report.Entries[nameof(HighWaterHealthCheckExtensions.HighWaterHealthCheck)]
            .Description.ShouldNotBeNull().ShouldContain("stuck at 1");
        entryFor(report).Description.ShouldNotBeNull().ShouldContain("stuck at 2");
    }

    [Fact]
    public async Task registering_an_ancillary_check_does_not_reconfigure_the_default_one()
    {
        using var host = buildHost(services =>
        {
            addDefaultStore(services);
            addAncillaryStore(services);

            services.AddHealthChecks()
                .AddPolecatHighWaterHealthCheck(staleThreshold: TimeSpan.FromHours(1))
                .AddPolecatHighWaterHealthCheck<IHwAncillaryStore>(staleThreshold: Threshold);
        });

        await stallAsync(host.Services.GetRequiredService<IDocumentStore>(), mark: 1);
        await stallAsync(host.Services.GetRequiredService<IHwAncillaryStore>(), mark: 1);

        var report = await probeTwiceAsync(host);

        report.Entries[nameof(HighWaterHealthCheckExtensions.HighWaterHealthCheck)].Status
            .ShouldBe(HealthStatus.Healthy);
        entryFor(report).Status.ShouldBe(HealthStatus.Unhealthy);
    }

    private sealed class RecordingCoordinator
        : Polecat.Events.Daemon.Coordination.IProjectionCoordinator<IHwAncillaryStore>
    {
        public List<string> AskedFor { get; } = new();

        public ValueTask<IProjectionDaemon> DaemonForDatabase(string databaseIdentifier)
        {
            AskedFor.Add(databaseIdentifier);
            throw new InvalidOperationException("Recording only");
        }

        public IProjectionDaemon DaemonForMainDatabase() => throw new NotSupportedException();

        public ValueTask<IReadOnlyList<IProjectionDaemon>> AllDaemonsAsync() => throw new NotSupportedException();

        public Task PauseAsync() => Task.CompletedTask;

        public Task ResumeAsync() => Task.CompletedTask;

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
