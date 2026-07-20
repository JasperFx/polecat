using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using NSubstitute;
using Polecat;
using Polecat.Events.Daemon;
using Polecat.Projections;
using Polecat.Storage;
using Polecat.TestUtils;
using Shouldly;
using Xunit;
using static Polecat.Events.Daemon.HighWaterHealthCheckExtensions;

namespace Polecat.AspNetCore.Testing;

public record HwFakeEvent();

public partial class HwFakeStream
{
    public Guid Id { get; set; }
}

public partial class HwFakeProjection: SingleStreamProjection<HwFakeStream, Guid>
{
    public static HwFakeStream Create(JasperFx.Events.IEvent<HwFakeEvent> e) =>
        new() { Id = e.StreamId };

    public void Apply(HwFakeEvent @event, HwFakeStream projection) { }
}

/// <summary>
///     Polecat parity for Marten's HighWaterHealthCheckTests (marten#4982 / polecat#339).
///     Self-contained DB harness: builds a DocumentStore against the SQL Server test instance,
///     seeds the high-water mark directly, and drives the check with a controllable clock.
///     Run one TFM at a time — DB-backed tests collide on the shared database.
/// </summary>
public class HighWaterHealthCheckTests: IAsyncLifetime
{
    private static readonly string ConnectionString = ConnectionSource.ConnectionString;

    private readonly string _schemaName;
    private readonly MutableTimeProvider _timeProvider;
    private readonly DateTimeOffset _now = DateTimeOffset.UtcNow;
    private readonly HighWaterStateTracker _tracker = new();
    private DocumentStore? _store;

    public HighWaterHealthCheckTests()
    {
        _schemaName = "hw_health_" + Guid.NewGuid().ToString("N")[..8];
        _timeProvider = new MutableTimeProvider(_now);
    }

    public async Task InitializeAsync()
    {
        // Drop the schema for a clean slate.
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF SCHEMA_ID('{_schemaName}') IS NOT NULL
            BEGIN
                DECLARE @sql NVARCHAR(MAX) = '';
                SELECT @sql += 'DROP TABLE IF EXISTS ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';' + CHAR(13)
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{_schemaName}';
                EXEC sp_executesql @sql;
            END
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        if (_store != null)
        {
            await _store.DisposeAsync();
        }
    }

    private DocumentStore configure(Action<StoreOptions> configure)
    {
        var options = new StoreOptions
        {
            ConnectionString = ConnectionString,
            AutoCreateSchemaObjects = AutoCreate.All,
            DatabaseSchemaName = _schemaName,
            UseNativeJsonType = ConnectionSource.SupportsNativeJson
        };

        configure(options);

        _store = new DocumentStore(options);
        return _store;
    }

    private HighWaterHealthCheck buildCheck(TimeSpan? staleThreshold = null, long minimumGap = 1,
        bool autoRestart = false, IProjectionCoordinator? coordinator = null)
    {
        var services = new ServiceCollection();
        if (coordinator != null)
        {
            services.AddSingleton(coordinator);
        }

        return new(_store!,
            new HighWaterHealthCheckSettings(staleThreshold ?? TimeSpan.FromSeconds(30), minimumGap, autoRestart),
            _timeProvider, _tracker, services.BuildServiceProvider());
    }

    private async Task appendEventsAsync(int count)
    {
        await using var session = _store!.LightweightSession();
        var events = Enumerable.Range(0, count).Select(_ => new HwFakeEvent()).Cast<object>().ToArray();
        session.Events.StartStream(Guid.NewGuid(), events);
        await session.SaveChangesAsync();
    }

    private async Task seedHighWaterMarkAsync(long sequence)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            MERGE [{_schemaName}].[pc_event_progression] AS t
            USING (SELECT 'HighWaterMark' AS name, CAST({sequence} AS bigint) AS last_seq_id) AS s
            ON t.name = s.name
            WHEN MATCHED THEN UPDATE SET last_seq_id = s.last_seq_id
            WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated)
                VALUES (s.name, s.last_seq_id, SYSDATETIMEOFFSET());
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    // Seeds the HighWaterMark row's liveness heartbeat (jasperfx#539). Requires
    // EnableExtendedProgressionTracking so the `heartbeat` column exists.
    private async Task seedHighWaterHeartbeatAsync(long sequence, DateTimeOffset heartbeat)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            MERGE [{_schemaName}].[pc_event_progression] AS t
            USING (SELECT 'HighWaterMark' AS name, CAST({sequence} AS bigint) AS last_seq_id,
                   CAST(@hb AS datetimeoffset) AS heartbeat) AS s
            ON t.name = s.name
            WHEN MATCHED THEN UPDATE SET last_seq_id = s.last_seq_id, heartbeat = s.heartbeat
            WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated, heartbeat)
                VALUES (s.name, s.last_seq_id, SYSDATETIMEOFFSET(), s.heartbeat);
            """;
        cmd.Parameters.Add(new SqlParameter("@hb", heartbeat));
        await cmd.ExecuteNonQueryAsync();
    }

    // ---- registration --------------------------------------------------------------------

    [Fact]
    public void registers_settings_timeprovider_tracker_and_check()
    {
        var builder = new FakeHealthCheckBuilderStub();
        builder.AddPolecatHighWaterHealthCheck(TimeSpan.FromSeconds(30));

        builder.Services.ShouldContain(x => x.ServiceType == typeof(HighWaterHealthCheckSettings));
        builder.Services.ShouldContain(x => x.ServiceType == typeof(TimeProvider));
        builder.Services.ShouldContain(x => x.ServiceType == typeof(HighWaterStateTracker));

        var provider = builder.Services.BuildServiceProvider();
        provider.GetServices<HealthCheckRegistration>()
            .ShouldContain(reg => reg.Name == nameof(HighWaterHealthCheck));
    }

    // ---- gating --------------------------------------------------------------------------

    [Fact]
    public async Task healthy_when_no_async_projections_even_if_events_pile_up()
    {
        // No async projection registered -> the high-water agent runs nowhere, so a stuck mark
        // is legitimate. The gate short-circuits before any database read.
        configure(_ => { });
        await appendEventsAsync(20);

        var result = await buildCheck().CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Fact]
    public async Task healthy_when_daemon_mode_is_disabled_even_if_mark_is_stuck()
    {
        configure(opts =>
        {
            opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.Disabled;
        });
        await _store!.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await appendEventsAsync(20);
        await seedHighWaterMarkAsync(1);

        var result = await buildCheck().CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    // ---- signal --------------------------------------------------------------------------

    [Fact]
    public async Task healthy_when_mark_is_caught_up()
    {
        configure(opts =>
        {
            opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.Solo;
        });
        await _store!.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await appendEventsAsync(20);
        var highest = await _store.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
        await seedHighWaterMarkAsync(highest);

        var result = await buildCheck().CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Fact]
    public async Task healthy_within_grace_window_when_mark_first_seen_behind()
    {
        configure(opts =>
        {
            opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.Solo;
        });
        await _store!.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await appendEventsAsync(20);
        await seedHighWaterMarkAsync(1);

        // First observation of the gap only starts the clock; not yet stale.
        var result = await buildCheck().CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Fact]
    public async Task unhealthy_when_mark_stuck_behind_past_threshold()
    {
        configure(opts =>
        {
            opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.Solo;
        });
        await _store!.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await appendEventsAsync(20);
        await seedHighWaterMarkAsync(1);

        var check = buildCheck(TimeSpan.FromSeconds(30));

        // First check records the stalled mark at _now.
        (await check.CheckHealthAsync(new HealthCheckContext())).Status.ShouldBe(HealthStatus.Healthy);

        // Advance the clock past the threshold with the mark still stuck.
        _timeProvider.Now = _now.AddSeconds(60);

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Unhealthy);
    }

    [Fact]
    public async Task healthy_when_mark_advances_before_threshold()
    {
        configure(opts =>
        {
            opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.Solo;
        });
        await _store!.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await appendEventsAsync(20);
        await seedHighWaterMarkAsync(1);

        var check = buildCheck(TimeSpan.FromSeconds(30));

        // First check: gap observed, clock starts.
        (await check.CheckHealthAsync(new HealthCheckContext())).Status.ShouldBe(HealthStatus.Healthy);

        // The mark advances (agent alive) and time moves forward past the threshold.
        await seedHighWaterMarkAsync(10);
        _timeProvider.Now = _now.AddSeconds(60);

        // The advance resets the clock, so it must not be reported stale.
        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    // ---- heartbeat primary signal (polecat#341) ------------------------------------------

    [Fact]
    public async Task healthy_when_heartbeat_is_fresh_even_though_mark_is_behind()
    {
        // ExtendedProgression on -> the heartbeat is the primary signal. A fresh heartbeat means the
        // agent is cycling, so a mark sitting behind the latest event is NOT unhealthy (unlike the gap
        // heuristic, which would trip here).
        configure(opts =>
        {
            opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.Solo;
            opts.Events.EnableExtendedProgressionTracking = true;
        });
        await _store!.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await appendEventsAsync(20);
        await seedHighWaterHeartbeatAsync(1, _now.AddSeconds(-5)); // mark stuck at 1, heartbeat only 5s old

        var result = await buildCheck(TimeSpan.FromSeconds(30)).CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Fact]
    public async Task unhealthy_when_heartbeat_is_stale_even_though_mark_is_caught_up()
    {
        // A stale heartbeat means the loop stopped cycling. This trips even when the mark is fully caught
        // up (gap == 0) — the case the gap heuristic is blind to.
        configure(opts =>
        {
            opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.Solo;
            opts.Events.EnableExtendedProgressionTracking = true;
        });
        await _store!.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await appendEventsAsync(20);
        var highest = await _store.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
        await seedHighWaterHeartbeatAsync(highest, _now.AddSeconds(-90)); // caught up, heartbeat 90s old

        var result = await buildCheck(TimeSpan.FromSeconds(30)).CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Unhealthy);
    }

    // ---- autoRestart remediation (polecat#341) -------------------------------------------

    [Fact]
    public async Task autorestart_triggers_a_restart_once_and_still_reports_unhealthy()
    {
        configure(opts =>
        {
            opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.Solo;
            opts.Events.EnableExtendedProgressionTracking = true;
        });
        await _store!.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await appendEventsAsync(20);
        var highest = await _store.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
        await seedHighWaterHeartbeatAsync(highest, _now.AddSeconds(-90));

        var daemon = Substitute.For<IProjectionDaemon>();
        var coordinator = new FakeCoordinator(daemon);

        var check = buildCheck(TimeSpan.FromSeconds(30), autoRestart: true, coordinator: coordinator);

        // First stale cycle: restart the loop, still report Unhealthy so an alert fires.
        (await check.CheckHealthAsync(new HealthCheckContext())).Status.ShouldBe(HealthStatus.Unhealthy);
        await daemon.Received(1).RestartHighWaterAgentAsync(Arg.Any<CancellationToken>());

        // Second cycle inside the same staleness window: still Unhealthy, but NOT restarted again (capped).
        (await check.CheckHealthAsync(new HealthCheckContext())).Status.ShouldBe(HealthStatus.Unhealthy);
        await daemon.Received(1).RestartHighWaterAgentAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task without_autorestart_no_restart_is_attempted()
    {
        configure(opts =>
        {
            opts.Projections.Add<HwFakeProjection>(ProjectionLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.Solo;
            opts.Events.EnableExtendedProgressionTracking = true;
        });
        await _store!.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await appendEventsAsync(20);
        var highest = await _store.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
        await seedHighWaterHeartbeatAsync(highest, _now.AddSeconds(-90));

        var daemon = Substitute.For<IProjectionDaemon>();
        var coordinator = new FakeCoordinator(daemon);

        var result = await buildCheck(TimeSpan.FromSeconds(30), coordinator: coordinator)
            .CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        await daemon.DidNotReceive().RestartHighWaterAgentAsync(Arg.Any<CancellationToken>());
    }

    // Minimal IProjectionCoordinator that hands back a single daemon — avoids mocking a ValueTask-returning
    // member (CA2012) while letting the daemon itself stay an NSubstitute for Received(...) assertions.
    private sealed class FakeCoordinator: IProjectionCoordinator
    {
        private readonly IProjectionDaemon _daemon;

        public FakeCoordinator(IProjectionDaemon daemon) => _daemon = daemon;

        public IProjectionDaemon DaemonForMainDatabase() => _daemon;

        public ValueTask<IProjectionDaemon> DaemonForDatabase(string databaseIdentifier) => new(_daemon);

        public ValueTask<IReadOnlyList<IProjectionDaemon>> AllDaemonsAsync() => new(new[] { _daemon });

        public Task PauseAsync() => Task.CompletedTask;

        public Task ResumeAsync() => Task.CompletedTask;

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class MutableTimeProvider: TimeProvider
    {
        public MutableTimeProvider(DateTimeOffset now) => Now = now;
        public DateTimeOffset Now { get; set; }
        public override DateTimeOffset GetUtcNow() => Now;
    }

    private sealed class FakeHealthCheckBuilderStub: IHealthChecksBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IHealthChecksBuilder Add(HealthCheckRegistration registration)
        {
            Services.AddSingleton(registration);
            return this;
        }
    }
}
