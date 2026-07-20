using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
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

    private HighWaterHealthCheck buildCheck(TimeSpan? staleThreshold = null, long minimumGap = 1) =>
        new(_store!, new HighWaterHealthCheckSettings(staleThreshold ?? TimeSpan.FromSeconds(30), minimumGap),
            _timeProvider, _tracker);

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
