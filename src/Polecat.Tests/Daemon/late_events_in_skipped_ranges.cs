using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Polecat.Events.Daemon;
using Polecat.Events.Schema;
using Polecat.Internal.OpenTelemetry;
using Polecat.Projections;

namespace Polecat.Tests.Daemon;

public sealed class LateEventDatabases : HighWaterDatabases;

public sealed class PerTenantLateEventDatabases : HighWaterDatabases;

/// <summary>A logger that keeps every message with its level, for the facts below.</summary>
internal sealed class LateEventLogs : ILogger
{
    private readonly ConcurrentQueue<(LogLevel Level, string Message)> _messages = new();

    public IReadOnlyList<(LogLevel Level, string Message)> Messages => _messages.ToList();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter) => _messages.Enqueue((logLevel, formatter(state, exception)));
}

/// <summary>Listens to the <c>polecat.daemon.late_events</c> counter and keeps each count's skip reason.</summary>
internal sealed class LateEventMetrics : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly ConcurrentQueue<(string? EventType, string? Reason)> _recorded = new();

    public LateEventMetrics()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Name == OpenTelemetryOptions.LateEventsMetricName) listener.EnableMeasurementEvents(instrument);
        };

        _listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? eventType = null;
            string? reason = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "event_type") eventType = tag.Value as string;
                if (tag.Key == OpenTelemetryOptions.SkipReasonTag) reason = tag.Value as string;
            }

            for (var i = 0; i < value; i++) _recorded.Enqueue((eventType, reason));
        });

        _listener.Start();
    }

    public IReadOnlyList<(string? EventType, string? Reason)> Recorded => _recorded.ToList();

    public void Dispose() => _listener.Dispose();
}

/// <summary>
///     #1054 (marten#6007): an event that commits inside a sequence range the high water mark already skipped is
///     found, recorded, reported, and folded into the async single-stream projections that handle it.
/// </summary>
public class late_events_in_skipped_ranges(LateEventDatabases databases)
    : HighWaterLockingContext(databases), IClassFixture<LateEventDatabases>
{
    // Row versioning, so the operator's advance below counts its gap without waiting for the append in it
    private const DatabaseLocking Locking = DatabaseLocking.ReadCommittedSnapshot;
    private static readonly TimeSpan Threshold = TimeSpan.FromMilliseconds(100);

    private readonly LateEventLogs _logs = new();

    private Task<DocumentStore> storeAsync(Action<StoreOptions>? configure = null) => CreateStoreAsync(Locking, opts =>
    {
        opts.DaemonSettings.StaleSequenceThreshold = Threshold;
        configure?.Invoke(opts);
    });

    private PolecatHighWaterDetector detectorFor(DocumentStore store) => new(store.Options.EventGraph,
        store.Options.ConnectionString, store.Options.DaemonSettings, _logs, store.Options.ResiliencePipeline,
        new LateEventRecovery(store, store.Database, _logs));

    private static string TickAlias(DocumentStore store) => store.Options.EventGraph.EventMappingFor(typeof(Tick)).EventTypeName;

    [Fact]
    public void late_event_detection_is_on_by_default_and_restore_v5_defaults_turns_it_off()
    {
        var options = new StoreOptions();
        options.Events.EnableLateEventDetection.ShouldBeTrue();
        options.Projections.LateEventDetectionWindow.ShouldBe(TimeSpan.FromHours(1));
        options.Projections.LateEventProbeInterval.ShouldBe(TimeSpan.FromSeconds(10));

        tableNames(options).ShouldContain(HighWaterSkipsTable.TableName);
        tableNames(options).ShouldContain(HighWaterLateEventsTable.TableName);

        // Off, the two tables are left out of the configuration, so a database without them still matches it
        options.RestoreV5Defaults();
        options.Events.EnableLateEventDetection.ShouldBeFalse();
        tableNames(options).ShouldNotContain(HighWaterSkipsTable.TableName);
        tableNames(options).ShouldNotContain(HighWaterLateEventsTable.TableName);

        static string[] tableNames(StoreOptions options) =>
            ((Weasel.Core.Migrations.IFeatureSchema)new EventStoreFeatureSchema(options)).Objects
            .Select(x => x.Identifier.Name).ToArray();
    }

    [Fact]
    public async Task an_event_committed_after_a_capped_skip_is_recorded_reported_and_folded_into_its_stream_exactly_once()
    {
        using var metrics = new LateEventMetrics();

        var store = await storeAsync(opts =>
        {
            opts.DaemonSettings.StaleSequenceThreshold = TimeSpan.FromMilliseconds(250);
            opts.Projections.SkipStaleGapsDespiteLiveTransactionsAfter = TimeSpan.FromSeconds(1);
            opts.Projections.LateEventProbeInterval = TimeSpan.FromMilliseconds(100);
            opts.Projections.Snapshot<TickTotal>(SnapshotLifecycle.Async);
        });
        var streams = await StartStreamsAsync(store, 2); // seq 1, 2

        using var daemon = await store.BuildProjectionDaemonAsync(logger: _logs);
        await daemon.StartAllAsync();
        await store.Database.WaitForNonStaleProjectionDataAsync(TimeSpan.FromSeconds(30));

        // A slow append to the first stream: seq 3, held open
        await using (var slow = await InFlightAppend.StartAsync(store, streams[0]))
        {
            // Committed above it, so the mark is held under the open append until the cap runs out
            await AppendAsync(store, streams[1]); // seq 4

            await waitForAsync(async () => await progressAsync("HighWaterMark") >= 4,
                "the high water mark never skipped past the open append");
            await waitForAsync(async () => await progressAsync("TickTotal:All") >= 4,
                () => $"the projection shard never got past the skipped range: {progressionAsync().GetAwaiter().GetResult()}");

            // The skip is recorded with the sequence number that was still missing
            (await scalarStringAsync(
                    $"SELECT reason + ':' + gaps FROM dbo.{HighWaterSkipsTable.TableName} WHERE tenant_id = '' AND ending_sequence = 4"))
                .ShouldBe($"{nameof(HighWaterSkipReason.LiveTransaction)}:[[3,3]]");

            await slow.CommitAsync();
        }

        // Recorded in the ledger
        await waitForAsync(async () => await scalarAsync($"SELECT COUNT(*) FROM dbo.{HighWaterLateEventsTable.TableName}") == 1,
            "the late event was never recorded");

        (await scalarStringAsync(
                $"SELECT CONCAT(seq_id, '|', stream_id, '|', event_type, '|', skip_starting_sequence, '|', skip_ending_sequence, '|', skip_reason) FROM dbo.{HighWaterLateEventsTable.TableName}"))
            .ShouldBe($"3|{streams[0]}|{TickAlias(store)}|2|4|{nameof(HighWaterSkipReason.LiveTransaction)}");

        // Folded into the stream's document
        await waitForAsync(async () =>
        {
            await using var query = store.QuerySession();
            return (await query.LoadAsync<TickTotal>(streams[0], Ct))?.Count == 2;
        }, "the late event never reached the single stream projection");

        await waitForAsync(async () => await scalarAsync(
                $"SELECT COUNT(*) FROM dbo.{HighWaterLateEventsTable.TableName} WHERE repaired_projections LIKE '%TickTotal\"%'") == 1,
            "the repair was never recorded as done");

        // More probes neither report it again nor fold it in again
        await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
        await using (var query = store.QuerySession())
        {
            (await query.LoadAsync<TickTotal>(streams[0], Ct)).ShouldNotBeNull().Count.ShouldBe(2);
        }

        metrics.Recorded.ShouldContain((TickAlias(store), nameof(HighWaterSkipReason.LiveTransaction)));
        _logs.Messages.Count(x => x.Level == LogLevel.Warning && x.Message.StartsWith("Event 3 ")
                                                             && x.Message.Contains("committed inside the sequence range (2, 4]"))
            .ShouldBe(1);
        _logs.Messages.ShouldNotContain(x => x.Level == LogLevel.Error && x.Message.Contains("bug"));

        await daemon.StopAllAsync();
    }

    [Fact]
    public async Task an_event_filling_a_range_skipped_by_proof_is_logged_as_a_detector_bug()
    {
        using var metrics = new LateEventMetrics();

        var store = await storeAsync();
        var detector = detectorFor(store);
        var streams = await skipADeadGapByProofAsync(store, detector);

        // Nothing could have filled seq 3, so this is the detector's mistake to report
        await insertCommittedEventAsync(store, 3, streams[0]);

        await detector.LateEvents!.ProbeAsync(Ct);

        (await scalarStringAsync($"SELECT skip_reason FROM dbo.{HighWaterLateEventsTable.TableName} WHERE seq_id = 3"))
            .ShouldBe(nameof(HighWaterSkipReason.Proven));
        _logs.Messages.Count(x => x.Level == LogLevel.Error && x.Message.StartsWith("Event 3 ")
                                                           && x.Message.Contains("bug in Polecat's high water detection"))
            .ShouldBe(1);
        metrics.Recorded.ShouldContain((TickAlias(store), nameof(HighWaterSkipReason.Proven)));

        // Found once, whoever probes again
        await detector.LateEvents.ProbeAsync(Ct);
        await detectorFor(store).LateEvents!.ProbeAsync(Ct);
        _logs.Messages.Count(x => x.Message.StartsWith("Event 3 ")).ShouldBe(1);
        (await scalarAsync($"SELECT COUNT(*) FROM dbo.{HighWaterLateEventsTable.TableName}")).ShouldBe(1);
    }

    [Fact]
    public async Task a_skipped_range_that_stays_empty_past_the_window_is_closed_and_not_checked_again()
    {
        var store = await storeAsync(opts => opts.Projections.LateEventDetectionWindow = TimeSpan.FromMilliseconds(300));
        var detector = detectorFor(store);
        var streams = await skipADeadGapByProofAsync(store, detector);

        // Still inside the window: checked, nothing there, stays open
        await detector.LateEvents!.ProbeAsync(Ct);
        (await scalarAsync($"SELECT COUNT(*) FROM dbo.{HighWaterSkipsTable.TableName} WHERE closed_at IS NULL"))
            .ShouldBe(1);

        await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
        await detector.LateEvents.ProbeAsync(Ct);
        (await scalarAsync($"SELECT COUNT(*) FROM dbo.{HighWaterSkipsTable.TableName} WHERE closed_at IS NULL"))
            .ShouldBe(0);

        await insertCommittedEventAsync(store, 3, streams[0]);
        await detector.LateEvents.ProbeAsync(Ct);

        (await scalarAsync($"SELECT COUNT(*) FROM dbo.{HighWaterLateEventsTable.TableName}")).ShouldBe(0);
        _logs.Messages.ShouldNotContain(x => x.Message.StartsWith("Event 3 "));
    }

    [Fact]
    public async Task an_event_committed_after_a_manual_advance_is_recorded()
    {
        var store = await storeAsync();
        var streams = await StartStreamsAsync(store, 2); // seq 1, 2

        var detector = detectorFor(store);
        await detector.Detect(Ct); // mark 2

        await using (var slow = await InFlightAppend.StartAsync(store, streams[0])) // seq 3
        {
            await AppendAsync(store, streams[1]); // seq 4

            await store.Advanced.AdvanceHighWaterMarkToLatestAsync(Ct);

            await slow.CommitAsync();
        }

        (await scalarStringAsync(
                $"SELECT CONCAT(starting_sequence, ':', reason, ':', gaps) FROM dbo.{HighWaterSkipsTable.TableName} WHERE tenant_id = '' AND ending_sequence = 4"))
            .ShouldBe($"2:{nameof(HighWaterSkipReason.Manual)}:[[3,3]]");

        await detector.LateEvents!.ProbeAsync(Ct);

        (await scalarStringAsync($"SELECT skip_reason FROM dbo.{HighWaterLateEventsTable.TableName} WHERE seq_id = 3"))
            .ShouldBe(nameof(HighWaterSkipReason.Manual));
    }

    [Fact]
    public async Task nothing_is_recorded_when_no_range_was_skipped()
    {
        var store = await storeAsync();
        await StartStreamsAsync(store, 3);

        var detector = detectorFor(store);
        (await detector.Detect(Ct)).CurrentMark.ShouldBe(3);
        await detector.LateEvents!.ProbeAsync(Ct);

        (await scalarAsync($"SELECT COUNT(*) FROM dbo.{HighWaterSkipsTable.TableName}")).ShouldBe(0);
        (await scalarAsync($"SELECT COUNT(*) FROM dbo.{HighWaterLateEventsTable.TableName}")).ShouldBe(0);
    }

    // Two streams on seq 1 and 2, the mark at 2, seq 3 burned by a rolled-back append, seq 4 committed, and a
    // detector that skips (2, 4] because nothing in flight could fill 3
    private async Task<Guid[]> skipADeadGapByProofAsync(DocumentStore store, PolecatHighWaterDetector detector)
    {
        var streams = await StartStreamsAsync(store, 2);
        (await detector.Detect(Ct)).CurrentMark.ShouldBe(2);

        var rolledBack = await InFlightAppend.StartAsync(store, streams[0]);
        await AppendAsync(store, streams[1]);
        await rolledBack.RollBackAsync();

        (await detector.DetectInSafeZone(Ct)).CurrentMark.ShouldBe(2);
        await Task.Delay(Threshold + TimeSpan.FromMilliseconds(150), Ct);

        var skipped = await detector.DetectInSafeZone(Ct);
        skipped.CurrentMark.ShouldBe(4);
        skipped.IncludesSkipping.ShouldBeTrue();

        (await scalarStringAsync(
                $"SELECT CONCAT(starting_sequence, ':', reason, ':', gaps) FROM dbo.{HighWaterSkipsTable.TableName} WHERE tenant_id = '' AND ending_sequence = 4"))
            .ShouldBe($"2:{nameof(HighWaterSkipReason.Proven)}:[[3,3]]");

        return streams;
    }

    // What a late commit of seq 3 looks like to the probe, without an append to hold open
    private Task insertCommittedEventAsync(DocumentStore store, long sequence, Guid stream) => ExecuteAsync(
        ConnectionString, $"""
            SET IDENTITY_INSERT dbo.pc_events ON;
            INSERT INTO dbo.pc_events (seq_id, id, stream_id, version, data, type, tenant_id)
            VALUES ({sequence}, NEWID(), '{stream}', 99, '{"{}"}', '{TickAlias(store)}', '{StorageConstants.DefaultTenantId}');
            SET IDENTITY_INSERT dbo.pc_events OFF;
            """);

    private Task<long> progressAsync(string name)
        => scalarAsync($"SELECT ISNULL(MAX(last_seq_id), 0) FROM dbo.pc_event_progression WHERE name = '{name}' OR name LIKE '%.{name}'");

    private async Task<long> scalarAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(Ct) ?? 0L);
    }

    private async Task<string?> scalarStringAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync(Ct) as string;
    }

    private Task<string?> progressionAsync()
        => scalarStringAsync("SELECT STRING_AGG(CONCAT(name, '@', last_seq_id), ', ') FROM dbo.pc_event_progression");

    private static Task waitForAsync(Func<Task<bool>> condition, string failure)
        => waitForAsync(condition, () => failure);

    private static async Task waitForAsync(Func<Task<bool>> condition, Func<string> failure)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(20);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(100, Ct);
        }

        throw new ShouldAssertException(failure());
    }
}

/// <summary>
///     #1054 under per-tenant partitioned events: a tenant's skip is recorded under that tenant when the coordinator
///     persists the mark that carries it, an event that later commits inside it is found within that tenant, and
///     removing the tenant takes its rows with it.
/// </summary>
public class per_tenant_late_events_in_skipped_ranges(PerTenantLateEventDatabases databases)
    : HighWaterLockingContext(databases), IClassFixture<PerTenantLateEventDatabases>
{
    private const string Tenant = "Late";

    private readonly LateEventLogs _logs = new();

    private Task<DocumentStore> storeAsync() => CreateStoreAsync(DatabaseLocking.ReadCommittedSnapshot, opts =>
    {
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
        opts.DaemonSettings.StaleSequenceThreshold = TimeSpan.FromMilliseconds(100);
    });

    [Fact]
    public async Task a_tenants_skip_is_recorded_for_that_tenant_and_a_late_event_inside_it_is_found()
    {
        var store = await storeAsync();
        await StartStreamsAsync(store, 2, Tenant); // the tenant's seq 1, 2

        var detector = new PolecatHighWaterDetector(store.Options.EventGraph, store.Options.ConnectionString,
            store.Options.DaemonSettings, _logs, store.Options.ResiliencePipeline,
            new LateEventRecovery(store, store.Database, _logs));
        IHighWaterDetector coordinatorView = detector;

        // Persisted a while ago, so the tenant reads as stale on its first sight of the gap
        await coordinatorView.MarkHighWaterForTenantAsync(Tenant, 2, DateTimeOffset.UtcNow.AddMinutes(-1), Ct);

        var ordinal = await ordinalAsync();
        await ExecuteAsync(ConnectionString, $"SELECT NEXT VALUE FOR dbo.pc_events_sequence_{ordinal};"); // seq 3 burned
        var stream = (await StartStreamsAsync(store, 1, Tenant))[0]; // seq 4

        var vector = await detector.DetectForTenantsAsync([Tenant], Ct);
        vector.TryGetStatistics(Tenant, out var stats).ShouldBeTrue();
        stats.CurrentMark.ShouldBe(4);
        stats.IncludesSkipping.ShouldBeTrue();

        // Not recorded until the mark that carries it is
        (await scalarStringAsync($"SELECT gaps FROM dbo.{HighWaterSkipsTable.TableName} WHERE tenant_id = '{Tenant}'"))
            .ShouldBeNull();

        await coordinatorView.MarkHighWaterForTenantAsync(Tenant, 4, DateTimeOffset.UtcNow, Ct);

        (await scalarStringAsync(
                $"SELECT CONCAT(starting_sequence, ':', reason, ':', gaps) FROM dbo.{HighWaterSkipsTable.TableName} WHERE tenant_id = '{Tenant}' AND ending_sequence = 4"))
            .ShouldBe($"2:{nameof(HighWaterSkipReason.Proven)}:[[3,3]]");

        await ExecuteAsync(ConnectionString, $$"""
            INSERT INTO dbo.pc_events (seq_id, id, stream_id, version, data, type, tenant_id, tenant_ordinal)
            VALUES (3, NEWID(), '{{stream}}', 99, '{}', 'tick', '{{Tenant}}', {{ordinal}});
            """);

        await detector.LateEvents!.ProbeAsync(Ct);

        (await scalarStringAsync(
                $"SELECT CONCAT(skip_ending_sequence, ':', skip_reason) FROM dbo.{HighWaterLateEventsTable.TableName} WHERE tenant_id = '{Tenant}' AND seq_id = 3"))
            .ShouldBe($"4:{nameof(HighWaterSkipReason.Proven)}");
    }

    [Fact]
    public async Task removing_a_tenant_removes_its_high_water_skips_and_late_events()
    {
        var store = await storeAsync();
        await store.Advanced.AddPolecatManagedTenantsAsync(Ct, "skip_rem", "skip_rem_kept");

        await ExecuteAsync(ConnectionString, $"""
            INSERT INTO dbo.{HighWaterSkipsTable.TableName} (ending_sequence, tenant_id, starting_sequence, gaps)
            VALUES (10, 'skip_rem', 5, '[[6,6]]'), (10, 'skip_rem_kept', 5, '[[6,6]]');
            INSERT INTO dbo.{HighWaterLateEventsTable.TableName} (seq_id, tenant_id, stream_id, event_type, skip_starting_sequence, skip_ending_sequence)
            VALUES (6, 'skip_rem', 'a', 'x', 5, 10), (6, 'skip_rem_kept', 'b', 'x', 5, 10);
            """);

        await store.Advanced.RemovePolecatManagedTenantsAsync(["skip_rem"], Ct);

        (await scalarStringAsync($"SELECT STRING_AGG(tenant_id, ',') FROM dbo.{HighWaterSkipsTable.TableName}"))
            .ShouldBe("skip_rem_kept");
        (await scalarStringAsync($"SELECT STRING_AGG(tenant_id, ',') FROM dbo.{HighWaterLateEventsTable.TableName}"))
            .ShouldBe("skip_rem_kept");
    }

    private async Task<int> ordinalAsync()
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT ordinal FROM dbo.pc_tenant_partitions WHERE tenant_id = '{Tenant}';";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private async Task<string?> scalarStringAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync(Ct) as string;
    }
}
