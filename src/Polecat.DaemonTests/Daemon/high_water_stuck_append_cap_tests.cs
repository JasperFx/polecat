using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using JasperFx.Events.Projections;
using Microsoft.Extensions.Logging;
using Polecat.Events.Daemon;

namespace Polecat.Tests.Daemon;

public sealed class StuckAppendDatabases : HighWaterDatabases;

public sealed class TenantStuckAppendDatabases : HighWaterDatabases;

/// <summary>
///     A logger that keeps what it is given, so a fact can read the message a skip leaves. It is the only
///     trace a skipped append leaves anywhere.
/// </summary>
internal sealed class CapturedDetectorLogs : ILogger
{
    private readonly ConcurrentQueue<(LogLevel Level, string Message)> _messages = new();

    public IEnumerable<string> Warnings => _messages.Where(x => x.Level == LogLevel.Warning).Select(x => x.Message);

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter) => _messages.Enqueue((logLevel, formatter(state, exception)));
}

/// <summary>
///     #852. Since #833 an append in flight holds the mark for as long as it stays open, on every database
///     setting, so one transaction left open stalls every async projection and subscription.
///     <c>SkipStaleGapsDespiteLiveTransactionsAfter</c> bounds that for anyone who would sooner risk the
///     event than the stall.
/// </summary>
public class high_water_stuck_append_cap_tests(StuckAppendDatabases databases)
    : HighWaterLockingContext(databases), IClassFixture<StuckAppendDatabases>
{
    private static readonly TimeSpan Cap = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan Threshold = TimeSpan.FromMilliseconds(100);

    private readonly CapturedDetectorLogs _logs = new();

    private Task<DocumentStore> CreateCappedStoreAsync(DatabaseLocking locking, TimeSpan? cap = null)
        => CreateStoreAsync(locking, opts =>
        {
            opts.Projections.SkipStaleGapsDespiteLiveTransactionsAfter = cap ?? Cap;
            opts.DaemonSettings.StaleSequenceThreshold = Threshold;
        });

    private PolecatHighWaterDetector DetectorWithLogs(DocumentStore store) => new(store.Options.EventGraph,
        store.Options.ConnectionString, store.Options.DaemonSettings, _logs, store.Options.ResiliencePipeline);

    /// <summary>What the agent does while a mark is held: the safe zone, once a threshold.</summary>
    private static async Task<HighWaterStatistics> SafeZoneForAsync(PolecatHighWaterDetector detector,
        TimeSpan duration)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        HighWaterStatistics stats;
        do
        {
            await Task.Delay(Threshold + TimeSpan.FromMilliseconds(50), Ct);
            stats = await detector.DetectInSafeZone(Ct);
            stats.IncludesSkipping.ShouldBeFalse();
        } while (stopwatch.Elapsed < duration);

        return stats;
    }

    private static async Task<HighWaterStatistics> SafeZoneUntilItSkipsAsync(PolecatHighWaterDetector detector)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        while (stopwatch.Elapsed < Cap * 5)
        {
            await Task.Delay(Threshold + TimeSpan.FromMilliseconds(50), Ct);

            var stats = await detector.DetectInSafeZone(Ct);
            if (stats.IncludesSkipping) return stats;
        }

        throw new TimeoutException("Nothing was skipped");
    }

    [Fact]
    public void the_cap_is_off_unless_it_is_asked_for()
    {
        new StoreOptions().Projections.SkipStaleGapsDespiteLiveTransactionsAfter.ShouldBeNull();
    }

    /// <summary>
    ///     With the threshold far below the cap, this fact has the observation that ages the gap thrown away
    ///     several times before the cap arrives, which is the mistake a cap read from it would make.
    /// </summary>
    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task a_stuck_append_holds_the_mark_until_the_cap_and_no_longer(DatabaseLocking locking)
    {
        var store = await CreateCappedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2);

        var detector = DetectorWithLogs(store);
        await detector.Detect(Ct);

        await using var stuck = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await AppendAsync(store, streams[1]); // seq 4

        await detector.Detect(Ct); // the clock starts here

        var held = await SafeZoneForAsync(detector, Cap / 2);
        held.CurrentMark.ShouldBe(2, "the cap is not up");
        _logs.Warnings.ShouldBeEmpty();

        var skipped = await SafeZoneUntilItSkipsAsync(detector);
        skipped.CurrentMark.ShouldBe(4);

        // ...and the warning is the only record of it, so it has to say what went.
        var warning = _logs.Warnings.ShouldHaveSingleItem();
        warning.ShouldContain("(2, 4]");
        warning.ShouldContain(nameof(StoreOptions.Projections.SkipStaleGapsDespiteLiveTransactionsAfter));
    }

    [Fact]
    public async Task detect_never_crosses_an_append_in_flight_however_long_it_has_been_open()
    {
        var store = await CreateCappedStoreAsync(DatabaseLocking.ReadCommittedSnapshot, TimeSpan.FromMilliseconds(100));
        var streams = await StartStreamsAsync(store, 2);

        var detector = DetectorFor(store);
        await detector.Detect(Ct);

        await using var stuck = await InFlightAppend.StartAsync(store, streams[0]);
        await AppendAsync(store, streams[1]);

        await detector.Detect(Ct);
        await Task.Delay(TimeSpan.FromMilliseconds(300), Ct);

        (await detector.Detect(Ct)).CurrentMark.ShouldBe(2);
    }

    [Fact]
    public async Task an_append_that_lands_inside_the_cap_is_never_skipped()
    {
        var store = await CreateCappedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2);

        var detector = DetectorWithLogs(store);
        await detector.Detect(Ct);

        await using var slow = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await AppendAsync(store, streams[1]); // seq 4

        await detector.Detect(Ct);
        await SafeZoneForAsync(detector, Cap / 2);

        await slow.CommitAsync();

        var after = await detector.DetectInSafeZone(Ct);
        after.CurrentMark.ShouldBe(4);
        after.IncludesSkipping.ShouldBeFalse();
        _logs.Warnings.ShouldBeEmpty();
    }

    /// <summary>
    ///     The clock belongs to the position, so a mark that moves up to the next stuck append has given it
    ///     the whole cap too. Otherwise a long-held mark would spend its age on an append that opened a moment
    ///     ago.
    /// </summary>
    [Fact]
    public async Task a_later_append_gets_a_cap_of_its_own()
    {
        var store = await CreateCappedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 3);

        var detector = DetectorWithLogs(store);
        await detector.Detect(Ct);

        await using var first = await InFlightAppend.StartAsync(store, streams[0]); // seq 4
        await AppendAsync(store, streams[2]); // seq 5

        await detector.Detect(Ct);
        await Task.Delay(Cap / 2, Ct);

        await using var second = await InFlightAppend.StartAsync(store, streams[1]); // seq 6
        await AppendAsync(store, streams[2]); // seq 7

        var skipped = await SafeZoneUntilItSkipsAsync(detector);
        skipped.CurrentMark.ShouldBe(5, "seq 6 was handed out after the clock started on seq 4");

        // Now held at 5 by 6, on a clock that has only just started.
        (await SafeZoneForAsync(detector, Cap / 2)).CurrentMark.ShouldBe(5);

        (await SafeZoneUntilItSkipsAsync(detector)).CurrentMark.ShouldBe(7);
    }

    /// <summary>A restart starts the clock again, which errs towards holding.</summary>
    [Fact]
    public async Task a_new_detector_does_not_inherit_the_age_of_a_stall()
    {
        var store = await CreateCappedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2);

        await DetectorFor(store).Detect(Ct);

        await using var stuck = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await AppendAsync(store, streams[1]); // seq 4

        await DetectorWithLogs(store).Detect(Ct);

        await Task.Delay(Cap, Ct);

        var restarted = DetectorWithLogs(store);
        (await restarted.DetectInSafeZone(Ct)).CurrentMark.ShouldBe(2);
        (await SafeZoneForAsync(restarted, Cap / 2)).CurrentMark.ShouldBe(2);
    }

    /// <summary>
    ///     Pinned because it is the whole of what the option costs. The event committed, a read of the stream
    ///     returns it, and the daemon will never see it.
    /// </summary>
    [Fact]
    public async Task an_append_that_commits_after_the_cap_is_behind_the_mark()
    {
        var store = await CreateCappedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2);

        var detector = DetectorWithLogs(store);
        await detector.Detect(Ct);

        await using var stuck = await InFlightAppend.StartAsync(store, streams[0]); // seq 3
        await AppendAsync(store, streams[1]); // seq 4

        await detector.Detect(Ct);
        (await SafeZoneUntilItSkipsAsync(detector)).CurrentMark.ShouldBe(4);

        await stuck.CommitAsync();

        await using var query = store.QuerySession();
        (await query.Events.FetchStreamAsync(streams[0], token: Ct)).Select(x => x.Sequence).ShouldContain(3);

        var after = await detector.Detect(Ct);
        after.CurrentMark.ShouldBe(4);
        after.LastMark.ShouldBe(4, "seq 3 is below the mark for good");
    }

    /// <summary>The stall as a user meets it, and the way out of it.</summary>
    [Fact]
    public async Task a_running_daemon_gets_past_a_stuck_append()
    {
        var store = await CreateStoreAsync(DatabaseLocking.ReadCommittedSnapshot, opts =>
        {
            opts.Projections.Snapshot<TickTotal>(SnapshotLifecycle.Async);
            opts.Projections.SkipStaleGapsDespiteLiveTransactionsAfter = TimeSpan.FromSeconds(1);
            opts.DaemonSettings.StaleSequenceThreshold = TimeSpan.FromMilliseconds(250);
        });
        var streams = await StartStreamsAsync(store, 2);

        using var daemon = await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        await TotalReachesAsync(store, streams[1], 1);

        await using var stuck = await InFlightAppend.StartAsync(store, streams[0]);
        await AppendAsync(store, streams[1]);

        // Held, and then, without anyone doing anything, not.
        await TotalReachesAsync(store, streams[1], 2);

        await daemon.StopAllAsync();
    }

    private static async Task TotalReachesAsync(DocumentStore store, Guid stream, int expected)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var count = 0;

        while (stopwatch.Elapsed < TimeSpan.FromSeconds(30))
        {
            await using var query = store.QuerySession();
            count = (await query.LoadAsync<TickTotal>(stream, Ct))?.Count ?? 0;
            if (count == expected) return;

            await Task.Delay(TimeSpan.FromMilliseconds(100), Ct);
        }

        count.ShouldBe(expected);
    }
}

/// <summary>#852 on the path <c>UseTenantPartitionedEvents</c> uses, where a tenant's mark has a cap of its own.</summary>
public class per_tenant_high_water_stuck_append_cap_tests(TenantStuckAppendDatabases databases)
    : HighWaterLockingContext(databases), IClassFixture<TenantStuckAppendDatabases>
{
    private const string Tenant = "Red";

    private static readonly TimeSpan Cap = TimeSpan.FromMilliseconds(800);

    private readonly CapturedDetectorLogs _logs = new();

    private Task<DocumentStore> CreateCappedStoreAsync(DatabaseLocking locking) => CreateStoreAsync(locking, opts =>
    {
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
        opts.Projections.SkipStaleGapsDespiteLiveTransactionsAfter = Cap;
    });

    private PolecatHighWaterDetector DetectorWithLogs(DocumentStore store) => new(store.Options.EventGraph,
        store.Options.ConnectionString, store.Options.DaemonSettings, _logs, store.Options.ResiliencePipeline);

    private static Task PersistMarkAsync(DocumentStore store, long mark, DateTimeOffset stamp)
        => ((IHighWaterDetector)DetectorFor(store)).MarkHighWaterForTenantAsync(Tenant, mark, stamp, Ct);

    private static async Task<HighWaterStatistics> DetectAsync(PolecatHighWaterDetector detector)
    {
        var vector = await detector.DetectForTenantsAsync([Tenant], Ct);
        vector.TryGetStatistics(Tenant, out var stats).ShouldBeTrue();
        return stats;
    }

    [Theory]
    [InlineData(DatabaseLocking.LockingReadCommitted)]
    [InlineData(DatabaseLocking.ReadCommittedSnapshot)]
    [InlineData(DatabaseLocking.OptimizedLocking)]
    public async Task a_tenants_stuck_append_holds_its_mark_until_the_cap_and_no_longer(DatabaseLocking locking)
    {
        var store = await CreateCappedStoreAsync(locking);
        var streams = await StartStreamsAsync(store, 2, Tenant);
        await PersistMarkAsync(store, 2, DateTimeOffset.UtcNow);

        await using var stuck = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 3
        await AppendAsync(store, streams[1], Tenant); // seq 4

        var detector = DetectorWithLogs(store);

        var held = await DetectAsync(detector);
        held.CurrentMark.ShouldBe(2);
        held.IncludesSkipping.ShouldBeFalse();

        await Task.Delay(Cap + TimeSpan.FromMilliseconds(100), Ct);

        var skipped = await DetectAsync(detector);
        skipped.CurrentMark.ShouldBe(4);
        skipped.IncludesSkipping.ShouldBeTrue();

        var warning = _logs.Warnings.ShouldHaveSingleItem();
        warning.ShouldContain(Tenant);
        warning.ShouldContain("(2, 4]");
    }

    /// <summary>
    ///     <c>last_updated</c> says this tenant has been idle for five minutes, which is far beyond the cap and
    ///     says nothing about an append that opened just now. Reading the cap from it would step straight over
    ///     a live append on the first poll that ever saw it.
    /// </summary>
    [Fact]
    public async Task an_idle_tenant_does_not_skip_a_new_append_on_first_sight()
    {
        var store = await CreateCappedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2, Tenant);
        await PersistMarkAsync(store, 2, DateTimeOffset.UtcNow.AddMinutes(-5));

        await using var stuck = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 3
        await AppendAsync(store, streams[1], Tenant); // seq 4

        var stats = await DetectAsync(DetectorWithLogs(store));
        stats.CurrentMark.ShouldBe(2);
        stats.IncludesSkipping.ShouldBeFalse();
    }

    /// <summary>
    ///     No persisted mark means no <c>last_updated</c>, so nothing here ever reads as stale. The cap is the
    ///     only thing that could release this tenant, so it can't depend on that.
    /// </summary>
    [Fact]
    public async Task a_tenant_with_no_persisted_mark_is_released_too()
    {
        var store = await CreateCappedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2, Tenant);
        await ExecuteAsync(ConnectionString, "DELETE FROM dbo.pc_events;");

        await using var stuck = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 3
        await AppendAsync(store, streams[1], Tenant); // seq 4

        var detector = DetectorWithLogs(store);
        (await DetectAsync(detector)).CurrentMark.ShouldBe(0);

        await Task.Delay(Cap + TimeSpan.FromMilliseconds(100), Ct);

        (await DetectAsync(detector)).CurrentMark.ShouldBe(4);
    }

    [Fact]
    public async Task a_tenant_that_catches_up_starts_again_from_nothing()
    {
        var store = await CreateCappedStoreAsync(DatabaseLocking.ReadCommittedSnapshot);
        var streams = await StartStreamsAsync(store, 2, Tenant);
        await PersistMarkAsync(store, 2, DateTimeOffset.UtcNow);

        var detector = DetectorWithLogs(store);

        await using var first = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 3
        await AppendAsync(store, streams[1], Tenant); // seq 4

        await DetectAsync(detector);
        await first.CommitAsync();

        var caughtUp = await DetectAsync(detector);
        caughtUp.CurrentMark.ShouldBe(4);
        await PersistMarkAsync(store, 4, DateTimeOffset.UtcNow);
        await DetectAsync(detector);

        await Task.Delay(Cap + TimeSpan.FromMilliseconds(100), Ct);

        await using var second = await InFlightAppend.StartAsync(store, streams[0], Tenant); // seq 5
        await AppendAsync(store, streams[1], Tenant); // seq 6

        var held = await DetectAsync(detector);
        held.CurrentMark.ShouldBe(4, "this gap is new, whatever the last one's age was");
        held.IncludesSkipping.ShouldBeFalse();
    }
}
