using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

[Collection("integration")]
public class high_water_detector_tests : IntegrationContext
{
    public high_water_detector_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        // Clean slate for each test
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DELETE FROM [dbo].[pc_events];
            DELETE FROM [dbo].[pc_streams];
            DELETE FROM [dbo].[pc_event_progression];
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task detect_with_no_events()
    {
        var detector = CreateDetector();
        var stats = await detector.Detect(CancellationToken.None);

        stats.CurrentMark.ShouldBe(0);
        stats.HighestSequence.ShouldBe(0);
    }

    [Fact]
    public async Task detect_all_contiguous()
    {
        var eventCount = 5;
        var seqIds = await InsertContiguousEventsAsync(eventCount);

        var detector = CreateDetector();
        var stats = await detector.Detect(CancellationToken.None);

        stats.HighestSequence.ShouldBe(seqIds.Last());
        stats.CurrentMark.ShouldBe(seqIds.Last());
    }

    [Fact]
    public async Task detect_stops_at_gap()
    {
        // Insert 10 events, then delete some to create a gap
        var seqIds = await InsertContiguousEventsAsync(10);

        // Delete events at index 3, 4, 5 (the 4th, 5th, 6th events)
        await DeleteEventsBySeqIdAsync(seqIds[3], seqIds[4], seqIds[5]);

        var detector = CreateDetector();
        var stats = await detector.Detect(CancellationToken.None);

        // Should stop before the gap (at the 3rd event)
        stats.CurrentMark.ShouldBe(seqIds[2]);
        stats.HighestSequence.ShouldBe(seqIds.Last());
    }

    [Fact]
    public async Task detect_in_safe_zone_advances_past_gap()
    {
        // Insert events and create a gap
        var seqIds = await InsertContiguousEventsAsync(10);
        await DeleteEventsBySeqIdAsync(seqIds[3], seqIds[4], seqIds[5]);

        // #791: the gap is aged from when this detector first saw the mark held under it, not from
        // the HighWaterMark row's last_updated, which every detection cycle re-stamps.
        var detector = CreateDetector(staleSequenceThreshold: TimeSpan.FromMilliseconds(500));
        (await detector.Detect(CancellationToken.None)).CurrentMark.ShouldBe(seqIds[2]);

        await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        var stats = await detector.DetectInSafeZone(CancellationToken.None);

        // Should have skipped past the gap to the max
        stats.CurrentMark.ShouldBe(seqIds.Last());
        stats.IncludesSkipping.ShouldBeTrue();
    }

    /// <summary>
    ///     #791. A running daemon polls the high water mark every cycle, and since #705 every cycle
    ///     re-stamps the HighWaterMark row's last_updated, caught up or not. The stale-gap skip used to
    ///     age a gap from that column, so on a polled store an inter-event gap never got older than one
    ///     cycle and was never skipped: catch-up waited out its five-minute backstop under it.
    /// </summary>
    [Fact]
    public async Task an_inter_event_gap_is_skipped_while_every_cycle_restamps_the_row()
    {
        var seqIds = await InsertContiguousEventsAsync(10);
        await DeleteEventsBySeqIdAsync(seqIds[3], seqIds[4], seqIds[5]);

        var detector = CreateDetector(staleSequenceThreshold: TimeSpan.FromSeconds(1));

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var stats = await detector.DetectInSafeZone(CancellationToken.None);
        while (stats.CurrentMark < seqIds.Last() && stopwatch.Elapsed < TimeSpan.FromSeconds(10))
        {
            // What HighWaterAgent.CheckNowAsync does: DetectInSafeZone on every pass, each of which
            // re-stamps the row. (Not Detect: it never crosses a gap, which is the safe zone's job. #833.)
            await Task.Delay(TimeSpan.FromMilliseconds(200), TestContext.Current.CancellationToken);
            stats = await detector.DetectInSafeZone(CancellationToken.None);
        }

        stats.CurrentMark.ShouldBe(seqIds.Last());
        stats.IncludesSkipping.ShouldBeTrue();
        stopwatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(10));
    }

    /// <summary>
    ///     #791, the other direction: a stale last_updated is no reason to skip. It is what a store
    ///     nobody was polling looks like, so trusting it would skip a gap that has only just opened —
    ///     possibly an append still in flight — the moment a daemon starts.
    /// </summary>
    [Fact]
    public async Task a_gap_is_not_skipped_because_the_persisted_row_is_old()
    {
        var seqIds = await InsertContiguousEventsAsync(10);
        await DeleteEventsBySeqIdAsync(seqIds[3], seqIds[4], seqIds[5]);

        var detector = CreateDetector(staleSequenceThreshold: TimeSpan.FromSeconds(30));
        await detector.Detect(CancellationToken.None);

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            UPDATE [dbo].[pc_event_progression]
            SET last_updated = DATEADD(HOUR, -1, SYSDATETIMEOFFSET())
            WHERE name = 'HighWaterMark';
            """;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        var stats = await detector.DetectInSafeZone(CancellationToken.None);

        stats.CurrentMark.ShouldBe(seqIds[2]);
        stats.IncludesSkipping.ShouldBeFalse();
    }

    [Fact]
    public async Task mark_persists_in_database()
    {
        var seqIds = await InsertContiguousEventsAsync(5);

        var detector = CreateDetector();
        await detector.Detect(CancellationToken.None);

        // Verify the high water mark was persisted
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT last_seq_id FROM [dbo].[pc_event_progression]
            WHERE name = 'HighWaterMark';
            """;
        var result = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        result.ShouldNotBeNull();
        ((long)result!).ShouldBe(seqIds.Last());
    }

    private PolecatHighWaterDetector CreateDetector(TimeSpan? staleSequenceThreshold = null)
    {
        var settings = theStore.Options.DaemonSettings;
        if (staleSequenceThreshold.HasValue)
        {
            settings = new JasperFx.Events.Daemon.DaemonSettings
            {
                StaleSequenceThreshold = staleSequenceThreshold.Value
            };
        }

        return new PolecatHighWaterDetector(
            theStore.Database.Events,
            theStore.Options.ConnectionString,
            settings,
            NullLogger<PolecatHighWaterDetector>.Instance,
            theStore.Options.ResiliencePipeline);
    }

    private async Task<List<long>> InsertContiguousEventsAsync(int count)
    {
        var seqIds = new List<long>();
        for (var i = 0; i < count; i++)
        {
            var streamId = Guid.NewGuid();
            theSession.Events.StartStream(streamId, new QuestStarted($"Quest {i + 1}"));
            await theSession.SaveChangesAsync();
        }

        // Read back the actual seq_ids
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT seq_id FROM [dbo].[pc_events] ORDER BY seq_id;";
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            seqIds.Add(reader.GetInt64(0));
        }

        return seqIds;
    }

    private async Task DeleteEventsBySeqIdAsync(params long[] seqIds)
    {
        await using var conn = await OpenConnectionAsync();
        foreach (var seqId in seqIds)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM [dbo].[pc_events] WHERE seq_id = @seqId;";
            cmd.Parameters.AddWithValue("@seqId", seqId);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
