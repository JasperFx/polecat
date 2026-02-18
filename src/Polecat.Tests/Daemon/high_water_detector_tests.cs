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

    public override async Task InitializeAsync()
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

        // First, run normal detect to set a baseline
        var detector = CreateDetector();
        await detector.Detect(CancellationToken.None);

        // Now manually set last_updated to be old enough to be stale
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            UPDATE [dbo].[pc_event_progression]
            SET last_updated = DATEADD(SECOND, -10, SYSDATETIMEOFFSET())
            WHERE name = 'HighWaterMark';
            """;
        await cmd.ExecuteNonQueryAsync();

        var stats = await detector.DetectInSafeZone(CancellationToken.None);

        // Should have skipped past the gap to the max
        stats.CurrentMark.ShouldBe(seqIds.Last());
        stats.IncludesSkipping.ShouldBeTrue();
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
        var result = await cmd.ExecuteScalarAsync();
        result.ShouldNotBeNull();
        ((long)result!).ShouldBe(seqIds.Last());
    }

    private PolecatHighWaterDetector CreateDetector()
    {
        return new PolecatHighWaterDetector(
            theStore.Database.Events,
            theStore.Options.ConnectionString,
            theStore.Options.DaemonSettings,
            NullLogger<PolecatHighWaterDetector>.Instance);
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
