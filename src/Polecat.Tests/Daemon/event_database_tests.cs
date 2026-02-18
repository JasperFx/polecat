using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

[Collection("integration")]
public class event_database_tests : IntegrationContext
{
    public event_database_tests(DefaultStoreFixture fixture) : base(fixture)
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
    public async Task fetch_highest_with_no_events()
    {
        var highest = await theStore.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
        highest.ShouldBe(0);
    }

    [Fact]
    public async Task fetch_highest_after_inserts()
    {
        // Insert 5 events
        for (var i = 0; i < 5; i++)
        {
            var streamId = Guid.NewGuid();
            theSession.Events.StartStream(streamId, new QuestStarted($"Quest {i + 1}"));
            await theSession.SaveChangesAsync();
        }

        var highest = await theStore.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
        highest.ShouldBeGreaterThan(0);

        // Verify we have exactly 5 events in the table
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [dbo].[pc_events];";
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        count.ShouldBe(5);
    }

    [Fact]
    public async Task find_floor_at_time()
    {
        // Insert events
        for (var i = 0; i < 3; i++)
        {
            var streamId = Guid.NewGuid();
            theSession.Events.StartStream(streamId, new QuestStarted($"Quest {i + 1}"));
            await theSession.SaveChangesAsync();
        }

        var highest = await theStore.Database.FetchHighestEventSequenceNumber(CancellationToken.None);

        // Get the timestamp of the last event
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT MAX(timestamp) FROM [dbo].[pc_events];";
        var maxTimestamp = (DateTimeOffset)(await cmd.ExecuteScalarAsync())!;

        // Find floor at that timestamp — should include all events up to max
        var floor = await theStore.Database.FindEventStoreFloorAtTimeAsync(
            maxTimestamp, CancellationToken.None);

        floor.ShouldNotBeNull();
        floor.Value.ShouldBe(highest);
    }
}
