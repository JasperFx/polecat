using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

[Collection("integration")]
public class event_loader_tests : IntegrationContext
{
    public event_loader_tests(DefaultStoreFixture fixture) : base(fixture)
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
    public async Task load_returns_correct_page()
    {
        await InsertEventsAsync(5);
        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        page.Count.ShouldBe(5);
        page.Floor.ShouldBe(0);
    }

    [Fact]
    public async Task load_respects_batch_size()
    {
        await InsertEventsAsync(10);
        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 3);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        page.Count.ShouldBe(3);
    }

    [Fact]
    public async Task load_respects_floor_and_ceiling()
    {
        await InsertEventsAsync(10);

        // Get the actual seq_ids so we can set floor/ceiling precisely
        var allSeqIds = await GetAllSeqIdsAsync();
        allSeqIds.Count.ShouldBe(10);

        // Load only events from seq_id 3 to 7 (exclusive floor, inclusive ceiling)
        var loader = CreateLoader();
        var request = CreateRequest(allSeqIds[2], allSeqIds[6], batchSize: 100);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        // Should get events at positions 3, 4, 5, 6 (4 events from seq 4 to 7 inclusive)
        page.Count.ShouldBe(4);

        // All returned events should have seq_id > floor and <= ceiling
        foreach (var e in page)
        {
            e.Sequence.ShouldBeGreaterThan(allSeqIds[2]);
            e.Sequence.ShouldBeLessThanOrEqualTo(allSeqIds[6]);
        }
    }

    [Fact]
    public async Task load_calculates_ceiling()
    {
        await InsertEventsAsync(5);
        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        // When all events are loaded and count < batchSize, ceiling == highWater
        page.Ceiling.ShouldBe(highWater);
    }

    [Fact]
    public async Task load_empty_range_returns_empty_page()
    {
        // No events inserted
        var loader = CreateLoader();
        var request = CreateRequest(0, 100, batchSize: 100);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        page.Count.ShouldBe(0);
    }

    private PolecatEventLoader CreateLoader()
    {
        return new PolecatEventLoader(theStore.Database.Events, theStore.Options);
    }

    private static EventRequest CreateRequest(long floor, long highWater, int batchSize)
    {
        return new EventRequest
        {
            Floor = floor,
            HighWater = highWater,
            BatchSize = batchSize,
            Name = new ShardName("TestLoader"),
            ErrorOptions = new ErrorHandlingOptions(),
            Runtime = null!,
            Metrics = null!
        };
    }

    private async Task InsertEventsAsync(int count)
    {
        for (var i = 0; i < count; i++)
        {
            var streamId = Guid.NewGuid();
            theSession.Events.StartStream(streamId, new QuestStarted($"Quest {i + 1}"));
            await theSession.SaveChangesAsync();
        }
    }

    private async Task<long> GetHighestSeqIdAsync()
    {
        return await theStore.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
    }

    private async Task<List<long>> GetAllSeqIdsAsync()
    {
        var seqIds = new List<long>();
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
}
