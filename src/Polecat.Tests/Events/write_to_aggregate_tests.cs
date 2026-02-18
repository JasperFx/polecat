using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class write_to_aggregate_tests : IntegrationContext
{
    public write_to_aggregate_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task write_to_aggregate_with_sync_callback()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Callback Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.WriteToAggregate<QuestAggregate>(streamId, stream =>
        {
            stream.Aggregate.ShouldNotBeNull();
            stream.Aggregate!.Name.ShouldBe("Callback Quest");
            stream.AppendOne(new MembersJoined(1, "Town", ["Hero"]));
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task write_to_aggregate_with_async_callback()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Async Callback"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.WriteToAggregate<QuestAggregate>(streamId, async stream =>
        {
            await Task.Yield();
            stream.AppendMany(
                new MembersJoined(1, "Castle", ["Knight"]),
                new MonsterSlain("Dragon", 100));
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
    }

    [Fact]
    public async Task write_to_aggregate_persists_events()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Persist Check"),
            new MembersJoined(1, "Start", ["A"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.WriteToAggregate<QuestAggregate>(streamId, stream =>
        {
            stream.AppendOne(new MonsterSlain("Goblin", 10));
        });

        // Verify via a fresh session that events are persisted
        await using var session3 = theStore.LightweightSession();
        var aggregate = await session3.Events.FetchLatest<QuestAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate!.MonstersSlain.ShouldBe(1);
        aggregate.Members.ShouldBe(["A"]);
    }
}
