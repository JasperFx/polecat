using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class fetch_latest_tests : IntegrationContext
{
    public fetch_latest_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task fetch_latest_returns_aggregate()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Latest Quest"),
            new MembersJoined(1, "Town", ["Alpha", "Beta"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var aggregate = await query.Events.FetchLatest<QuestAggregate>(streamId);

        aggregate.ShouldNotBeNull();
        aggregate!.Name.ShouldBe("Latest Quest");
        aggregate.Members.ShouldBe(["Alpha", "Beta"]);
    }

    [Fact]
    public async Task fetch_latest_returns_null_for_nonexistent()
    {
        var streamId = Guid.NewGuid();

        await using var query = theStore.QuerySession();
        var aggregate = await query.Events.FetchLatest<QuestAggregate>(streamId);

        aggregate.ShouldBeNull();
    }

    [Fact]
    public async Task fetch_latest_matches_aggregate_stream()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Consistency"),
            new MembersJoined(1, "Start", ["X"]),
            new MonsterSlain("Rat", 5));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var fromLatest = await query.Events.FetchLatest<QuestAggregate>(streamId);
        var fromAggregate = await query.Events.AggregateStreamAsync<QuestAggregate>(streamId);

        fromLatest.ShouldNotBeNull();
        fromAggregate.ShouldNotBeNull();
        fromLatest!.Name.ShouldBe(fromAggregate!.Name);
        fromLatest.Members.ShouldBe(fromAggregate.Members);
        fromLatest.MonstersSlain.ShouldBe(fromAggregate.MonstersSlain);
    }
}
