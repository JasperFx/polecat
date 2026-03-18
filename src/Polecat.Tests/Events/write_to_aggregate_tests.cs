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

    [Fact]
    public async Task write_to_aggregate_with_initial_version_sync()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Versioned Quest"),
            new MembersJoined(1, "Town", ["Hero"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.WriteToAggregate<QuestAggregate>(streamId, 2, stream =>
        {
            stream.AppendOne(new MonsterSlain("Troll", 50));
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
    }

    [Fact]
    public async Task write_to_aggregate_with_initial_version_async()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Versioned Async"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.WriteToAggregate<QuestAggregate>(streamId, 1, async stream =>
        {
            await Task.Yield();
            stream.AppendOne(new MembersJoined(1, "Castle", ["Knight"]));
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task write_to_aggregate_with_wrong_initial_version_throws()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Wrong Version"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await Should.ThrowAsync<Exception>(async () =>
        {
            await session2.Events.WriteToAggregate<QuestAggregate>(streamId, 99, stream =>
            {
                stream.AppendOne(new MonsterSlain("Ghost", 5));
            });
        });
    }

    [Fact]
    public async Task write_exclusively_to_aggregate_sync()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Exclusive Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.WriteExclusivelyToAggregate<QuestAggregate>(streamId, stream =>
        {
            stream.Aggregate.ShouldNotBeNull();
            stream.Aggregate!.Name.ShouldBe("Exclusive Quest");
            stream.AppendOne(new MembersJoined(1, "Dungeon", ["Warrior"]));
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task write_exclusively_to_aggregate_async()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Exclusive Async"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.WriteExclusivelyToAggregate<QuestAggregate>(streamId, async stream =>
        {
            await Task.Yield();
            stream.AppendMany(
                new MembersJoined(1, "Forest", ["Elf"]),
                new MonsterSlain("Orc", 30));
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
    }

    [Fact]
    public async Task write_exclusively_to_aggregate_persists_events()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Exclusive Persist"),
            new MembersJoined(1, "Start", ["A"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.WriteExclusivelyToAggregate<QuestAggregate>(streamId, stream =>
        {
            stream.Aggregate.ShouldNotBeNull();
            stream.Aggregate!.Members.ShouldBe(["A"]);
            stream.AppendOne(new MonsterSlain("Spider", 15));
        });

        await using var query = theStore.QuerySession();
        var aggregate = await query.Events.FetchLatest<QuestAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate!.MonstersSlain.ShouldBe(1);
    }

    [Fact]
    public async Task write_exclusively_persists_and_releases_lock()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Lock Release"));
        await theSession.SaveChangesAsync();

        // First exclusive write
        await using var session2 = theStore.LightweightSession();
        await session2.Events.WriteExclusivelyToAggregate<QuestAggregate>(streamId, stream =>
        {
            stream.AppendOne(new MembersJoined(1, "Camp", ["Scout"]));
        });

        // Second exclusive write should succeed (lock released after first)
        await using var session3 = theStore.LightweightSession();
        await session3.Events.WriteExclusivelyToAggregate<QuestAggregate>(streamId, stream =>
        {
            stream.Aggregate.ShouldNotBeNull();
            stream.AppendOne(new MonsterSlain("Rat", 1));
        });

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
    }
}
