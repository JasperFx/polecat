using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public class QuestAggregate
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<string> Members { get; set; } = new();
    public int MonstersSlain { get; set; }

    public static QuestAggregate Create(QuestStarted e) => new() { Name = e.Name };

    public void Apply(MembersJoined e) => Members.AddRange(e.Members);

    public void Apply(MonsterSlain e) => MonstersSlain++;
}

[Collection("integration")]
public class fetch_for_writing_tests : IntegrationContext
{
    public fetch_for_writing_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task fetch_new_stream_returns_null_aggregate_and_version_zero()
    {
        var streamId = Guid.NewGuid();
        var stream = await theSession.Events.FetchForWriting<QuestAggregate>(streamId);

        stream.Aggregate.ShouldBeNull();
        stream.StartingVersion.ShouldBe(0);
        stream.Id.ShouldBe(streamId);
    }

    [Fact]
    public async Task fetch_existing_stream_returns_aggregate_and_version()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Ring Quest"),
            new MembersJoined(1, "Shire", ["Frodo", "Sam"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.Name.ShouldBe("Ring Quest");
        stream.Aggregate.Members.ShouldBe(["Frodo", "Sam"]);
        stream.StartingVersion.ShouldBe(2);
        stream.Id.ShouldBe(streamId);
    }

    [Fact]
    public async Task fetch_and_append_then_save()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Adventure"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AppendOne(new MembersJoined(1, "Start", ["Hero"]));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
        events[1].Data.ShouldBeOfType<MembersJoined>();
        events[1].Version.ShouldBe(2);
    }

    [Fact]
    public async Task fetch_with_expected_version_succeeds()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Quest"),
            new MembersJoined(1, "Town", ["A"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId, 2);

        stream.Aggregate.ShouldNotBeNull();
        stream.StartingVersion.ShouldBe(2);
    }

    [Fact]
    public async Task fetch_with_wrong_expected_version_throws()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            session2.Events.FetchForWriting<QuestAggregate>(streamId, 5));
    }

    [Fact]
    public async Task fetch_for_exclusive_writing_existing_stream()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Exclusive Quest"),
            new MembersJoined(1, "Castle", ["Knight"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForExclusiveWriting<QuestAggregate>(streamId);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.Name.ShouldBe("Exclusive Quest");
        stream.StartingVersion.ShouldBe(2);

        stream.AppendOne(new MonsterSlain("Dragon", 100));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
    }

    [Fact]
    public async Task fetch_for_exclusive_writing_new_stream()
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForExclusiveWriting<QuestAggregate>(streamId);

        stream.Aggregate.ShouldBeNull();
        stream.StartingVersion.ShouldBe(0);

        stream.AppendOne(new QuestStarted("New Exclusive"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
    }

    [Fact]
    public async Task try_fast_forward_allows_reuse_across_saves()
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();

        // First round: start stream
        var stream = await session.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AppendOne(new QuestStarted("FastForward Quest"));
        stream.AppendMany(new MembersJoined(1, "Start", ["A", "B"]));
        await session.SaveChangesAsync();

        // Fast forward and append more
        stream.TryFastForwardVersion();
        stream.AppendOne(new MonsterSlain("Goblin", 10));
        stream.AppendOne(new MonsterSlain("Orc", 20));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(4);
        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
        events[2].Version.ShouldBe(3);
        events[3].Version.ShouldBe(4);
    }

    [Fact]
    public async Task append_via_stream_sets_correct_versions()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Version Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);
        stream.AppendMany(
            new MembersJoined(1, "Town", ["X"]),
            new MonsterSlain("Rat", 5));
        stream.CurrentVersion.ShouldBe(3); // starting 1 + 2 appended
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
        events[2].Version.ShouldBe(3);
    }

    [Fact]
    public async Task fetch_for_writing_sets_aggregate_id()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("ID Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.Id.ShouldBe(streamId);
    }
}
