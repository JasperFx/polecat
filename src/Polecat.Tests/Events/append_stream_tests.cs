using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class append_stream_tests : IntegrationContext
{
    public append_stream_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task append_to_existing_stream()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Append Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId,
            new MembersJoined(2, "Town", ["Gandalf"]));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(2);
        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
    }

    [Fact]
    public async Task append_multiple_events_at_once()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Multi Append"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId,
            new MembersJoined(1, "Town", ["A"]),
            new ArrivedAtLocation("Castle", 2),
            new MonsterSlain("Dragon", 100));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(4);
        events[3].Version.ShouldBe(4);
    }

    [Fact]
    public async Task append_creates_stream_if_not_exists()
    {
        var streamId = Guid.NewGuid();

        theSession.Events.Append(streamId,
            new QuestStarted("Implicit Stream"));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);

        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldNotBeNull();
        state.Version.ShouldBe(1);
    }

    [Fact]
    public async Task append_multiple_times_in_same_session()
    {
        var streamId = Guid.NewGuid();

        theSession.Events.Append(streamId,
            new QuestStarted("Same Session"));
        theSession.Events.Append(streamId,
            new MembersJoined(1, "Start", ["Hero"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(2);
        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
    }

    [Fact]
    public async Task append_updates_stream_version()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Version Quest"),
            new MembersJoined(1, "Start", ["A"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId,
            new ArrivedAtLocation("Destination", 5));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldNotBeNull();
        state.Version.ShouldBe(3);
    }

    [Fact]
    public async Task append_with_expected_version_succeeds()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Concurrency Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId, 2,
            new MembersJoined(1, "Start", ["B"]));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task append_with_wrong_expected_version_throws()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Concurrency Fail"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId, 5,
            new MembersJoined(1, "Start", ["C"]));

        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            session2.SaveChangesAsync());
    }

    [Fact]
    public async Task append_returns_stream_action()
    {
        var streamId = Guid.NewGuid();
        var action = theSession.Events.Append(streamId,
            new QuestStarted("Action Return"));

        action.ShouldNotBeNull();
        action.Id.ShouldBe(streamId);
        action.ActionType.ShouldBe(StreamActionType.Append);
        action.Events.Count.ShouldBe(1);
    }

    [Fact]
    public async Task multiple_streams_in_single_save()
    {
        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        theSession.Events.StartStream(stream1,
            new QuestStarted("Quest 1"));
        theSession.Events.StartStream(stream2,
            new QuestStarted("Quest 2"),
            new MembersJoined(1, "Start", ["X"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events1 = await query.Events.FetchStreamAsync(stream1);
        var events2 = await query.Events.FetchStreamAsync(stream2);

        events1.Count.ShouldBe(1);
        events2.Count.ShouldBe(2);
    }
}
