using JasperFx.Events;
using Polecat.Exceptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class start_stream_tests : IntegrationContext
{
    public start_stream_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task start_stream_with_guid_id()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Destroy the Ring"),
            new MembersJoined(1, "Hobbiton", ["Frodo", "Sam"]));

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(2);
        events[0].Data.ShouldBeOfType<QuestStarted>();
        events[1].Data.ShouldBeOfType<MembersJoined>();
    }

    [Fact]
    public async Task start_stream_assigns_sequential_versions()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Adventure"),
            new MembersJoined(1, "Start", ["Hero"]),
            new ArrivedAtLocation("Dungeon", 2));

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
        events[2].Version.ShouldBe(3);
    }

    [Fact]
    public async Task start_stream_assigns_global_sequences()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Quest1"),
            new MembersJoined(1, "Start", ["A"]));

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].Sequence.ShouldBeGreaterThan(0);
        events[1].Sequence.ShouldBeGreaterThan(events[0].Sequence);
    }

    [Fact]
    public async Task start_stream_captures_event_type_names()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Quest"));

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].EventTypeName.ShouldBe("quest_started");
        events[0].DotNetTypeName.ShouldContain("QuestStarted");
    }

    [Fact]
    public async Task start_stream_sets_stream_id_on_events()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Quest"));

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].StreamId.ShouldBe(streamId);
    }

    [Fact]
    public async Task start_stream_creates_stream_state()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Quest"),
            new MembersJoined(1, "Start", ["A", "B"]));

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);

        state.ShouldNotBeNull();
        state.Id.ShouldBe(streamId);
        state.Version.ShouldBe(2);
    }

    [Fact]
    public async Task start_stream_with_auto_generated_id()
    {
        var action = theSession.Events.StartStream(
            new QuestStarted("Auto Quest"));

        action.Id.ShouldNotBe(Guid.Empty);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(action.Id);
        events.Count.ShouldBe(1);
    }

    [Fact]
    public async Task start_stream_with_aggregate_type()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<QuestParty>(streamId,
            new QuestStarted("Typed Quest"));

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldNotBeNull();
        state.Version.ShouldBe(1);
    }

    [Fact]
    public async Task start_stream_duplicate_id_throws()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("First"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.StartStream(streamId,
            new QuestStarted("Duplicate"));

        await Should.ThrowAsync<ExistingStreamIdCollisionException>(
            session2.SaveChangesAsync());
    }

    [Fact]
    public async Task start_stream_returns_stream_action()
    {
        var streamId = Guid.NewGuid();
        var action = theSession.Events.StartStream(streamId,
            new QuestStarted("Quest"),
            new MembersJoined(1, "Start", ["A"]));

        action.ShouldNotBeNull();
        action.Id.ShouldBe(streamId);
        action.ActionType.ShouldBe(StreamActionType.Start);
        action.Events.Count.ShouldBe(2);
    }

    // Aggregate type for typed stream tests
    public class QuestParty
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
