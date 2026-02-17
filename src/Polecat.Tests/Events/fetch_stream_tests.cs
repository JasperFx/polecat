using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class fetch_stream_tests : IntegrationContext
{
    public fetch_stream_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task fetch_nonexistent_stream_returns_empty()
    {
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(Guid.NewGuid());
        events.Count.ShouldBe(0);
    }

    [Fact]
    public async Task fetch_stream_state_returns_null_for_nonexistent()
    {
        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(Guid.NewGuid());
        state.ShouldBeNull();
    }

    [Fact]
    public async Task fetch_stream_preserves_event_data()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Fellowship"),
            new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas", "Gimli"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        var quest = events[0].Data.ShouldBeOfType<QuestStarted>();
        quest.Name.ShouldBe("Fellowship");

        var joined = events[1].Data.ShouldBeOfType<MembersJoined>();
        joined.Day.ShouldBe(1);
        joined.Location.ShouldBe("Rivendell");
        joined.Members.ShouldBe(["Aragorn", "Legolas", "Gimli"]);
    }

    [Fact]
    public async Task fetch_stream_up_to_version()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Version Filter"),
            new MembersJoined(1, "Start", ["A"]),
            new ArrivedAtLocation("Mid", 2),
            new MonsterSlain("Goblin", 50));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, version: 2);

        events.Count.ShouldBe(2);
        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
    }

    [Fact]
    public async Task fetch_stream_from_version()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("From Version"),
            new MembersJoined(1, "Start", ["A"]),
            new ArrivedAtLocation("Mid", 2),
            new MonsterSlain("Goblin", 50));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, fromVersion: 3);

        events.Count.ShouldBe(2);
        events[0].Version.ShouldBe(3);
        events[1].Version.ShouldBe(4);
    }

    [Fact]
    public async Task fetch_stream_events_have_timestamps()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Timestamp Quest"));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].Timestamp.ShouldNotBe(default);
    }

    [Fact]
    public async Task fetch_stream_events_have_tenant_id()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Tenant Quest"));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].TenantId.ShouldBe(Tenancy.DefaultTenantId);
    }

    [Fact]
    public async Task fetch_stream_events_have_unique_ids()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Unique IDs"),
            new MembersJoined(1, "Start", ["A"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events[0].Id.ShouldNotBe(Guid.Empty);
        events[1].Id.ShouldNotBe(Guid.Empty);
        events[0].Id.ShouldNotBe(events[1].Id);
    }

    [Fact]
    public async Task fetch_stream_state_has_timestamps()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("State Timestamps"));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);

        state.ShouldNotBeNull();
        state.LastTimestamp.ShouldNotBe(default);
        state.Created.ShouldNotBe(default);
    }

    [Fact]
    public async Task events_and_documents_in_same_save()
    {
        var streamId = Guid.NewGuid();
        var user = new User { Id = Guid.NewGuid(), FirstName = "Event", LastName = "User" };

        theSession.Events.StartStream(streamId,
            new QuestStarted("Mixed Save"));
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        var loaded = await query.LoadAsync<User>(user.Id);

        events.Count.ShouldBe(1);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Event");
    }
}
