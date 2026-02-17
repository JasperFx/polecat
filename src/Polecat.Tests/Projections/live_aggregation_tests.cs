using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

[Collection("integration")]
public class live_aggregation_tests : IntegrationContext
{
    public live_aggregation_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_aggregate_stream_from_start()
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Destroy the Ring"),
            new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas", "Gimli"]));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Id.ShouldBe(streamId);
        party.Name.ShouldBe("Destroy the Ring");
        party.Members.ShouldBe(["Aragorn", "Legolas", "Gimli"]);
        party.Location.ShouldBe("Rivendell");
    }

    [Fact]
    public async Task aggregate_returns_null_for_empty_stream()
    {
        await using var query = theStore.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(Guid.NewGuid());

        party.ShouldBeNull();
    }

    [Fact]
    public async Task aggregate_with_version_cutoff()
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Version Quest"),
            new MembersJoined(1, "Start", ["A"]),
            new ArrivedAtLocation("Dungeon", 2),
            new MonsterSlain("Goblin", 50));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        // Only aggregate up to version 2 (QuestStarted + MembersJoined)
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId, version: 2);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Version Quest");
        party.Members.ShouldBe(["A"]);
        party.Location.ShouldBe("Start");
        party.MonstersSlain.ShouldBeEmpty();
    }

    [Fact]
    public async Task aggregate_with_append()
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Appended Quest"),
            new MembersJoined(1, "Town", ["Hero"]));
        await session.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId,
            new ArrivedAtLocation("Dungeon", 2),
            new MonsterSlain("Dragon", 100));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Appended Quest");
        party.Members.ShouldBe(["Hero"]);
        party.Location.ShouldBe("Dungeon");
        party.MonstersSlain.ShouldContain("Dragon");
    }

    [Fact]
    public async Task aggregate_with_existing_state()
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        // Only append events after the first — no QuestStarted
        session.Events.StartStream(streamId,
            new QuestStarted("State Quest"),
            new MembersJoined(1, "Town", ["A", "B"]));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var existingState = new QuestParty { Name = "Pre-existing" };
        // fromVersion: 2 to skip the QuestStarted, state carries over
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId,
            state: existingState, fromVersion: 2);

        party.ShouldNotBeNull();
        // Name should remain from existing state since we skipped QuestStarted
        party.Name.ShouldBe("Pre-existing");
        party.Members.ShouldBe(["A", "B"]);
    }

    [Fact]
    public async Task aggregate_handles_should_delete()
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Doomed Quest"),
            new MembersJoined(1, "Start", ["Hero"]),
            new QuestEnded("Doomed Quest"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId);

        party.ShouldBeNull();
    }

    [Fact]
    public async Task aggregate_without_registration()
    {
        // No Snapshot<QuestParty> call — convention-based discovery should work
        // The default store fixture has no projection registrations
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Convention Quest"),
            new MembersJoined(1, "Village", ["Wizard"]));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Convention Quest");
        party.Members.ShouldBe(["Wizard"]);
    }

    [Fact]
    public async Task aggregate_does_not_persist()
    {
        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Transient Quest"));
        await session.SaveChangesAsync();

        // Aggregate — should NOT persist
        await using var query = theStore.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId);
        party.ShouldNotBeNull();

        // Load — should return null since live aggregation doesn't persist
        await using var query2 = theStore.QuerySession();
        var loaded = await query2.LoadAsync<QuestParty>(streamId);
        loaded.ShouldBeNull();
    }
}
