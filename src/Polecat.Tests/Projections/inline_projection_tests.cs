using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

/// <summary>
///     Self-aggregating document type for testing inline projections.
///     Uses conventional Apply/Create methods.
/// </summary>
public class QuestParty
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<string> Members { get; set; } = new();
    public string? Location { get; set; }
    public List<string> MonstersSlain { get; set; } = new();
    public bool IsFinished { get; set; }
    public int Version { get; set; }

    // Create: called for the first event in a new stream
    public static QuestParty Create(QuestStarted e)
    {
        return new QuestParty { Name = e.Name };
    }

    // Apply: called for subsequent events
    public void Apply(MembersJoined e)
    {
        Members.AddRange(e.Members);
        Location = e.Location;
    }

    public void Apply(MembersDeparted e)
    {
        foreach (var m in e.Members) Members.Remove(m);
    }

    public void Apply(ArrivedAtLocation e)
    {
        Location = e.Location;
    }

    public void Apply(MonsterSlain e)
    {
        MonstersSlain.Add(e.Name);
    }

    public bool ShouldDelete(QuestEnded e)
    {
        return true;
    }
}

[Collection("integration")]
public class inline_projection_tests : IntegrationContext
{
    public inline_projection_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreWithProjections()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Inline);
        });
        return theStore;
    }

    [Fact]
    public async Task snapshot_is_created_on_start_stream()
    {
        var store = await CreateStoreWithProjections();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Destroy the Ring"));
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Id.ShouldBe(streamId);
        party.Name.ShouldBe("Destroy the Ring");
    }

    [Fact]
    public async Task snapshot_applies_multiple_events_in_start()
    {
        var store = await CreateStoreWithProjections();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Fellowship"),
            new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas", "Gimli"]));
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Fellowship");
        party.Members.ShouldBe(["Aragorn", "Legolas", "Gimli"]);
        party.Location.ShouldBe("Rivendell");
    }

    [Fact]
    public async Task snapshot_updates_on_append()
    {
        var store = await CreateStoreWithProjections();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Adventure"),
            new MembersJoined(1, "Start", ["Hero"]));
        await session.SaveChangesAsync();

        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamId,
            new ArrivedAtLocation("Dungeon", 2),
            new MonsterSlain("Goblin", 50));
        await session2.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Location.ShouldBe("Dungeon");
        party.MonstersSlain.ShouldContain("Goblin");
    }

    [Fact]
    public async Task snapshot_handles_members_departed()
    {
        var store = await CreateStoreWithProjections();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Departure Test"),
            new MembersJoined(1, "Town", ["A", "B", "C"]));
        await session.SaveChangesAsync();

        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamId,
            new MembersDeparted(2, "Town", ["B"]));
        await session2.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Members.ShouldBe(["A", "C"]);
    }

    [Fact]
    public async Task should_delete_removes_snapshot()
    {
        var store = await CreateStoreWithProjections();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Doomed Quest"),
            new MembersJoined(1, "Start", ["Hero"]));
        await session.SaveChangesAsync();

        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamId,
            new QuestEnded("Doomed Quest"));
        await session2.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);
        party.ShouldBeNull();
    }

    [Fact]
    public async Task multiple_streams_projected_independently()
    {
        var store = await CreateStoreWithProjections();

        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.Events.StartStream(stream1,
            new QuestStarted("Quest 1"),
            new MembersJoined(1, "Town A", ["Alpha"]));
        session.Events.StartStream(stream2,
            new QuestStarted("Quest 2"),
            new MembersJoined(1, "Town B", ["Beta", "Gamma"]));
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party1 = await query.LoadAsync<QuestParty>(stream1);
        var party2 = await query.LoadAsync<QuestParty>(stream2);

        party1.ShouldNotBeNull();
        party1.Name.ShouldBe("Quest 1");
        party1.Members.ShouldBe(["Alpha"]);

        party2.ShouldNotBeNull();
        party2.Name.ShouldBe("Quest 2");
        party2.Members.ShouldBe(["Beta", "Gamma"]);
    }

    [Fact]
    public async Task events_and_projection_in_same_transaction()
    {
        var store = await CreateStoreWithProjections();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Atomic Quest"));
        await session.SaveChangesAsync();

        // Verify both events and projected document exist
        await using var query = store.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);

        var party = await query.LoadAsync<QuestParty>(streamId);
        party.ShouldNotBeNull();
        party.Name.ShouldBe("Atomic Quest");
    }

    [Fact]
    public async Task snapshot_with_version_tracking()
    {
        var store = await CreateStoreWithProjections();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Version Quest"),
            new MembersJoined(1, "Start", ["A"]),
            new ArrivedAtLocation("Mid", 2));
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        // Version should be set to the last event's version (3)
        party.Version.ShouldBe(3);
    }

    [Fact]
    public async Task projection_with_mixed_document_operations()
    {
        var store = await CreateStoreWithProjections();

        var streamId = Guid.NewGuid();
        var user = new User { Id = Guid.NewGuid(), FirstName = "Test", LastName = "User" };

        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Mixed Quest"));
        session.Store(user);
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);
        var loaded = await query.LoadAsync<User>(user.Id);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Mixed Quest");
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Test");
    }
}
