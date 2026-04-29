using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

/// <summary>
///     Aggregate document with a string identity, for use with string-keyed streams.
/// </summary>
public class StringQuestParty
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public List<string> Members { get; set; } = new();
    public string? Location { get; set; }
    public List<string> MonstersSlain { get; set; } = new();
}

/// <summary>
///     Self-aggregating document type with string identity, for use with
///     the Snapshot&lt;T, TId&gt; API (conventional Apply/Create methods on the type itself).
/// </summary>
public class SelfAggregatingStringQuest
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public List<string> Members { get; set; } = new();

    public static SelfAggregatingStringQuest Create(QuestStarted e)
    {
        return new SelfAggregatingStringQuest { Name = e.Name };
    }

    public void Apply(MembersJoined e)
    {
        Members.AddRange(e.Members);
    }
}

/// <summary>
///     Custom SingleStreamProjection with TId = string, demonstrating the new
///     two-type-parameter signature that mirrors Marten's SingleStreamProjection&lt;TDoc, TId&gt;.
/// </summary>
public class StringQuestPartyProjection : SingleStreamProjection<StringQuestParty, string>
{
    public StringQuestPartyProjection()
    {
    }

    public static StringQuestParty Create(IEvent<QuestStarted> @event)
    {
        return new StringQuestParty
        {
            Id = @event.StreamKey!,
            Name = @event.Data.Name
        };
    }

    public void Apply(MembersJoined e, StringQuestParty party)
    {
        party.Members.AddRange(e.Members);
        party.Location = e.Location;
    }

    public void Apply(MembersDeparted e, StringQuestParty party)
    {
        foreach (var m in e.Members) party.Members.Remove(m);
    }

    public void Apply(ArrivedAtLocation e, StringQuestParty party)
    {
        party.Location = e.Location;
    }

    public void Apply(MonsterSlain e, StringQuestParty party)
    {
        party.MonstersSlain.Add(e.Name);
    }

    public bool ShouldDelete(QuestEnded e)
    {
        return true;
    }
}

[Collection("integration")]
public class single_stream_projection_with_string_identity_tests : IntegrationContext
{
    public single_stream_projection_with_string_identity_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreWithStringProjection()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "string_proj";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<StringQuestPartyProjection>(JasperFx.Events.Projections.ProjectionLifecycle.Inline);
        });
        return theStore;
    }

    [Fact]
    public async Task custom_projection_creates_aggregate_on_start_stream()
    {
        var store = await CreateStoreWithStringProjection();

        var streamKey = "quest-" + Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamKey,
            new QuestStarted("Destroy the Ring"));
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<StringQuestParty>(streamKey);

        party.ShouldNotBeNull();
        party.Id.ShouldBe(streamKey);
        party.Name.ShouldBe("Destroy the Ring");
    }

    [Fact]
    public async Task custom_projection_applies_multiple_events()
    {
        var store = await CreateStoreWithStringProjection();

        var streamKey = "quest-" + Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamKey,
            new QuestStarted("Fellowship"),
            new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas", "Gimli"]));
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<StringQuestParty>(streamKey);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Fellowship");
        party.Members.ShouldBe(["Aragorn", "Legolas", "Gimli"]);
        party.Location.ShouldBe("Rivendell");
    }

    [Fact]
    public async Task custom_projection_updates_on_append()
    {
        var store = await CreateStoreWithStringProjection();

        var streamKey = "quest-" + Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamKey,
            new QuestStarted("Adventure"),
            new MembersJoined(1, "Start", ["Hero"]));
        await session.SaveChangesAsync();

        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamKey,
            new ArrivedAtLocation("Dungeon", 2),
            new MonsterSlain("Goblin", 50));
        await session2.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<StringQuestParty>(streamKey);

        party.ShouldNotBeNull();
        party.Location.ShouldBe("Dungeon");
        party.MonstersSlain.ShouldContain("Goblin");
    }

    [Fact]
    public async Task custom_projection_should_delete_removes_aggregate()
    {
        var store = await CreateStoreWithStringProjection();

        var streamKey = "quest-" + Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamKey,
            new QuestStarted("Doomed Quest"),
            new MembersJoined(1, "Start", ["Hero"]));
        await session.SaveChangesAsync();

        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamKey,
            new QuestEnded("Doomed Quest"));
        await session2.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<StringQuestParty>(streamKey);
        party.ShouldBeNull();
    }

    [Fact]
    public async Task multiple_string_keyed_streams_projected_independently()
    {
        var store = await CreateStoreWithStringProjection();

        var key1 = "quest-" + Guid.NewGuid();
        var key2 = "quest-" + Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.Events.StartStream(key1,
            new QuestStarted("Quest 1"),
            new MembersJoined(1, "Town A", ["Alpha"]));
        session.Events.StartStream(key2,
            new QuestStarted("Quest 2"),
            new MembersJoined(1, "Town B", ["Beta", "Gamma"]));
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var party1 = await query.LoadAsync<StringQuestParty>(key1);
        var party2 = await query.LoadAsync<StringQuestParty>(key2);

        party1.ShouldNotBeNull();
        party1.Name.ShouldBe("Quest 1");
        party1.Members.ShouldBe(["Alpha"]);

        party2.ShouldNotBeNull();
        party2.Name.ShouldBe("Quest 2");
        party2.Members.ShouldBe(["Beta", "Gamma"]);
    }

    [Fact]
    public async Task self_aggregating_projection_with_string_identity()
    {
        // Use SingleStreamProjection<T, TId> with a self-aggregating type
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "string_snap";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<SingleStreamProjection<SelfAggregatingStringQuest, string>>(ProjectionLifecycle.Inline);
        });

        var streamKey = "snap-" + Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamKey,
            new QuestStarted("Snapshot Quest"),
            new MembersJoined(1, "Castle", ["Knight"]));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var party = await query.LoadAsync<SelfAggregatingStringQuest>(streamKey);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Snapshot Quest");
        party.Members.ShouldBe(["Knight"]);
    }
}
