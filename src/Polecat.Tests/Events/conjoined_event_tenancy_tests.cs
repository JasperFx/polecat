using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class conjoined_event_tenancy_tests : IntegrationContext
{
    public conjoined_event_tenancy_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateConjoinedStore()
    {
        var schemaName = "tenancy_evt_" + Guid.NewGuid().ToString("N")[..8];
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = schemaName;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });
        return theStore;
    }

    [Fact]
    public async Task events_isolated_by_tenant()
    {
        var store = await CreateConjoinedStore();
        var streamId = Guid.NewGuid();

        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Events.StartStream(streamId,
            new QuestStarted("Red Quest"),
            new MembersJoined(1, "Red Town", ["RedHero"]));
        await redSession.SaveChangesAsync();

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var events = await blueQuery.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(0);
    }

    [Fact]
    public async Task same_stream_id_different_tenants()
    {
        var store = await CreateConjoinedStore();
        var streamId = Guid.NewGuid();

        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Events.StartStream(streamId,
            new QuestStarted("Red Quest"));
        await redSession.SaveChangesAsync();

        // Same stream ID in Blue should NOT conflict
        await using var blueSession = store.LightweightSession(new SessionOptions { TenantId = "Blue" });
        blueSession.Events.StartStream(streamId,
            new QuestStarted("Blue Quest"),
            new MembersJoined(1, "Blue Town", ["BlueHero"]));
        await blueSession.SaveChangesAsync();

        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var redEvents = await redQuery.Events.FetchStreamAsync(streamId);
        redEvents.Count.ShouldBe(1);
        redEvents[0].Data.ShouldBeOfType<QuestStarted>().Name.ShouldBe("Red Quest");

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var blueEvents = await blueQuery.Events.FetchStreamAsync(streamId);
        blueEvents.Count.ShouldBe(2);
        blueEvents[0].Data.ShouldBeOfType<QuestStarted>().Name.ShouldBe("Blue Quest");
    }

    [Fact]
    public async Task aggregate_stream_respects_tenant()
    {
        var store = await CreateConjoinedStore();
        var streamId = Guid.NewGuid();

        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Events.StartStream(streamId,
            new QuestStarted("Red Quest"),
            new MembersJoined(1, "Red Town", ["RedHero"]));
        await redSession.SaveChangesAsync();

        await using var blueSession = store.LightweightSession(new SessionOptions { TenantId = "Blue" });
        blueSession.Events.StartStream(streamId,
            new QuestStarted("Blue Quest"),
            new MembersJoined(1, "Blue Town", ["BlueHero"]));
        await blueSession.SaveChangesAsync();

        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var redParty = await redQuery.Events.AggregateStreamAsync<QuestParty>(streamId);
        redParty.ShouldNotBeNull();
        redParty.Name.ShouldBe("Red Quest");
        redParty.Members.ShouldBe(["RedHero"]);

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var blueParty = await blueQuery.Events.AggregateStreamAsync<QuestParty>(streamId);
        blueParty.ShouldNotBeNull();
        blueParty.Name.ShouldBe("Blue Quest");
        blueParty.Members.ShouldBe(["BlueHero"]);
    }

    [Fact]
    public async Task stream_state_isolated_by_tenant()
    {
        var store = await CreateConjoinedStore();
        var streamId = Guid.NewGuid();

        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Events.StartStream(streamId,
            new QuestStarted("Red Quest"),
            new MembersJoined(1, "Red Town", ["A", "B"]));
        await redSession.SaveChangesAsync();

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var state = await blueQuery.Events.FetchStreamStateAsync(streamId);
        state.ShouldBeNull();

        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var redState = await redQuery.Events.FetchStreamStateAsync(streamId);
        redState.ShouldNotBeNull();
        redState.Version.ShouldBe(2);
    }

    [Fact]
    public async Task append_to_stream_respects_tenant()
    {
        var store = await CreateConjoinedStore();
        var streamId = Guid.NewGuid();

        // Start stream as Red
        await using var redSession = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redSession.Events.StartStream(streamId,
            new QuestStarted("Red Quest"));
        await redSession.SaveChangesAsync();

        // Append to Red's stream
        await using var redAppend = store.LightweightSession(new SessionOptions { TenantId = "Red" });
        redAppend.Events.Append(streamId, new MembersJoined(1, "Red Town", ["Hero"]));
        await redAppend.SaveChangesAsync();

        // Blue should see nothing
        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        var blueEvents = await blueQuery.Events.FetchStreamAsync(streamId);
        blueEvents.Count.ShouldBe(0);

        // Red should see 2 events
        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var redEvents = await redQuery.Events.FetchStreamAsync(streamId);
        redEvents.Count.ShouldBe(2);
    }
}
