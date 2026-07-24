using JasperFx.Events;
using Polecat.Events;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

// #364 follow-up (Marten parity): AggregateToAsync<T>() folds every event matched by an event query
// into a single aggregate, regardless of stream, optionally starting from supplied state. The
// aggregate's identity is stamped from the last queried event's stream. Reuses the self-aggregating
// QuestParty (inline_projection_tests.cs) and SelfAggregatingStringQuest
// (single_stream_projection_with_string_identity_tests.cs) declared in this namespace.
public class aggregateto_linq_operator_tests : OneOffConfigurationsContext
{
    private readonly MembersJoined _joined1 = new(1, "Emond's Field", ["Rand", "Matrim", "Perrin", "Thom"]);
    private readonly MembersDeparted _departed1 = new(2, "Baerlon", ["Thom"]);

    private readonly MembersJoined _joined2 = new(3, "Tar Valon", ["Elayne", "Moiraine", "Elmindreda"]);
    private readonly MembersDeparted _departed2 = new(4, "Caemlyn", ["Moiraine"]);

    private async Task<DocumentStore> CreateStore()
    {
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        return theStore;
    }

    [Fact]
    public async Task can_aggregate_events_to_aggregate_type_asynchronously()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        session.Events.StartStream(Guid.NewGuid(), _joined1, _departed1);
        session.Events.StartStream(Guid.NewGuid(), _joined2, _departed2);
        await session.SaveChangesAsync();

        #region sample_aggregate_to_async

        var questParty = await session.Events
            .QueryAllRawEvents()

            // You could of course chain all the Linq
            // Where()/OrderBy()/Take()/Skip() operators
            // you need here

            .AggregateToAsync<QuestParty>();

        #endregion

        questParty.ShouldNotBeNull();
        questParty.Members.ShouldBe(["Rand", "Matrim", "Perrin", "Elayne", "Elmindreda"]);
    }

    [Fact]
    public async Task can_aggregate_with_initial_state_asynchronously()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        var initialParty = new QuestParty { Members = ["Lan"] };
        session.Events.StartStream(Guid.NewGuid(), _joined1, _departed1);
        session.Events.StartStream(Guid.NewGuid(), _joined2, _departed2);
        await session.SaveChangesAsync();

        var questParty = await session.Events.QueryAllRawEvents().AggregateToAsync(initialParty);

        questParty.ShouldNotBeNull();
        questParty.Members.ShouldBe(["Lan", "Rand", "Matrim", "Perrin", "Elayne", "Elmindreda"]);
    }

    [Fact]
    public async Task returns_null_when_the_query_matches_no_events()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        var questParty = await session.Events.QueryAllRawEvents()
            .Where(x => x.StreamId == Guid.NewGuid())
            .AggregateToAsync<QuestParty>();

        questParty.ShouldBeNull();
    }

    [Fact]
    public async Task gets_the_id_set()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        var id = Guid.NewGuid();
        session.Events.StartStream(id, _joined1, _departed1);
        session.Events.StartStream(Guid.NewGuid(), _joined2, _departed2);
        await session.SaveChangesAsync();

        var questParty = await session.Events.QueryAllRawEvents()
            .Where(x => x.StreamId == id)
            .AggregateToAsync<QuestParty>();

        questParty.ShouldNotBeNull();
        questParty.Id.ShouldBe(id);
    }

    [Fact]
    public async Task gets_the_key_set()
    {
        ConfigureStore(opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        await using var session = theStore.LightweightSession();

        var key = Guid.NewGuid().ToString();
        session.Events.StartStream(key, new QuestStarted("Save the World"), _joined1);
        session.Events.StartStream(Guid.NewGuid().ToString(), new QuestStarted("Other"), _joined2);
        await session.SaveChangesAsync();

        var quest = await session.Events.QueryAllRawEvents()
            .Where(x => x.StreamKey == key)
            .AggregateToAsync<SelfAggregatingStringQuest>();

        quest.ShouldNotBeNull();
        quest.Id.ShouldBe(key);
        quest.Name.ShouldBe("Save the World");
        quest.Members.ShouldBe(["Rand", "Matrim", "Perrin", "Thom"]);
    }
}
