using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     Coverage for the methods added to round Polecat's event-store API up to the
///     <see cref="JasperFx.Events.IEventStoreOperations"/> surface: issue #88
///     (Append / StartStream IEnumerable + Type overloads), issue #89 (LoadAsync),
///     issue #90 (CompletelyReplaceEvent).
/// </summary>
[Collection("integration")]
public class event_store_api_completeness_tests : IntegrationContext
{
    public event_store_api_completeness_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    // -- Issue #88: IEnumerable<object> + Type-parameter overloads ---------

    [Fact]
    public async Task append_accepts_ienumerable_payload()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Enumerable Quest"));
        await theSession.SaveChangesAsync();

        IEnumerable<object> incoming =
        [
            new MembersJoined(1, "Town", ["A"]),
            new ArrivedAtLocation("Castle", 2),
        ];

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId, incoming);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(streamId)).Count.ShouldBe(3);
    }

    [Fact]
    public async Task append_with_expected_version_accepts_ienumerable_payload()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Versioned"));
        await theSession.SaveChangesAsync();

        IEnumerable<object> more = [new MembersJoined(1, "Town", ["A"])];

        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId, 2L, more);
        await session2.SaveChangesAsync();
    }

    [Fact]
    public async Task start_stream_accepts_ienumerable_payload()
    {
        var streamId = Guid.NewGuid();
        IEnumerable<object> events =
        [
            new QuestStarted("Lazy"),
            new MembersJoined(1, "Town", ["A"]),
        ];

        theSession.Events.StartStream(streamId, events);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(streamId)).Count.ShouldBe(2);
    }

    [Fact]
    public async Task start_stream_with_runtime_aggregate_type_sets_aggregate_type()
    {
        var streamId = Guid.NewGuid();

        var action = theSession.Events.StartStream(typeof(QuestParty), streamId, new QuestStarted("RuntimeType"));
        await theSession.SaveChangesAsync();

        // Client-side state on the StreamAction carries the aggregate type;
        // existing Polecat.FetchStreamStateAsync doesn't hydrate AggregateType
        // on the returned StreamState yet (see start_stream_with_aggregate_type).
        action.AggregateType.ShouldBe(typeof(QuestParty));

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamStateAsync(streamId))!.Version.ShouldBe(1);
    }

    [Fact]
    public async Task start_stream_generic_with_ienumerable_payload()
    {
        var streamId = Guid.NewGuid();
        IEnumerable<object> events = [new QuestStarted("GenericIEnumerable")];

        var action = theSession.Events.StartStream<QuestParty>(streamId, events);
        await theSession.SaveChangesAsync();

        action.AggregateType.ShouldBe(typeof(QuestParty));
        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamStateAsync(streamId))!.Version.ShouldBe(1);
    }

    // -- Issue #89: LoadAsync single-event lookup --------------------------

    [Fact]
    public async Task load_async_typed_returns_typed_event_when_found()
    {
        var streamId = Guid.NewGuid();
        var action = theSession.Events.StartStream(streamId, new QuestStarted("Loaded"));
        await theSession.SaveChangesAsync();

        var eventId = action.Events[0].Id;

        await using var query = theStore.QuerySession();
        var loaded = await query.Events.LoadAsync<QuestStarted>(eventId);

        loaded.ShouldNotBeNull();
        loaded.Id.ShouldBe(eventId);
        loaded.StreamId.ShouldBe(streamId);
        loaded.Data.Name.ShouldBe("Loaded");
    }

    [Fact]
    public async Task load_async_typed_returns_null_when_event_type_mismatches()
    {
        var streamId = Guid.NewGuid();
        var action = theSession.Events.StartStream(streamId, new QuestStarted("Mismatched"));
        await theSession.SaveChangesAsync();

        var eventId = action.Events[0].Id;

        await using var query = theStore.QuerySession();
        var wrongCast = await query.Events.LoadAsync<MembersJoined>(eventId);

        wrongCast.ShouldBeNull();
    }

    [Fact]
    public async Task load_async_typed_returns_null_when_event_id_not_found()
    {
        await using var query = theStore.QuerySession();
        var notThere = await query.Events.LoadAsync<QuestStarted>(Guid.NewGuid());
        notThere.ShouldBeNull();
    }

    [Fact]
    public async Task load_async_untyped_returns_event_wrapper_with_runtime_data_type()
    {
        var streamId = Guid.NewGuid();
        var action = theSession.Events.StartStream(streamId, new QuestStarted("Untyped"));
        await theSession.SaveChangesAsync();

        var eventId = action.Events[0].Id;

        await using var query = theStore.QuerySession();
        var loaded = await query.Events.LoadAsync(eventId);

        loaded.ShouldNotBeNull();
        loaded.Id.ShouldBe(eventId);
        loaded.Data.ShouldBeOfType<QuestStarted>();
        ((QuestStarted)loaded.Data).Name.ShouldBe("Untyped");
    }

    [Fact]
    public async Task load_async_untyped_returns_null_when_event_id_not_found()
    {
        await using var query = theStore.QuerySession();
        var notThere = await query.Events.LoadAsync(Guid.NewGuid());
        notThere.ShouldBeNull();
    }

    // -- Issue #90: CompletelyReplaceEvent --------------------------------

    [Fact]
    public async Task completely_replace_event_swaps_data_and_returns_new_id()
    {
        var streamId = Guid.NewGuid();
        var action = theSession.Events.StartStream(streamId, new QuestStarted("Original"));
        await theSession.SaveChangesAsync();

        var sequence = action.Events[0].Sequence;
        var originalId = action.Events[0].Id;

        // Replace with a Compacted-style snapshot event at the same sequence.
        await using var session2 = theStore.LightweightSession();
        var newId = session2.Events.CompletelyReplaceEvent(sequence, new QuestStarted("Compacted"));
        await session2.SaveChangesAsync();

        newId.ShouldNotBe(originalId);
        newId.ShouldNotBe(Guid.Empty);

        // The id at that sequence is now the replacement id; data is the new payload.
        await using var query = theStore.QuerySession();
        var loaded = await query.Events.LoadAsync<QuestStarted>(newId);
        loaded.ShouldNotBeNull();
        loaded.Data.Name.ShouldBe("Compacted");
    }

    public class QuestParty
    {
        public Guid Id { get; set; }
    }
}
