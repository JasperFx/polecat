using JasperFx.Events;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class tombstone_stream_tests : IntegrationContext
{
    public tombstone_stream_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task tombstone_stream_deletes_stream_and_events()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Doomed Quest"),
            new MembersJoined(1, "Town", ["A", "B"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.TombstoneStream(streamId);
        await session2.SaveChangesAsync();

        // Stream should be completely gone
        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldBeNull();
    }

    [Fact]
    public async Task tombstone_stream_removes_all_events()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Gone Quest"),
            new MembersJoined(1, "Cave", ["X"]),
            new MembersDeparted(2, "Cave", ["X"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.TombstoneStream(streamId);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(0);
    }

    [Fact]
    public async Task tombstone_stream_by_string_key()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "tombstone_string";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
        });

        var streamKey = "tombstone-test-" + Guid.NewGuid();

        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamKey,
            new QuestStarted("Key Quest"),
            new MembersJoined(1, "Start", ["Hero"]));
        await session1.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.TombstoneStream(streamKey);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamKey);
        state.ShouldBeNull();
    }

    [Fact]
    public async Task tombstone_does_not_affect_other_streams()
    {
        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        theSession.Events.StartStream(stream1, new QuestStarted("Quest 1"));
        theSession.Events.StartStream(stream2, new QuestStarted("Quest 2"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.TombstoneStream(stream1);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state1 = await query.Events.FetchStreamStateAsync(stream1);
        var state2 = await query.Events.FetchStreamStateAsync(stream2);

        state1.ShouldBeNull();
        state2.ShouldNotBeNull();
    }

    [Fact]
    public async Task can_start_new_stream_with_same_id_after_tombstone()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Original"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.TombstoneStream(streamId);
        await session2.SaveChangesAsync();

        // Should be able to reuse the same stream ID
        await using var session3 = theStore.LightweightSession();
        session3.Events.StartStream(streamId, new QuestStarted("Reborn"));
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
        events[0].Data.ShouldBeOfType<QuestStarted>().Name.ShouldBe("Reborn");
    }
}
