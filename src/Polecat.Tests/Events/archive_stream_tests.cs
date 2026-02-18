using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class archive_stream_tests : IntegrationContext
{
    public archive_stream_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task archive_stream_marks_stream_as_archived()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Doomed Quest"),
            new MembersJoined(1, "Town", ["A"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.ArchiveStream(streamId);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);

        state.ShouldNotBeNull();
        state!.IsArchived.ShouldBeTrue();
    }

    [Fact]
    public async Task archive_stream_marks_events_as_archived()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archive Events Quest"),
            new MembersJoined(1, "Start", ["X", "Y"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.ArchiveStream(streamId);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(2);
        events.ShouldAllBe(e => e.IsArchived);
    }

    [Fact]
    public async Task archived_events_still_fetchable()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Still Fetchable"),
            new MembersJoined(1, "Town", ["Hero"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.ArchiveStream(streamId);
        await session2.SaveChangesAsync();

        // Events are still returned by FetchStreamAsync (just marked as archived)
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
        events[0].Data.ShouldBeOfType<QuestStarted>();
    }

    [Fact]
    public async Task fetch_stream_state_shows_archived()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("State Check"));
        await theSession.SaveChangesAsync();

        // Verify not archived initially
        await using var query1 = theStore.QuerySession();
        var stateBefore = await query1.Events.FetchStreamStateAsync(streamId);
        stateBefore!.IsArchived.ShouldBeFalse();

        // Archive
        await using var session2 = theStore.LightweightSession();
        session2.Events.ArchiveStream(streamId);
        await session2.SaveChangesAsync();

        // Verify archived after
        await using var query2 = theStore.QuerySession();
        var stateAfter = await query2.Events.FetchStreamStateAsync(streamId);
        stateAfter!.IsArchived.ShouldBeTrue();
    }
}
