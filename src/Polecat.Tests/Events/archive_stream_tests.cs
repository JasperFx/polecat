using Polecat.Exceptions;
using Polecat.Tests.Harness;
using Shouldly;

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
    public async Task archived_events_are_excluded_from_fetch()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archive Events Quest"),
            new MembersJoined(1, "Start", ["X", "Y"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.ArchiveStream(streamId);
        await session2.SaveChangesAsync();

        // Archived events should NOT be returned by FetchStreamAsync
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(0);
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

    [Fact]
    public async Task appending_to_archived_stream_throws()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("No Append Quest"));
        await theSession.SaveChangesAsync();

        await using var archiveSession = theStore.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        // Try to append to the archived stream
        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId, new MembersJoined(2, "Cave", ["Bilbo"]));

        var ex = await Should.ThrowAsync<InvalidStreamException>(async () =>
        {
            await session2.SaveChangesAsync();
        });

        ex.Message.ShouldContain("archived");
    }

    [Fact]
    public async Task unarchive_stream_restores_events()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Unarchive Quest"),
            new MembersJoined(1, "Village", ["Frodo", "Sam"]));
        await theSession.SaveChangesAsync();

        // Archive
        await using var archiveSession = theStore.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        // Verify archived
        await using var querySession1 = theStore.QuerySession();
        var events1 = await querySession1.Events.FetchStreamAsync(streamId);
        events1.Count.ShouldBe(0);

        // UnArchive
        await using var unarchiveSession = theStore.LightweightSession();
        unarchiveSession.Events.UnArchiveStream(streamId);
        await unarchiveSession.SaveChangesAsync();

        // Events should be visible again
        await using var querySession2 = theStore.QuerySession();
        var events2 = await querySession2.Events.FetchStreamAsync(streamId);
        events2.Count.ShouldBe(2);
    }

    [Fact]
    public async Task unarchive_stream_allows_appending_again()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Resume Quest"));
        await theSession.SaveChangesAsync();

        // Archive
        await using var archiveSession = theStore.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        // UnArchive
        await using var unarchiveSession = theStore.LightweightSession();
        unarchiveSession.Events.UnArchiveStream(streamId);
        await unarchiveSession.SaveChangesAsync();

        // Append after unarchive
        await using var appendSession = theStore.LightweightSession();
        appendSession.Events.Append(streamId, new MembersJoined(3, "Mountain", ["Legolas"]));
        await appendSession.SaveChangesAsync();

        await using var querySession = theStore.QuerySession();
        var events = await querySession.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task unarchive_stream_clears_is_archived_on_state()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("State Quest"));
        await theSession.SaveChangesAsync();

        await using var archiveSession = theStore.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        await using var unarchiveSession = theStore.LightweightSession();
        unarchiveSession.Events.UnArchiveStream(streamId);
        await unarchiveSession.SaveChangesAsync();

        await using var querySession = theStore.QuerySession();
        var state = await querySession.Events.FetchStreamStateAsync(streamId);
        state.ShouldNotBeNull();
        state.IsArchived.ShouldBeFalse();
    }
}
