using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class archive_edge_case_tests : IntegrationContext
{
    public archive_edge_case_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task double_archive_is_idempotent()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Double Archive"));
        await theSession.SaveChangesAsync();

        // Archive once
        await using var session2 = theStore.LightweightSession();
        session2.Events.ArchiveStream(streamId);
        await session2.SaveChangesAsync();

        // Archive again — should not throw
        await using var session3 = theStore.LightweightSession();
        session3.Events.ArchiveStream(streamId);
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldNotBeNull();
        state!.IsArchived.ShouldBeTrue();
    }

    [Fact]
    public async Task double_unarchive_is_idempotent()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Double Unarchive"));
        await theSession.SaveChangesAsync();

        // Archive
        await using var session2 = theStore.LightweightSession();
        session2.Events.ArchiveStream(streamId);
        await session2.SaveChangesAsync();

        // Unarchive once
        await using var session3 = theStore.LightweightSession();
        session3.Events.UnArchiveStream(streamId);
        await session3.SaveChangesAsync();

        // Unarchive again — should not throw
        await using var session4 = theStore.LightweightSession();
        session4.Events.UnArchiveStream(streamId);
        await session4.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldNotBeNull();
        state!.IsArchived.ShouldBeFalse();
    }

    [Fact]
    public async Task archive_nonexistent_stream_is_noop()
    {
        var streamId = Guid.NewGuid();

        // Archive a stream that doesn't exist — should not throw
        await using var session = theStore.LightweightSession();
        session.Events.ArchiveStream(streamId);
        await session.SaveChangesAsync();

        // Verify stream doesn't exist
        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldBeNull();
    }

    [Fact]
    public async Task fetch_for_writing_on_archived_stream()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archived Writing"),
            new MembersJoined(1, "Town", ["A", "B"]));
        await theSession.SaveChangesAsync();

        await using var archiveSession = theStore.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        // FetchForWriting on archived stream — events are filtered out
        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);

        // Aggregate should be null since archived events are excluded
        stream.Aggregate.ShouldBeNull();
    }
}
