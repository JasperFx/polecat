using Polecat.Exceptions;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class event_stream_edge_cases : IntegrationContext
{
    public event_stream_edge_cases(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task empty_append_is_handled()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Empty Append"));
        await theSession.SaveChangesAsync();

        // Append with no events
        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId);
        await session2.SaveChangesAsync();

        // Version should remain at 1
        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldNotBeNull();
        state!.Version.ShouldBe(1);
    }

    [Fact]
    public async Task archive_then_fetch_for_writing_returns_empty()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archived Writing"),
            new MembersJoined(1, "Town", ["A"]));
        await theSession.SaveChangesAsync();

        await using var archiveSession = theStore.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        // FetchForWriting on archived stream should return null aggregate
        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestAggregate>(streamId);

        // Archived stream events are excluded from fetch, so aggregate should be null
        stream.Aggregate.ShouldBeNull();
    }

    [Fact]
    public async Task concurrent_archive_and_append_race()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Race"));
        await theSession.SaveChangesAsync();

        // Archive the stream first
        await using var archiveSession = theStore.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        // Now try to append — should throw because stream is archived
        await using var appendSession = theStore.LightweightSession();
        appendSession.Events.Append(streamId, new MembersJoined(1, "Cave", ["X"]));

        var ex = await Should.ThrowAsync<InvalidStreamException>(async () =>
        {
            await appendSession.SaveChangesAsync();
        });
        ex.Message.ShouldContain("archived");
    }

    [Fact]
    public async Task tombstone_then_start_stream_with_same_id()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Original"));
        await theSession.SaveChangesAsync();

        await using var tombstoneSession = theStore.LightweightSession();
        tombstoneSession.Events.TombstoneStream(streamId);
        await tombstoneSession.SaveChangesAsync();

        // Reuse the same ID
        await using var session3 = theStore.LightweightSession();
        session3.Events.StartStream(streamId, new QuestStarted("Reborn"));
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
        events[0].Data.ShouldBeOfType<QuestStarted>().Name.ShouldBe("Reborn");
    }

    [Fact]
    public async Task fetch_stream_with_version_zero_returns_all_events()
    {
        // version: 0 means "no version cap" — returns all events
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Version Zero"),
            new MembersJoined(1, "Town", ["A"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, version: 0);

        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task multiple_start_and_append_in_single_save()
    {
        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        theSession.Events.StartStream(stream1, new QuestStarted("Quest 1"));
        theSession.Events.StartStream(stream2, new QuestStarted("Quest 2"));
        theSession.Events.Append(stream1, new MembersJoined(1, "Forest", ["Elf"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events1 = await query.Events.FetchStreamAsync(stream1);
        var events2 = await query.Events.FetchStreamAsync(stream2);

        events1.Count.ShouldBe(2);
        events2.Count.ShouldBe(1);
    }

    [Fact]
    public async Task archived_stream_excluded_from_fetch()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archived Fetch"),
            new MembersJoined(1, "Start", ["Hero"]),
            new MonsterSlain("Dragon", 100));
        await theSession.SaveChangesAsync();

        await using var archiveSession = theStore.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(0);
    }
}
