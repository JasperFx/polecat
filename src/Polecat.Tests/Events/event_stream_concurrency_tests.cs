using JasperFx.Events;
using Polecat.Exceptions;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     Tests for event stream concurrency, archive/tombstone edge cases,
///     and multi-event append ordering.
/// </summary>
[Collection("integration")]
public class event_stream_concurrency_tests : IntegrationContext
{
    public event_stream_concurrency_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    // ===== Concurrent appends with expected version =====

    [Fact]
    public async Task concurrent_appends_with_same_expected_version_one_fails()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Concurrency Test"));
        await theSession.SaveChangesAsync();

        // Two sessions read current version (1) and try to append with same expected version
        await using var session1 = theStore.LightweightSession();
        await using var session2 = theStore.LightweightSession();

        session1.Events.Append(streamId, 2, new MembersJoined(1, "Town", ["Alice"]));
        session2.Events.Append(streamId, 2, new MembersJoined(1, "City", ["Bob"]));

        // First one should succeed
        await session1.SaveChangesAsync();

        // Second one should fail with version conflict
        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            session2.SaveChangesAsync());
    }

    // ===== Append to archived stream =====

    [Fact]
    public async Task append_to_archived_stream_throws()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Archive Me"));
        await theSession.SaveChangesAsync();

        // Archive the stream
        await using var session2 = theStore.LightweightSession();
        session2.Events.ArchiveStream(streamId);
        await session2.SaveChangesAsync();

        // Try to append to archived stream
        await using var session3 = theStore.LightweightSession();
        session3.Events.Append(streamId, new MembersJoined(2, "Somewhere", ["Charlie"]));

        await Should.ThrowAsync<InvalidStreamException>(session3.SaveChangesAsync());
    }

    // ===== Unarchive then append =====

    [Fact]
    public async Task unarchive_then_append_succeeds()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("UnArchive Me"));
        await theSession.SaveChangesAsync();

        // Archive
        await using var session2 = theStore.LightweightSession();
        session2.Events.ArchiveStream(streamId);
        await session2.SaveChangesAsync();

        // Unarchive
        await using var session3 = theStore.LightweightSession();
        session3.Events.UnArchiveStream(streamId);
        await session3.SaveChangesAsync();

        // Now append should work
        await using var session4 = theStore.LightweightSession();
        session4.Events.Append(streamId, new MembersJoined(3, "Back", ["Dave"]));
        await session4.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
    }

    // ===== Tombstone then attempt operations =====

    [Fact]
    public async Task tombstone_stream_removes_all_data()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Tombstone Me"),
            new MembersJoined(1, "Town", ["Alice", "Bob"]));
        await theSession.SaveChangesAsync();

        // Tombstone
        await using var session2 = theStore.LightweightSession();
        session2.Events.TombstoneStream(streamId);
        await session2.SaveChangesAsync();

        // Fetch should return empty
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(0);
    }

    // ===== Start stream with duplicate ID =====

    [Fact]
    public async Task start_stream_with_duplicate_id_throws()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("First"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Events.StartStream(streamId, new QuestStarted("Duplicate"));

        await Should.ThrowAsync<ExistingStreamIdCollisionException>(session2.SaveChangesAsync());
    }

    // ===== Append multiple events in order =====

    [Fact]
    public async Task append_multiple_events_preserves_order()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Ordering"),
            new MembersJoined(1, "Town", ["Alice"]),
            new MembersJoined(2, "Forest", ["Bob"]),
            new MonsterSlain("Dragon", 100));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(4);

        events[0].Version.ShouldBe(1);
        events[0].Data.ShouldBeOfType<QuestStarted>();

        events[1].Version.ShouldBe(2);
        events[1].Data.ShouldBeOfType<MembersJoined>();
        ((MembersJoined)events[1].Data).Location.ShouldBe("Town");

        events[2].Version.ShouldBe(3);
        events[2].Data.ShouldBeOfType<MembersJoined>();
        ((MembersJoined)events[2].Data).Location.ShouldBe("Forest");

        events[3].Version.ShouldBe(4);
        events[3].Data.ShouldBeOfType<MonsterSlain>();
    }

    // ===== Append events across multiple sessions =====

    [Fact]
    public async Task sequential_appends_across_sessions_increment_version()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Multi Session"));
        await theSession.SaveChangesAsync();

        for (var i = 0; i < 3; i++)
        {
            await using var session = theStore.LightweightSession();
            session.Events.Append(streamId, new MembersJoined(i + 1, $"Location{i}", [$"Member{i}"]));
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(4); // 1 start + 3 appends

        for (var i = 0; i < events.Count; i++)
        {
            events[i].Version.ShouldBe(i + 1);
        }
    }

    // ===== FetchForWriting =====

    [Fact]
    public async Task fetch_for_writing_returns_current_version()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Fetch Write"),
            new MembersJoined(1, "Town", ["Alice"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<QuestParty>(streamId);

        stream.CurrentVersion.ShouldBe(2);
        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.Name.ShouldBe("Fetch Write");

        // Append via the stream
        stream.AppendOne(new MembersJoined(2, "Forest", ["Bob"]));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
    }

    // ===== FetchForWriting on non-existent stream =====

    [Fact]
    public async Task fetch_for_writing_non_existent_stream_returns_empty()
    {
        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<QuestParty>(Guid.NewGuid());

        stream.CurrentVersion.ShouldBe(0);
        stream.Aggregate.ShouldBeNull();
    }

    // ===== Multiple streams in one SaveChanges =====

    [Fact]
    public async Task multiple_streams_started_in_one_save()
    {
        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();
        var stream3 = Guid.NewGuid();

        theSession.Events.StartStream(stream1, new QuestStarted("Stream 1"));
        theSession.Events.StartStream(stream2, new QuestStarted("Stream 2"));
        theSession.Events.StartStream(stream3, new QuestStarted("Stream 3"));

        theSession.PendingChanges.Streams.Count.ShouldBe(3);

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(stream1)).Count.ShouldBe(1);
        (await query.Events.FetchStreamAsync(stream2)).Count.ShouldBe(1);
        (await query.Events.FetchStreamAsync(stream3)).Count.ShouldBe(1);
    }

    // ===== Fetch stream up to version =====

    [Fact]
    public async Task fetch_stream_up_to_version()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Version Fetch"),
            new MembersJoined(1, "Town", ["A"]),
            new MembersJoined(2, "City", ["B"]),
            new QuestEnded("Version Fetch"));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, version: 2);
        events.Count.ShouldBe(2);
        events[0].Version.ShouldBe(1);
        events[1].Version.ShouldBe(2);
    }
}
