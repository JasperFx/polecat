using JasperFx.Events;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class stream_compacting_tests : IntegrationContext
{
    public stream_compacting_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task compact_stream_at_latest()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_latest");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Compact Quest"),
            new MembersJoined(1, "Town", ["Alice", "Bob"]),
            new MonsterSlain("Goblin", 10),
            new MembersJoined(2, "Forest", ["Charlie"]));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.CompactStreamAsync<QuestParty>(streamId);
        await session2.SaveChangesAsync();

        // After compaction, only one event should remain (the Compacted<T>)
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);

        var compacted = events[0].Data.ShouldBeOfType<Compacted<QuestParty>>();
        compacted.Snapshot.ShouldNotBeNull();
        compacted.Snapshot.Name.ShouldBe("Compact Quest");
        compacted.Snapshot.Members.ShouldContain("Alice");
        compacted.Snapshot.Members.ShouldContain("Bob");
        compacted.Snapshot.Members.ShouldContain("Charlie");
        compacted.Snapshot.MonstersSlain.ShouldContain("Goblin");
    }

    [Fact]
    public async Task compact_stream_preserves_stream_version()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_version");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Version Quest"),
            new MembersJoined(1, "Town", ["Hero"]),
            new MonsterSlain("Dragon", 100));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.CompactStreamAsync<QuestParty>(streamId);
        await session2.SaveChangesAsync();

        // Can still append events after compaction
        await using var session3 = theStore.LightweightSession();
        session3.Events.Append(streamId, new MembersJoined(3, "Castle", ["Knight"]));
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2); // Compacted + new event
    }

    [Fact]
    public async Task compact_stream_is_idempotent()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_idempotent");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Idempotent Quest"),
            new MembersJoined(1, "Town", ["Hero"]));
        await theSession.SaveChangesAsync();

        // First compaction
        await using var session2 = theStore.LightweightSession();
        await session2.Events.CompactStreamAsync<QuestParty>(streamId);
        await session2.SaveChangesAsync();

        // Second compaction should be a no-op (already compacted)
        await using var session3 = theStore.LightweightSession();
        await session3.Events.CompactStreamAsync<QuestParty>(streamId);
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
        events[0].Data.ShouldBeOfType<Compacted<QuestParty>>();
    }

    [Fact]
    public async Task compact_empty_stream_is_noop()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_empty");

        var streamId = Guid.NewGuid();

        // No events for this stream, should not throw
        await using var session = theStore.LightweightSession();
        await session.Events.CompactStreamAsync<QuestParty>(streamId);
        await session.SaveChangesAsync();
    }
}
