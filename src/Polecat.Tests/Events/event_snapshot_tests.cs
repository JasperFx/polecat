using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Shouldly;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class event_snapshot_tests : IntegrationContext
{
    public event_snapshot_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreWithInlineSnapshot()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "snapshot_tests";
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Inline);
        });
        return theStore;
    }

    [Fact]
    public async Task inline_snapshot_writes_to_pc_streams()
    {
        var store = await CreateStoreWithInlineSnapshot();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Snapshot Quest"),
            new MembersJoined(1, "Town", ["Frodo", "Sam"]));
        await session.SaveChangesAsync();

        // Verify snapshot was written to pc_streams
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT snapshot, snapshot_version FROM [snapshot_tests].[pc_streams] WHERE id = @id;";
        cmd.Parameters.AddWithValue("@id", streamId);

        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        var snapshotJson = reader.GetString(0);
        var snapshotVersion = reader.GetInt32(1);

        snapshotJson.ShouldNotBeNullOrEmpty();
        snapshotVersion.ShouldBe(2); // 2 events
    }

    [Fact]
    public async Task aggregate_stream_uses_snapshot_for_partial_replay()
    {
        var store = await CreateStoreWithInlineSnapshot();

        var streamId = Guid.NewGuid();

        // Start stream with initial events — snapshot will be cached at version 2
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(streamId,
            new QuestStarted("Fast Quest"),
            new MembersJoined(1, "Town", ["Aragorn"]));
        await session1.SaveChangesAsync();

        // Append more events — snapshot updates to version 4
        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamId,
            new MembersJoined(2, "Forest", ["Legolas"]),
            new MonsterSlain("Troll", 50));
        await session2.SaveChangesAsync();

        // AggregateStream should use snapshot + only replay events after snapshot
        await using var query = store.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Fast Quest");
        party.Members.ShouldContain("Aragorn");
        party.Members.ShouldContain("Legolas");
        party.MonstersSlain.ShouldContain("Troll");
    }

    [Fact]
    public async Task aggregate_stream_works_without_snapshot()
    {
        // No snapshot projection registered — should still work via full replay
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("No Snapshot"),
            new MembersJoined(1, "Village", ["Bilbo"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("No Snapshot");
        party.Members.ShouldContain("Bilbo");
    }

    [Fact]
    public async Task snapshot_updates_on_append()
    {
        var store = await CreateStoreWithInlineSnapshot();

        var streamId = Guid.NewGuid();
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(streamId, new QuestStarted("Updating"));
        await session1.SaveChangesAsync();

        // Check initial snapshot version
        var (_, v1) = await ReadSnapshotFromDb(streamId, "snapshot_tests");
        v1.ShouldBe(1);

        // Append more events
        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamId,
            new MembersJoined(1, "Camp", ["Gandalf"]),
            new MembersJoined(2, "Road", ["Gimli"]));
        await session2.SaveChangesAsync();

        // Snapshot version should update
        var (json, v2) = await ReadSnapshotFromDb(streamId, "snapshot_tests");
        v2.ShouldBe(3); // 1 initial + 2 appended
        json.ShouldContain("Gandalf");
        json.ShouldContain("Gimli");
    }

    [Fact]
    public async Task aggregate_with_version_cap_returns_snapshot_when_version_within()
    {
        var store = await CreateStoreWithInlineSnapshot();

        var streamId = Guid.NewGuid();
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(streamId,
            new QuestStarted("Capped"),
            new MembersJoined(1, "Start", ["A", "B"]),
            new MonsterSlain("Goblin", 10));
        await session1.SaveChangesAsync();
        // Snapshot at version 3

        // Request aggregate at version 2 (less than snapshot version)
        // This should NOT use the snapshot — falls back to full replay up to version 2
        await using var query = store.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId, version: 2);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Capped");
        party.Members.ShouldContain("A");
        party.MonstersSlain.ShouldBeEmpty(); // MonsterSlain is at version 3
    }

    [Fact]
    public async Task aggregate_with_explicit_from_version_skips_snapshot()
    {
        var store = await CreateStoreWithInlineSnapshot();

        var streamId = Guid.NewGuid();
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(streamId,
            new QuestStarted("FromVer"),
            new MembersJoined(1, "Here", ["X"]));
        await session1.SaveChangesAsync();

        // Explicit fromVersion bypasses snapshot optimization
        await using var query = store.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId, fromVersion: 1);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("FromVer");
    }

    private async Task<(string? json, int version)> ReadSnapshotFromDb(Guid streamId, string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT snapshot, snapshot_version FROM [{schema}].[pc_streams] WHERE id = @id;";
        cmd.Parameters.AddWithValue("@id", streamId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync() && !reader.IsDBNull(0))
        {
            return (reader.GetString(0), reader.GetInt32(1));
        }

        return (null, 0);
    }
}
