using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Shouldly;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class snapshot_edge_cases : IntegrationContext
{
    public snapshot_edge_cases(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreWithInlineSnapshot()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "snapshot_edge";
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Inline);
        });
        return theStore;
    }

    [Fact]
    public async Task snapshot_survives_archive_unarchive_cycle()
    {
        var store = await CreateStoreWithInlineSnapshot();

        var streamId = Guid.NewGuid();
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(streamId,
            new QuestStarted("Survive Quest"),
            new MembersJoined(1, "Town", ["Aragorn", "Legolas"]));
        await session1.SaveChangesAsync();

        // Verify snapshot exists
        var (json1, v1) = await ReadSnapshotFromDb(streamId, "snapshot_edge");
        v1.ShouldBe(2);
        json1.ShouldNotBeNullOrEmpty();

        // Archive
        await using var archiveSession = store.LightweightSession();
        archiveSession.Events.ArchiveStream(streamId);
        await archiveSession.SaveChangesAsync();

        // Unarchive
        await using var unarchiveSession = store.LightweightSession();
        unarchiveSession.Events.UnArchiveStream(streamId);
        await unarchiveSession.SaveChangesAsync();

        // Snapshot should still be there
        var (json2, v2) = await ReadSnapshotFromDb(streamId, "snapshot_edge");
        v2.ShouldBe(2);
        json2.ShouldNotBeNullOrEmpty();

        // AggregateStream should work using the snapshot
        await using var query = store.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId);
        party.ShouldNotBeNull();
        party!.Name.ShouldBe("Survive Quest");
        party.Members.ShouldContain("Aragorn");
        party.Members.ShouldContain("Legolas");
    }

    [Fact]
    public async Task snapshot_on_multi_event_append()
    {
        var store = await CreateStoreWithInlineSnapshot();

        var streamId = Guid.NewGuid();
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(streamId,
            new QuestStarted("Multi"),
            new MembersJoined(1, "Town", ["A"]),
            new MembersJoined(2, "Forest", ["B"]),
            new MonsterSlain("Goblin", 10),
            new MembersJoined(3, "Mountain", ["C"]));
        await session1.SaveChangesAsync();

        // Snapshot should capture all 5 events
        var (json, version) = await ReadSnapshotFromDb(streamId, "snapshot_edge");
        version.ShouldBe(5);
        json!.ShouldContain("A");
        json!.ShouldContain("B");
        json!.ShouldContain("C");
        json!.ShouldContain("Goblin");
    }

    [Fact]
    public async Task aggregate_without_snapshot_full_replay()
    {
        // No snapshot projection registered — full replay path
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Full Replay"),
            new MembersJoined(1, "Village", ["Frodo"]),
            new MonsterSlain("Troll", 50));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var party = await query.Events.AggregateStreamAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party!.Name.ShouldBe("Full Replay");
        party.Members.ShouldContain("Frodo");
        party.MonstersSlain.ShouldContain("Troll");
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
