using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Daemon;

[Collection("integration")]
public class async_daemon_tests : IntegrationContext
{
    public async_daemon_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreWithAsyncProjection()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
        });
        return theStore;
    }

    [Fact]
    public async Task projection_shards_are_registered()
    {
        var store = await CreateStoreWithAsyncProjection();
        var shards = store.Options.Projections.AllShards();
        shards.Count.ShouldBeGreaterThan(0);

        // Verify the shard has the correct name and lifecycle
        var all = store.Options.Projections.All;
        all.Count.ShouldBeGreaterThan(0);
        all.Any(x => x.Lifecycle == ProjectionLifecycle.Async).ShouldBeTrue();
    }

    [Fact]
    public async Task daemon_can_start_and_stop()
    {
        var store = await CreateStoreWithAsyncProjection();
        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();

        await daemon.StartAllAsync();
        daemon.IsRunning.ShouldBeTrue();

        await daemon.StopAllAsync();
    }

    [Fact]
    public async Task async_projection_processes_events()
    {
        var store = await CreateStoreWithAsyncProjection();

        // Insert events
        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Destroy the Ring"),
            new MembersJoined(1, "Rivendell", ["Aragorn", "Legolas", "Gimli"]));
        await session.SaveChangesAsync();

        // Run daemon to completion
        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        // Verify projected document
        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Destroy the Ring");
        party.Members.ShouldBe(["Aragorn", "Legolas", "Gimli"]);
        party.Location.ShouldBe("Rivendell");
    }

    [Fact]
    public async Task async_projection_tracks_progress()
    {
        var store = await CreateStoreWithAsyncProjection();

        // Insert events
        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Fellowship"),
            new MembersJoined(1, "Rivendell", ["Aragorn"]));
        await session.SaveChangesAsync();

        // Run daemon
        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        // Verify progress was recorded
        var highestSeq = await store.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
        var allProgress = await store.Database.AllProjectionProgress(CancellationToken.None);

        // Filter out HighWaterMark — look for projection-specific progress
        var projectionProgress = allProgress
            .Where(p => p.ShardName != "HighWaterMark")
            .ToList();

        projectionProgress.ShouldNotBeEmpty();
        projectionProgress.All(p => p.Sequence >= highestSeq).ShouldBeTrue();
    }

    [Fact]
    public async Task daemon_catches_up_to_events()
    {
        var store = await CreateStoreWithAsyncProjection();

        // Insert events before starting daemon
        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Late Start"),
            new MembersJoined(1, "Town", ["Hero"]));
        await session.SaveChangesAsync();

        // Start daemon and catch up
        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Late Start");
        party.Members.ShouldBe(["Hero"]);
    }

    [Fact]
    public async Task daemon_processes_append_events()
    {
        var store = await CreateStoreWithAsyncProjection();

        // Insert all events in two batches, then run daemon once
        var streamId = Guid.NewGuid();
        await using var session1 = store.LightweightSession();
        session1.Events.StartStream(streamId, new QuestStarted("Growing Quest"));
        await session1.SaveChangesAsync();

        await using var session2 = store.LightweightSession();
        session2.Events.Append(streamId,
            new MembersJoined(1, "Forest", ["Elf", "Dwarf"]),
            new ArrivedAtLocation("Mountain", 2));
        await session2.SaveChangesAsync();

        // Start daemon and catch up to all events
        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        await using var query = store.QuerySession();
        var party = await query.LoadAsync<QuestParty>(streamId);

        party.ShouldNotBeNull();
        party.Name.ShouldBe("Growing Quest");
        party.Members.ShouldBe(["Elf", "Dwarf"]);
        party.Location.ShouldBe("Mountain");
    }

    [Fact]
    public async Task multiple_streams_processed_by_daemon()
    {
        var store = await CreateStoreWithAsyncProjection();

        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.Events.StartStream(stream1,
            new QuestStarted("Quest Alpha"),
            new MembersJoined(1, "Town A", ["A1", "A2"]));
        session.Events.StartStream(stream2,
            new QuestStarted("Quest Beta"),
            new MembersJoined(1, "Town B", ["B1"]));
        await session.SaveChangesAsync();

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        await using var query = store.QuerySession();
        var party1 = await query.LoadAsync<QuestParty>(stream1);
        var party2 = await query.LoadAsync<QuestParty>(stream2);

        party1.ShouldNotBeNull();
        party1.Name.ShouldBe("Quest Alpha");
        party1.Members.ShouldBe(["A1", "A2"]);

        party2.ShouldNotBeNull();
        party2.Name.ShouldBe("Quest Beta");
        party2.Members.ShouldBe(["B1"]);
    }
}
