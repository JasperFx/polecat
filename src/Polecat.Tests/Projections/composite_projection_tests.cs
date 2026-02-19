using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Projections;

// Aggregate for stage 1: track quest party
public class CompositeQuestParty
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<string> Members { get; set; } = new();

    public static CompositeQuestParty Create(QuestStarted e) =>
        new() { Name = e.Name };

    public void Apply(MembersJoined e) => Members.AddRange(e.Members);
    public void Apply(MembersDeparted e)
    {
        foreach (var m in e.Members) Members.Remove(m);
    }
}

// Aggregate for stage 2: depends on quest party being projected first
public class QuestStats
{
    public Guid Id { get; set; }
    public int EventCount { get; set; }
    public string? LastLocation { get; set; }

    public static QuestStats Create(QuestStarted e) =>
        new() { EventCount = 1 };

    public void Apply(MembersJoined e)
    {
        EventCount++;
        LastLocation = e.Location;
    }

    public void Apply(MembersDeparted e) => EventCount++;
}

[Collection("integration")]
public class composite_projection_tests : IntegrationContext
{
    public composite_projection_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task composite_with_single_stage_snapshot()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "composite_test1";
            opts.Projections.CompositeProjectionFor("SingleStageComposite", composite =>
            {
                composite.Snapshot<CompositeQuestParty>();
            });
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Composite Quest"),
            new MembersJoined(1, "Town", ["Frodo", "Sam"]));
        await session.SaveChangesAsync();

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        await using var query = theStore.QuerySession();
        var party = await query.LoadAsync<CompositeQuestParty>(streamId);

        party.ShouldNotBeNull();
        party!.Name.ShouldBe("Composite Quest");
        party.Members.ShouldContain("Frodo");
        party.Members.ShouldContain("Sam");
    }

    [Fact]
    public async Task composite_with_two_stages()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "composite_test2";
            opts.Projections.CompositeProjectionFor("TwoStageComposite", composite =>
            {
                composite.Snapshot<CompositeQuestParty>(1);  // stage 1
                composite.Snapshot<QuestStats>(2);            // stage 2
            });
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Multi-Stage Quest"),
            new MembersJoined(1, "Village", ["Aragorn", "Legolas"]),
            new MembersDeparted(2, "Village", ["Legolas"]));
        await session.SaveChangesAsync();

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        await using var query = theStore.QuerySession();
        var party = await query.LoadAsync<CompositeQuestParty>(streamId);
        var stats = await query.LoadAsync<QuestStats>(streamId);

        party.ShouldNotBeNull();
        party!.Name.ShouldBe("Multi-Stage Quest");
        party.Members.Count.ShouldBe(1);
        party.Members.ShouldContain("Aragorn");

        stats.ShouldNotBeNull();
        stats!.EventCount.ShouldBe(3);
        stats.LastLocation.ShouldBe("Village");
    }

    [Fact]
    public async Task composite_with_multiple_snapshots_in_same_stage()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "composite_test3";
            opts.Projections.CompositeProjectionFor("ParallelComposite", composite =>
            {
                composite.Snapshot<CompositeQuestParty>();
                composite.Snapshot<QuestStats>();
            });
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Parallel Quest"),
            new MembersJoined(1, "Forest", ["Gandalf"]));
        await session.SaveChangesAsync();

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        await using var query = theStore.QuerySession();
        var party = await query.LoadAsync<CompositeQuestParty>(streamId);
        var stats = await query.LoadAsync<QuestStats>(streamId);

        party.ShouldNotBeNull();
        party!.Name.ShouldBe("Parallel Quest");
        party.Members.ShouldContain("Gandalf");

        stats.ShouldNotBeNull();
        stats!.EventCount.ShouldBe(2);
    }

    [Fact]
    public async Task composite_processes_appended_events()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "composite_test4";
            opts.Projections.CompositeProjectionFor("AppendComposite", composite =>
            {
                composite.Snapshot<CompositeQuestParty>();
            });
        });

        var streamId = Guid.NewGuid();
        await using var session1 = theStore.LightweightSession();
        session1.Events.StartStream(streamId, new QuestStarted("Append Quest"));
        await session1.SaveChangesAsync();

        // Process initial events
        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);

        // Append more events
        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId, new MembersJoined(2, "Cave", ["Bilbo"]));
        await session2.SaveChangesAsync();

        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        await using var query = theStore.QuerySession();
        var party = await query.LoadAsync<CompositeQuestParty>(streamId);

        party.ShouldNotBeNull();
        party!.Members.ShouldContain("Bilbo");
    }
}
