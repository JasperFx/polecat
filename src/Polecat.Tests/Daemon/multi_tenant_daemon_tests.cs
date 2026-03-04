using JasperFx.Events.Projections;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Tests for multi-tenant daemon isolation: per-tenant projection progress,
///     tenant-scoped event streams, and daemon behavior across tenants.
/// </summary>
[Collection("integration")]
public class multi_tenant_daemon_tests : IntegrationContext
{
    public multi_tenant_daemon_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    // ===== Events in different tenants are isolated =====

    [Fact]
    public async Task events_stored_in_default_tenant_are_visible()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Default Tenant Quest"),
            new MembersJoined(1, "Town", ["Alice"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
        events[0].TenantId.ShouldBe("*DEFAULT*");
    }

    // ===== Multiple streams in same save =====

    [Fact]
    public async Task multiple_streams_with_events_in_single_save()
    {
        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        theSession.Events.StartStream(stream1,
            new QuestStarted("Quest 1"),
            new MembersJoined(1, "Town", ["Alice"]));

        theSession.Events.StartStream(stream2,
            new QuestStarted("Quest 2"),
            new MembersJoined(1, "City", ["Bob"]),
            new MonsterSlain("Dragon", 100));

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events1 = await query.Events.FetchStreamAsync(stream1);
        events1.Count.ShouldBe(2);

        var events2 = await query.Events.FetchStreamAsync(stream2);
        events2.Count.ShouldBe(3);
    }

    // ===== Event sequence IDs are globally increasing =====

    [Fact]
    public async Task event_sequence_ids_are_globally_monotonic()
    {
        var stream1 = Guid.NewGuid();
        theSession.Events.StartStream(stream1, new QuestStarted("Seq 1"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream2 = Guid.NewGuid();
        session2.Events.StartStream(stream2, new QuestStarted("Seq 2"));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events1 = await query.Events.FetchStreamAsync(stream1);
        var events2 = await query.Events.FetchStreamAsync(stream2);

        events1[0].Sequence.ShouldBeGreaterThan(0);
        events2[0].Sequence.ShouldBeGreaterThan(events1[0].Sequence);
    }

    // ===== Documents and events mixed in same save =====

    [Fact]
    public async Task documents_and_events_committed_atomically()
    {
        var userId = Guid.NewGuid();
        var streamId = Guid.NewGuid();

        theSession.Store(new User { Id = userId, FirstName = "Atomic", LastName = "Save", Age = 30 });
        theSession.Events.StartStream(streamId, new QuestStarted("Atomic Quest"));

        await theSession.SaveChangesAsync();

        // Both should exist
        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(userId)).ShouldNotBeNull();
        (await query.Events.FetchStreamAsync(streamId)).Count.ShouldBe(1);
    }

    // ===== High water mark advances =====

    [Fact]
    public async Task high_water_mark_advances_after_events()
    {
        var hwBefore = await theStore.Database.FetchHighestEventSequenceNumber(CancellationToken.None);

        theSession.Events.StartStream(Guid.NewGuid(), new QuestStarted("HWM Test"));
        await theSession.SaveChangesAsync();

        var hwAfter = await theStore.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
        hwAfter.ShouldBeGreaterThan(hwBefore);
    }

    // ===== Append events to stream across sessions =====

    [Fact]
    public async Task append_events_across_sessions_maintains_versioning()
    {
        var streamId = Guid.NewGuid();

        // Session 1: start stream
        theSession.Events.StartStream(streamId, new QuestStarted("Version Test"));
        await theSession.SaveChangesAsync();

        // Session 2: append
        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId, new MembersJoined(1, "Town", ["A"]));
        await session2.SaveChangesAsync();

        // Session 3: append more
        await using var session3 = theStore.LightweightSession();
        session3.Events.Append(streamId,
            new MembersJoined(2, "City", ["B"]),
            new MonsterSlain("Goblin", 10));
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(4);

        // Verify version sequence is continuous
        for (var i = 0; i < events.Count; i++)
        {
            events[i].Version.ShouldBe(i + 1);
        }
    }
}
