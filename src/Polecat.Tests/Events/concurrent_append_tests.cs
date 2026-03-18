using Polecat.Exceptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class concurrent_append_tests : IntegrationContext
{
    public concurrent_append_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task append_optimistic_to_existing_stream()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Optimistic Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.AppendOptimistic(streamId, new MembersJoined(1, "Town", ["Hero"]));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task append_optimistic_to_nonexistent_stream_throws()
    {
        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        await Should.ThrowAsync<NonExistentStreamException>(async () =>
        {
            await session.Events.AppendOptimistic(streamId, new QuestStarted("Ghost"));
        });
    }

    [Fact]
    public async Task append_exclusive_to_existing_stream()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Exclusive Quest"));
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.AppendExclusive(streamId, new MembersJoined(1, "Castle", ["Knight"]));
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task append_exclusive_to_nonexistent_stream_throws()
    {
        var streamId = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        await Should.ThrowAsync<NonExistentStreamException>(async () =>
        {
            await session.Events.AppendExclusive(streamId, new QuestStarted("Ghost"));
        });
    }

    [Fact]
    public async Task append_exclusive_releases_lock_after_save()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Lock Test"));
        await theSession.SaveChangesAsync();

        // First exclusive append + save
        await using var session2 = theStore.LightweightSession();
        await session2.Events.AppendExclusive(streamId, new MembersJoined(1, "Town", ["A"]));
        await session2.SaveChangesAsync();

        // Second exclusive append should succeed (lock released)
        await using var session3 = theStore.LightweightSession();
        await session3.Events.AppendExclusive(streamId, new MonsterSlain("Rat", 1));
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
    }
}
