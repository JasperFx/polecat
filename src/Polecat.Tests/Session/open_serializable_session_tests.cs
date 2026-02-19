using System.Data;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Session;

[Collection("integration")]
public class open_serializable_session_tests : IntegrationContext
{
    public open_serializable_session_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task open_session_with_serializable_isolation()
    {
        await using var session = await theStore.OpenSessionAsync(new SessionOptions
        {
            IsolationLevel = IsolationLevel.Serializable
        });

        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue" });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task open_session_with_read_committed_does_not_eagerly_start_transaction()
    {
        await using var session = await theStore.OpenSessionAsync(new SessionOptions
        {
            IsolationLevel = IsolationLevel.ReadCommitted
        });

        session.Store(new Target { Id = Guid.NewGuid(), Color = "Green" });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task serializable_session_can_read_and_write()
    {
        var id = Guid.NewGuid();
        theSession.Store(new Target { Id = id, Color = "Original" });
        await theSession.SaveChangesAsync();

        await using var session = await theStore.OpenSessionAsync(new SessionOptions
        {
            IsolationLevel = IsolationLevel.Serializable
        });

        var loaded = await session.LoadAsync<Target>(id);
        loaded.ShouldNotBeNull();
        loaded!.Color.ShouldBe("Original");

        session.Store(new Target { Id = id, Color = "Updated" });
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var final = await query.LoadAsync<Target>(id);
        final!.Color.ShouldBe("Updated");
    }

    [Fact]
    public async Task serializable_session_with_events()
    {
        var streamId = Guid.NewGuid();

        await using var session = await theStore.OpenSessionAsync(new SessionOptions
        {
            IsolationLevel = IsolationLevel.Serializable
        });

        session.Events.StartStream(streamId, new QuestStarted("Serializable Quest"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
    }
}
