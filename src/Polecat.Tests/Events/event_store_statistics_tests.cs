using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class event_store_statistics_tests : IntegrationContext
{
    public event_store_statistics_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_fetch_event_store_statistics()
    {
        // Start a couple of streams with events
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();

        theSession.Events.StartStream(id1,
            new QuestStarted("Stats Quest 1"),
            new MembersJoined(1, "Town", ["A"]));
        theSession.Events.StartStream(id2,
            new QuestStarted("Stats Quest 2"));
        await theSession.SaveChangesAsync();

        var stats = await theStore.Advanced.FetchEventStoreStatistics();

        // We have at least the events/streams we just created
        // (other tests may have added more in the shared DB)
        stats.EventCount.ShouldBeGreaterThanOrEqualTo(3);
        stats.StreamCount.ShouldBeGreaterThanOrEqualTo(2);
        stats.EventSequenceNumber.ShouldBeGreaterThanOrEqualTo(3);
    }
}
