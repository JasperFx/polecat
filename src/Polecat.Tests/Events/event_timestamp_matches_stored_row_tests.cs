using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     #802: the in-memory event the session stamped and the row QuickAppend wrote used two different
///     clocks (client UtcNow vs the server's SYSDATETIMEOFFSET()), so an inline projection saw one
///     timestamp and a fetch or rebuild another.
/// </summary>
[Collection("integration")]
public class event_timestamp_matches_stored_row_tests : IntegrationContext
{
    public event_timestamp_matches_stored_row_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task the_stored_timestamp_is_the_one_on_the_appended_event()
    {
        var streamId = Guid.NewGuid();
        var action = theSession.Events.StartStream(streamId,
            new QuestStarted("Timestamps"),
            new MembersJoined(1, "Town", ["A"]));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var before = action.Events.Select(e => e.Timestamp).ToArray();

        await using var query = theStore.QuerySession();
        var fetched = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        fetched.Count.ShouldBe(2);
        for (var i = 0; i < fetched.Count; i++)
        {
            fetched[i].Timestamp.ShouldBe(before[i]);
        }
    }
}
