using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public class event_store_sequence_number_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    ///     <c>IDENT_CURRENT</c> answers with the seed on a table that has never held a row, so an empty
    ///     store used to report that its sequence was already at 1 (#842).
    /// </summary>
    [Fact]
    public async Task an_empty_store_has_a_sequence_number_of_zero()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var statistics = await theStore.Advanced.FetchEventStoreStatistics(Ct);

        statistics.EventCount.ShouldBe(0);
        statistics.EventSequenceNumber.ShouldBe(0);
    }

    [Fact]
    public async Task the_sequence_number_is_the_last_one_assigned()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var session = theStore.LightweightSession();
        var streamId = Guid.NewGuid();
        session.Events.StartStream(streamId, new QuestStarted("One"), new QuestStarted("Two"));
        await session.SaveChangesAsync(Ct);

        var last = (await session.Events.FetchStreamAsync(streamId, token: Ct)).Max(x => x.Sequence);

        (await theStore.Advanced.FetchEventStoreStatistics(Ct)).EventSequenceNumber.ShouldBe(last);
    }

    /// <summary>
    ///     What makes this a sequence number rather than a count or a <c>MAX(seq_id)</c>: it does not
    ///     come back down when the events do.
    /// </summary>
    [Fact]
    public async Task the_sequence_number_survives_the_events_it_counted()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new QuestStarted("One"));
            await session.SaveChangesAsync(Ct);
        }

        var before = await theStore.Advanced.FetchEventStoreStatistics(Ct);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.TombstoneStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        var after = await theStore.Advanced.FetchEventStoreStatistics(Ct);

        after.EventCount.ShouldBe(0);
        after.EventSequenceNumber.ShouldBe(before.EventSequenceNumber);
    }
}
