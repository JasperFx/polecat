using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public record OrderedAppendHappened(int Writer, int Round);

/// <summary>
///     marten#6097 / #6098 parity. Marten's Rich append mode reserved <c>seq_id</c> values and read the stream
///     versions in separate statements ahead of the stream lock, so two writers appending to one stream could commit
///     the higher version under the lower <c>seq_id</c>, and a stream read in <c>seq_id</c> order came back out of
///     version order. Polecat has no Rich mode: every append reads the stream's version under an update lock held
///     to the commit, and <c>seq_id</c> is assigned by the <c>INSERT</c> itself, after that lock.
/// </summary>
public class concurrent_appends_keep_seq_id_in_version_order : OneOffConfigurationsContext
{
    [Fact]
    public async Task concurrent_plain_appends_to_one_stream_read_back_in_version_order()
    {
        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new OrderedAppendHappened(-1, -1));
            await session.SaveChangesAsync();
        }

        const int writers = 8;
        const int rounds = 15;

        // No expected versions, the case marten#6097 failed in
        var tasks = Enumerable.Range(0, writers).Select(writer => Task.Run(async () =>
        {
            for (var round = 0; round < rounds; round++)
            {
                await using var session = theStore.LightweightSession();
                session.Events.Append(streamId, new OrderedAppendHappened(writer, round),
                    new OrderedAppendHappened(writer, round));
                await session.SaveChangesAsync();
            }
        }));

        await Task.WhenAll(tasks);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);

        events.Count.ShouldBe(1 + writers * rounds * 2);
        events.Select(x => x.Version).ShouldBe(Enumerable.Range(1, events.Count).Select(x => (long)x));

        // FetchStreamAsync orders by seq_id, so the versions above came back in seq_id order. Check that it did.
        events.Select(x => x.Sequence).ShouldBeInOrder(SortDirection.Ascending);
    }

    [Fact]
    public void polecat_only_appends_in_quick_mode()
    {
        // opts.Events has no AppendMode to set, and the closed-shape dialect refuses to build a Rich appender
        typeof(EventStoreOptions).GetProperty("AppendMode").ShouldBeNull();
        theStore.Options.EventGraph.AppendMode.ShouldBe(EventAppendMode.Quick);

        Should.Throw<NotSupportedException>(() =>
            new Polecat.Events.Storage.SqlServerEventStoreDialect().BuildRichDescriptor(theStore.Options.EventGraph,
                theStore.Options.StorageSerializer));
    }
}
