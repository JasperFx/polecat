using Polecat.Tests.Harness;

namespace Polecat.Tests.Compliance;

/// <summary>
///     #858: what the compliance fixtures rely on to keep their ordering facts deterministic.
/// </summary>
public class server_clock_tick_listener_tests : OneOffConfigurationsContext
{
    public class Doc
    {
        public Guid Id { get; set; }
    }

    [Fact]
    public async Task back_to_back_commits_never_share_a_timestamp()
    {
        ConfigureStore(opts => opts.Listeners.Add(ServerClockTickListener.Instance));

        var token = TestContext.Current.CancellationToken;
        var docs = Enumerable.Range(0, 30).Select(_ => new Doc { Id = Guid.NewGuid() }).ToArray();

        foreach (var doc in docs)
        {
            await using var session = theStore.LightweightSession();
            session.Store(doc);
            await session.SaveChangesAsync(token);
        }

        await using var query = theStore.QuerySession();
        var written = new List<DateTimeOffset>();
        foreach (var doc in docs)
        {
            var metadata = await query.MetadataForAsync(doc, token);
            written.Add(metadata!.LastModified);
        }

        written.ShouldBeInOrder(SortDirection.Ascending);
        written.Distinct().Count().ShouldBe(docs.Length);
    }
}
