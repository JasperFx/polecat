using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public record StatsBerthOpened;

public record StatsBerthUsed;

public partial class StatsBerth
{
    public Guid Id { get; set; }
    public int Uses { get; set; }

    public static StatsBerth Create(StatsBerthOpened _) => new();

    public void Apply(StatsBerthUsed _) => Uses++;
}

/// <summary>
///     #1130 (jasperfx#1282, marten#6152): <c>FetchAggregateStreamStatsAsync</c> under
///     <see cref="Polecat.Events.EventGraph.UseArchivedStreamPartitioning" />, which the compliance suite cannot turn
///     on. Polecat partitions <c>pc_events</c> by <c>is_archived</c> and leaves <c>pc_streams</c> whole, so the stats
///     read, which only reads <c>pc_streams</c>, decides on the stream row's own <c>is_archived</c> flag either way.
/// </summary>
[Collection("integration")]
public class aggregate_stream_stats_with_archived_partitioning : IntegrationContext
{
    public aggregate_stream_stats_with_archived_partitioning(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        // The partition scheme and function are database-global and other schemas' pc_events may sit on them
        // (#1041), so only this schema's tables are dropped
        await TestSchema.DropSchemaTablesAsync("stats_archived_part");
        await TestSchema.DropSequencesAsync("stats_archived_part");

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "stats_archived_part";
            opts.EventGraph.UseArchivedStreamPartitioning = true;
            opts.Projections.Snapshot<StatsBerth>(SnapshotLifecycle.Inline);
        });
    }

    [Fact]
    public async Task archived_streams_count_only_when_asked_for()
    {
        var token = TestContext.Current.CancellationToken;

        var live = theSession.Events.StartStream<StatsBerth>(new StatsBerthOpened(), new StatsBerthUsed()).Id;
        var archived = theSession.Events.StartStream<StatsBerth>(new StatsBerthOpened(), new StatsBerthUsed(),
            new StatsBerthUsed(), new StatsBerthUsed()).Id;
        await theSession.SaveChangesAsync(token);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.ArchiveStream(archived);
            await session.SaveChangesAsync(token);
        }

        var database = (IEventDatabase)theStore.Database;

        var withoutArchived = (await database.FetchAggregateStreamStatsAsync(new AggregateStreamStatsRequest(), token))
            .ShouldHaveSingleItem();
        withoutArchived.AggregateType.ShouldBe(typeof(StatsBerth));
        withoutArchived.StreamCount.ShouldBe(1);
        withoutArchived.MaxVersion.ShouldBe(2);
        withoutArchived.LongestStreams.Single().Id.ShouldBe(live);

        var withArchived = (await database.FetchAggregateStreamStatsAsync(
            new AggregateStreamStatsRequest { IncludeArchived = true }, token)).ShouldHaveSingleItem();
        withArchived.StreamCount.ShouldBe(2);
        withArchived.TotalEvents.ShouldBe(6);
        withArchived.LongestStreams.Select(x => x.Id).ShouldBe([archived, live]);
    }

    [Fact]
    public void the_read_is_one_batch_over_pc_streams_alone()
    {
        var events = theStore.Options.EventGraph;

        var ranked = PolecatDatabase.AggregateStreamStatsSql(events, thresholdCount: 2, filterByTenant: false,
            includeArchived: false, rankLongest: true);
        ranked.ShouldNotContain("pc_events");
        ranked.ShouldContain("ROW_NUMBER()");
        ranked.ShouldContain("@t1");

        // No longest streams asked for, no ranking
        PolecatDatabase.AggregateStreamStatsSql(events, thresholdCount: 0, filterByTenant: false,
            includeArchived: true, rankLongest: false).ShouldNotContain("ROW_NUMBER()");
    }
}
