using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Events.Archiving;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events.Rewriting;

public record ParcelSentV1(string To);

public record ParcelSent(string Recipient);

public record CrateFilled;

public record CrateEmptied;

public partial class RewrittenCrate
{
    public Guid Id { get; set; }
    public int Filled { get; set; }
    public int Emptied { get; set; }

    public void Apply(CrateFilled _) => Filled++;

    public void Apply(CrateEmptied _) => Emptied++;
}

/// <summary>
///     #1034 (marten#5643): every row of a store reads back as it was, archived history included, and a compacted
///     stream still folds to the same aggregate.
/// </summary>
public class rewriting_archived_and_compacted_rows : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<long> countAsync(string where)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {theStore.Options.EventGraph.EventsTableName} WHERE {where}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(Ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task every_row_of_the_store_reads_back_as_it_was_and_archived_history_is_rewritten_too(
        bool archivedPartitioning)
    {
        ConfigureStore(opts =>
        {
            opts.EventGraph.UseArchivedStreamPartitioning = archivedPartitioning;
            opts.Events.Upcast<ParcelSentV1, ParcelSent>(x => new ParcelSent(x.To));
            opts.Events.AddEventType<CrateFilled>();
            opts.Events.AddEventType<CrateEmptied>();
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // An archived stream that still has rows an upcaster reads
        var archived = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(archived, new ParcelSentV1("Utrecht"));
            await session.SaveChangesAsync(Ct);
            session.Events.ArchiveStream(archived);
            await session.SaveChangesAsync(Ct);
        }

        // A compacted stream
        var compacted = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<RewrittenCrate>(compacted, new CrateFilled(), new CrateEmptied(),
                new CrateFilled());
            await session.SaveChangesAsync(Ct);
            await session.Events.CompactStreamAsync<RewrittenCrate>(compacted);
            await session.SaveChangesAsync(Ct);
        }

        (await countAsync("is_archived = 1")).ShouldBeGreaterThan(0);

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.IncludeAllTypes();
            x.BatchSize = 2;
        }, Ct);

        var database = result.Databases.Single();
        database.UnknownTypes.ShouldBe(0);
        database.Skipped.ShouldBe(0);
        database.Rewritten.ShouldBe(1); // the archived ParcelSentV1, nothing else
        database.Visited.ShouldBe(await countAsync("1 = 1"));

        var sourceAlias = theStore.Options.EventGraph.EventMappingFor(typeof(ParcelSentV1)).EventTypeName;
        (await countAsync($"type = '{sourceAlias}'")).ShouldBe(0);

        await using var query = theStore.QuerySession();
        var archivedEvents = await query.Events.QueryAllRawEvents()
            .Where(x => x.MaybeArchived() && x.StreamId == archived)
            .ToListAsync(Ct);
        archivedEvents.Select(x => x.Data).OfType<ParcelSent>().Single().ShouldBe(new ParcelSent("Utrecht"));

        var crate = await query.Events.AggregateStreamAsync<RewrittenCrate>(compacted, token: Ct);
        crate!.Filled.ShouldBe(2);
        crate.Emptied.ShouldBe(1);

        // A second run finds nothing to do
        var again = await theStore.Advanced.RewriteStoredEventsAsync(x => x.IncludeAllTypes(), Ct);
        again.Databases.Single().Rewritten.ShouldBe(0);
    }
}
