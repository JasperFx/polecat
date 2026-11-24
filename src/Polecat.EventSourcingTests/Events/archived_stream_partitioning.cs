using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class archived_stream_partitioning : IntegrationContext
{
    public archived_stream_partitioning(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        // Start from an empty schema. Since Weasel 10.0 (weasel#920, #1140) the partition scheme and function
        // carry the schema in their names, so they are this schema's own; they are left alone all the same,
        // because Weasel reuses a scheme that is already there.
        await TestSchema.DropSchemaTablesAsync("archived_part");
        await TestSchema.DropSequencesAsync("archived_part");

        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "archived_part";
            opts.EventGraph.UseArchivedStreamPartitioning = true;
        });
    }

    [Fact]
    public async Task events_table_is_partitioned()
    {
        // Write some events to ensure schema is created
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Partitioned Quest"),
            new MembersJoined(1, "Town", ["Alpha"]));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Verify partition function exists
        var conn = await OpenConnectionAsync();

        await using var pfCmd = new SqlCommand(
            "SELECT COUNT(*) FROM sys.partition_functions WHERE name = 'pf_archived_part_pc_events_is_archived'", conn);
        var pfCount = (int)(await pfCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        pfCount.ShouldBeGreaterThan(0);

        // Verify partition scheme exists
        await using var psCmd = new SqlCommand(
            "SELECT COUNT(*) FROM sys.partition_schemes WHERE name = 'ps_archived_part_pc_events_is_archived'", conn);
        var psCount = (int)(await psCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        psCount.ShouldBeGreaterThan(0);

        // Verify the table in our schema has 2 partitions
        await using var partCmd = new SqlCommand("""
            SELECT COUNT(*)
            FROM sys.partitions p
            JOIN sys.objects o ON p.object_id = o.object_id
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = 'archived_part' AND o.name = 'pc_events' AND p.index_id <= 1
            """, conn);
        var partitionCount = (int)(await partCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        // With 1 boundary value (is_archived=1), we get 2 partitions:
        // Partition 1: is_archived < 1 (active), Partition 2: is_archived >= 1 (archived)
        partitionCount.ShouldBe(2);
    }

    [Fact]
    public async Task can_write_and_read_events_with_partitioning()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Readable Quest"),
            new MembersJoined(1, "Town", ["Beta", "Gamma"]));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        events.Count.ShouldBe(2);
        events[0].Data.ShouldBeOfType<QuestStarted>();
        events[1].Data.ShouldBeOfType<MembersJoined>();
    }

    [Fact]
    public async Task can_archive_and_query_active_events_only()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archive Quest"),
            new MembersJoined(1, "Town", ["Delta"]));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Archive the stream
        theSession.Events.ArchiveStream(streamId);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // FetchStream should return empty for archived streams (default filters archived)
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(0);

        // Aggregate should return null for archived stream
        var aggregate = await query.Events.FetchLatest<QuestAggregate>(streamId, TestContext.Current.CancellationToken);
        aggregate.ShouldBeNull();
    }
}
