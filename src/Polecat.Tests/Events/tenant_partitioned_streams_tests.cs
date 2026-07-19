using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     Covers #335 scope 2 — pc_streams is partitioned alongside pc_events under
///     UseTenantPartitionedEvents (Marten parity: mt_streams rides mt_events' tenant partitioning):
///     schema shape, physical partition placement of stream rows, and the stream-version update path
///     against the partitioned table.
/// </summary>
[Collection("tenant-partitioning")]
public class tenant_partitioned_streams_tests : IAsyncLifetime
{
    private const string Schema = "pt_streams";

    public async Task InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
        });
    }

    [Fact]
    public async Task streams_table_is_partitioned_alongside_events()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // pc_streams carries the tenant_ordinal partition column and sits on its own
        // partition scheme, exactly like pc_events.
        (await TestSchema.ColumnExistsAsync(Schema, "pc_streams", "tenant_ordinal")).ShouldBeTrue();
        (await TableIsOnPartitionSchemeAsync("pc_streams", "ps_pc_streams_tenant_ordinal")).ShouldBeTrue();
        (await TableIsOnPartitionSchemeAsync("pc_events", "ps_pc_events_tenant_ordinal")).ShouldBeTrue();
    }

    [Fact]
    public async Task stream_rows_land_in_their_tenant_partition()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Events.StartStream(Guid.NewGuid(), new QuestStarted("Red"), new MonsterSlain("a", 1));
            await red.SaveChangesAsync();
        }

        await using (var blue = store.LightweightSession(new SessionOptions { TenantId = "Blue" }))
        {
            blue.Events.StartStream(Guid.NewGuid(), new QuestStarted("Blue"));
            await blue.SaveChangesAsync();
        }

        // Stream rows carry their tenant's registry ordinal and land in distinct physical
        // partitions of pc_streams.
        var rows = await TestSchema.QueryAsync($"""
            SELECT $PARTITION.pf_pc_streams_tenant_ordinal(st.tenant_ordinal) AS p, st.tenant_id,
                   st.tenant_ordinal, tp.ordinal
            FROM [{Schema}].[pc_streams] st
            JOIN [{Schema}].[pc_tenant_partitions] tp ON tp.tenant_id = st.tenant_id
            ORDER BY st.tenant_id
            """);

        rows.Count.ShouldBe(2);
        rows.Select(r => r[0]).Distinct().Count().ShouldBe(2); // distinct physical partitions
        foreach (var row in rows)
        {
            ((int)row[2]).ShouldBe((int)row[3]); // row ordinal == the tenant's registry ordinal
        }
    }

    [Fact]
    public async Task appending_to_an_existing_stream_updates_the_partitioned_stream_row()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var stream = Guid.NewGuid();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            session.Events.StartStream(stream, new QuestStarted("Quest"));
            await session.SaveChangesAsync();
        }

        // Second append hits the UPDATE path against the partitioned pc_streams (with the
        // tenant_ordinal partition-elimination predicate).
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            session.Events.Append(stream, new MembersJoined(1, "Town", ["Hero"]), new MonsterSlain("b", 2));
            await session.SaveChangesAsync();
        }

        await using var query = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var state = await query.Events.FetchStreamStateAsync(stream);
        state.ShouldNotBeNull();
        state.Version.ShouldBe(3);

        (await query.Events.FetchStreamAsync(stream)).Count.ShouldBe(3);
    }

    private static async Task<bool> TableIsOnPartitionSchemeAsync(string table, string scheme)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COUNT(*)
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
            JOIN sys.data_spaces ds ON i.data_space_id = ds.data_space_id
            WHERE s.name = @schema AND t.name = @table AND ds.name = @scheme
            """;
        cmd.Parameters.AddWithValue("@schema", Schema);
        cmd.Parameters.AddWithValue("@table", table);
        cmd.Parameters.AddWithValue("@scheme", scheme);
        var count = (int)(await cmd.ExecuteScalarAsync())!;
        return count == 1;
    }
}
