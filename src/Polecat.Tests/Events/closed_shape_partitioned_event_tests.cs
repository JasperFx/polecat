using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     #273 event-dialect (closes #309): per-tenant event partitioning (UseTenantPartitionedEvents)
///     on the closed-shape append path. Mirrors <see cref="per_tenant_event_sequence_tests" />'s
///     isolation guarantee but through the closed-shape operation, which draws seq_id from each
///     tenant's own sequence via NEXT VALUE FOR and stamps the tenant_ordinal column.
/// </summary>
[Collection("tenant-partitioning")]
public class closed_shape_partitioned_event_tests : IAsyncLifetime
{
    private const string Schema = "closed_pt";

    public async Task InitializeAsync()
    {
        await DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await DropSequencesAsync(Schema);
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
    public async Task per_tenant_sequences_are_isolated_and_round_trip()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var redStream = Guid.NewGuid();
        var blueStream = Guid.NewGuid();

        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Events.StartStream(redStream,
                new QuestStarted("Red Quest"),
                new MembersJoined(1, "Red Town", ["RedHero"]));
            await red.SaveChangesAsync();
        }

        await using (var blue = store.LightweightSession(new SessionOptions { TenantId = "Blue" }))
        {
            blue.Events.StartStream(blueStream,
                new QuestStarted("Blue Quest"),
                new MembersJoined(1, "Blue Town", ["BlueHero"]),
                new MonsterSlain("Grendel", 10));
            await blue.SaveChangesAsync();
        }

        // Distinct ordinals per tenant.
        var partitionIds = await QueryAsync(
            $"SELECT tenant_id, ordinal FROM [{Schema}].[pc_tenant_partitions] ORDER BY tenant_id");
        partitionIds.Count.ShouldBe(2);
        partitionIds.Select(r => r[1]).Distinct().Count().ShouldBe(2);

        // Each tenant's seq_id comes from its own sequence, starting at 1 — overlapping across tenants.
        (await SeqIdsForTenantAsync("Red")).ShouldBe(new long[] { 1, 2 });
        (await SeqIdsForTenantAsync("Blue")).ShouldBe(new long[] { 1, 2, 3 });

        // Round-trip is tenant-scoped.
        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        (await redQuery.Events.FetchStreamAsync(redStream)).Count.ShouldBe(2);
        (await redQuery.Events.FetchStreamAsync(blueStream)).Count.ShouldBe(0);

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        (await blueQuery.Events.FetchStreamAsync(blueStream)).Count.ShouldBe(3);
    }

    [Fact]
    public async Task append_to_existing_partitioned_stream_continues_versions_and_sequence()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = Guid.NewGuid();

        await using (var s1 = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            s1.Events.StartStream(streamId, new QuestStarted("Red Quest"));
            await s1.SaveChangesAsync();
        }

        await using (var s2 = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            s2.Events.Append(streamId, new MembersJoined(1, "Town", ["X"]), new MonsterSlain("Orc", 1));
            await s2.SaveChangesAsync();
        }

        await using var query = store.QuerySession(new SessionOptions { TenantId = "Red" });
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(3);
        events.Select(e => e.Version).ShouldBe([1, 2, 3]);

        (await SeqIdsForTenantAsync("Red")).ShouldBe(new long[] { 1, 2, 3 });
    }

    // ---- helpers (mirrors per_tenant_event_sequence_tests) ----

    private static async Task<long[]> SeqIdsForTenantAsync(string tenantId)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT seq_id FROM [{Schema}].[pc_events] WHERE tenant_id = @t ORDER BY seq_id";
        cmd.Parameters.AddWithValue("@t", tenantId);
        var list = new List<long>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) list.Add(reader.GetInt64(0));
        return list.ToArray();
    }

    private static async Task<List<object[]>> QueryAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var rows = new List<object[]>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(values);
        }

        return rows;
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;

            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task DropSequencesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'DROP SEQUENCE [' + s.name + '].[' + sq.name + '];'
            FROM sys.sequences sq
            JOIN sys.schemas s ON sq.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }
}
