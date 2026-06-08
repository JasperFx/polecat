using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

/// <summary>
///     Covers #163 Phase 1 — per-tenant event sequencing (UseTenantPartitionedEvents): config guards,
///     the pc_tenant_partitions registry + per-tenant pc_events_sequence objects, per-tenant sequence
///     isolation, append/read round-trip under the flag, and off-flag regression.
/// </summary>
[Collection("tenant-partitioning")]
public class per_tenant_event_sequence_tests : IAsyncLifetime
{
    private const string PartitionedSchema = "pt_on";
    private const string DefaultSchema = "pt_off";

    public async Task InitializeAsync()
    {
        await DropSchemaTablesAsync(PartitionedSchema);
        await DropSchemaTablesAsync(DefaultSchema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await DropSequencesAsync(PartitionedSchema);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private static DocumentStore CreatePartitionedStore(string schema = PartitionedSchema)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
        });
    }

    [Fact]
    public void requires_conjoined_tenancy()
    {
        var ex = Should.Throw<InvalidOperationException>(() => DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            // TenancyStyle left at the default (Single)
            opts.EventGraph.UseTenantPartitionedEvents = true;
        }));

        ex.Message.ShouldContain("Conjoined");
    }

    [Fact]
    public void incompatible_with_archived_stream_partitioning()
    {
        var ex = Should.Throw<InvalidOperationException>(() => DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.EventGraph.UseArchivedStreamPartitioning = true;
        }));

        ex.Message.ShouldContain("UseArchivedStreamPartitioning");
    }

    [Fact]
    public async Task off_flag_keeps_identity_and_creates_no_registry_table()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = DefaultSchema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            // UseTenantPartitionedEvents left false
        });
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // seq_id stays a global IDENTITY column...
        (await IsIdentityAsync(DefaultSchema, "pc_events", "seq_id")).ShouldBeTrue();
        // ...and no per-tenant registry table is created.
        (await TableExistsAsync(DefaultSchema, "pc_tenant_partitions")).ShouldBeFalse();
    }

    [Fact]
    public async Task enabling_drops_identity_and_creates_registry_table()
    {
        using var store = CreatePartitionedStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        (await IsIdentityAsync(PartitionedSchema, "pc_events", "seq_id")).ShouldBeFalse();
        (await TableExistsAsync(PartitionedSchema, "pc_tenant_partitions")).ShouldBeTrue();
    }

    [Fact]
    public async Task per_tenant_sequences_are_isolated()
    {
        using var store = CreatePartitionedStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var redStream = Guid.NewGuid();
        var blueStream = Guid.NewGuid();

        // Tenant Red: 2 events.
        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Events.StartStream(redStream,
                new QuestStarted("Red Quest"),
                new MembersJoined(1, "Red Town", ["RedHero"]));
            await red.SaveChangesAsync();
        }

        // Tenant Blue: 3 events.
        await using (var blue = store.LightweightSession(new SessionOptions { TenantId = "Blue" }))
        {
            blue.Events.StartStream(blueStream,
                new QuestStarted("Blue Quest"),
                new MembersJoined(1, "Blue Town", ["BlueHero"]),
                new MonsterSlain("Grendel", 10));
            await blue.SaveChangesAsync();
        }

        // The registry assigned a distinct ordinal to each tenant.
        var partitionIds = await QueryAsync(
            $"SELECT tenant_id, ordinal FROM [{PartitionedSchema}].[pc_tenant_partitions] ORDER BY tenant_id");
        partitionIds.Count.ShouldBe(2);
        partitionIds.Select(r => r[1]).Distinct().Count().ShouldBe(2);

        // Each tenant has its OWN sequence starting at 1 — so seq_ids overlap across tenants, which is
        // exactly the per-tenant sequencing guarantee (and impossible under a single global IDENTITY).
        var redSeqs = await SeqIdsForTenantAsync("Red");
        var blueSeqs = await SeqIdsForTenantAsync("Blue");

        redSeqs.ShouldBe(new long[] { 1, 2 });
        blueSeqs.ShouldBe(new long[] { 1, 2, 3 });

        // Round-trip: each tenant reads its own stream; cross-tenant is invisible.
        await using var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" });
        (await redQuery.Events.FetchStreamAsync(redStream)).Count.ShouldBe(2);
        (await redQuery.Events.FetchStreamAsync(blueStream)).Count.ShouldBe(0);

        await using var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" });
        (await blueQuery.Events.FetchStreamAsync(blueStream)).Count.ShouldBe(3);
    }

    [Fact]
    public async Task events_are_physically_partitioned_by_tenant()
    {
        using var store = CreatePartitionedStore();
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

        // Each tenant's rows carry that tenant's ordinal and physically land in a distinct partition of
        // pc_events (computed via the partition function over tenant_ordinal).
        var rows = await QueryAsync($"""
            SELECT $PARTITION.pf_pc_events_tenant_ordinal(tenant_ordinal) AS p, tenant_id, COUNT(*) AS c
            FROM [{PartitionedSchema}].[pc_events]
            GROUP BY $PARTITION.pf_pc_events_tenant_ordinal(tenant_ordinal), tenant_id
            ORDER BY tenant_id
            """);

        rows.Count.ShouldBe(2);                                   // Red + Blue
        rows.Select(r => r[0]).Distinct().Count().ShouldBe(2);    // in different physical partitions
        rows.Single(r => (string)r[1] == "Red")[2].ShouldBe(2);  // Red has 2 events
        rows.Single(r => (string)r[1] == "Blue")[2].ShouldBe(1); // Blue has 1 event
    }

    private static async Task<long[]> SeqIdsForTenantAsync(string tenantId)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT seq_id FROM [{PartitionedSchema}].[pc_events] WHERE tenant_id = @t ORDER BY seq_id";
        cmd.Parameters.AddWithValue("@t", tenantId);
        var list = new List<long>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) list.Add(reader.GetInt64(0));
        return list.ToArray();
    }

    private static async Task<bool> IsIdentityAsync(string schema, string table, string column)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT COLUMNPROPERTY(OBJECT_ID('[{schema}].[{table}]'), @col, 'IsIdentity')";
        cmd.Parameters.AddWithValue("@col", column);
        var result = await cmd.ExecuteScalarAsync();
        return result is int i && i == 1;
    }

    private static async Task<bool> TableExistsAsync(string schema, string table)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT OBJECT_ID(@name, 'U')";
        cmd.Parameters.AddWithValue("@name", $"[{schema}].[{table}]");
        var result = await cmd.ExecuteScalarAsync();
        return result is not (null or DBNull);
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
