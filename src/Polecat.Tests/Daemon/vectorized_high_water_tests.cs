using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #163 Phase 2 — vectorized per-tenant high-water detection (the single-round-trip
///     pc_tenant_partitions → sys.sequences → pc_event_progression read). Mirrors Marten's
///     vectorized_high_water_detection / _flag_off.
/// </summary>
public class vectorized_high_water_tests : IAsyncLifetime
{
    private const string Schema = "pt_hw";

    public async Task InitializeAsync()
    {
        await DropSchemaTablesAsync(Schema);
        await DropSequencesAsync(Schema);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private static DocumentStore CreateStore(bool partitioned)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = partitioned;
        });
    }

    private static PolecatHighWaterDetector DetectorFor(DocumentStore store) =>
        new(store.Options.EventGraph, store.Options.ConnectionString, store.Options.DaemonSettings,
            NullLogger<PolecatHighWaterDetector>.Instance, store.Options.ResiliencePipeline);

    [Fact]
    public async Task vector_has_one_independent_reading_per_tenant()
    {
        using var store = CreateStore(partitioned: true);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        // Red: 2 events, Blue: 3 events, Green: never appends (flat tenant).
        await AppendAsync(store, "Red", 2);
        await AppendAsync(store, "Blue", 3);

        var detector = DetectorFor(store);
        var vector = await detector.DetectForTenantsAsync(["Red", "Blue", "Green"], CancellationToken.None);

        // Every requested tenant is present (LEFT JOINs keep the flat tenant in the vector).
        vector.TenantCount.ShouldBe(3);

        vector.TryGetStatistics("Red", out var red).ShouldBeTrue();
        red.HighestSequence.ShouldBe(2);
        red.CurrentMark.ShouldBe(2);

        vector.TryGetStatistics("Blue", out var blue).ShouldBeTrue();
        blue.HighestSequence.ShouldBe(3);

        // Green has no partition/sequence yet → bounded at zero, but still in the vector.
        vector.TryGetStatistics("Green", out var green).ShouldBeTrue();
        green.HighestSequence.ShouldBe(0);

        // Per-tenant ceilings drive bounded rebuilds.
        vector.CeilingFor("Red").ShouldBe(2);
        vector.CeilingFor("Blue").ShouldBe(3);
    }

    [Fact]
    public async Task flag_off_collapses_to_store_global()
    {
        using var store = CreateStore(partitioned: false);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var detector = DetectorFor(store);
        detector.SupportsTenantPartitioning.ShouldBeFalse();

        var vector = await detector.DetectForTenantsAsync(["Red", "Blue"], CancellationToken.None);

        // No per-tenant dimension — one store-global reading, no tenant entries.
        vector.TenantCount.ShouldBe(0);
        vector.Global.ShouldNotBeNull();
    }

    private static async Task AppendAsync(DocumentStore store, string tenant, int count)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        var events = new object[count];
        events[0] = new QuestStarted($"{tenant} Quest");
        for (var i = 1; i < count; i++) events[i] = new MonsterSlain($"M{i}", i);
        session.Events.StartStream(Guid.NewGuid(), events);
        await session.SaveChangesAsync();
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
