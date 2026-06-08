using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Events.Daemon.Progress;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.Events;

/// <summary>
///     Covers issue #166 — Polecat's <see cref="IEventStoreInstrumentation" /> surface plus the
///     6-column extended progression tracking it toggles (schema, daemon write, shard-state read,
///     and opt-out).
/// </summary>
public class event_store_instrumentation_tests : IAsyncLifetime
{
    private const string OnSchema = "instr_on";
    private const string OffSchema = "instr_off";

    private static readonly string[] ExtendedColumns =
    [
        "heartbeat", "agent_status", "pause_reason", "running_on_node",
        "warning_behind_threshold", "critical_behind_threshold"
    ];

    public async Task InitializeAsync()
    {
        await DropSchemaTablesAsync(OnSchema);
        await DropSchemaTablesAsync(OffSchema);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private static DocumentStore CreateStore(string schema, bool extended)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.EnableExtendedProgressionTracking = extended;
        });
    }

    [Fact]
    public void events_options_implement_instrumentation_and_toggle_is_shared()
    {
        var options = new StoreOptions();

        // The .Events options surface is the IEventStoreInstrumentation, mirroring how Marten
        // exposes it via EventGraph on StoreOptions.Events.
        options.Events.ShouldBeAssignableTo<IEventStoreInstrumentation>();

        var instrumentation = (IEventStoreInstrumentation)options.Events;

        // Default is opt-out.
        instrumentation.ExtendedProgressionEnabled.ShouldBeFalse();
        options.Events.EnableExtendedProgressionTracking.ShouldBeFalse();

        // Setting through the storage-agnostic interface flips the Polecat-named flag...
        instrumentation.ExtendedProgressionEnabled = true;
        options.Events.EnableExtendedProgressionTracking.ShouldBeTrue();

        // ...and vice versa — they are one setting.
        options.Events.EnableExtendedProgressionTracking = false;
        instrumentation.ExtendedProgressionEnabled.ShouldBeFalse();
    }

    [Fact]
    public async Task enabling_adds_the_six_columns_on_schema_apply()
    {
        using var store = CreateStore(OnSchema, extended: true);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var columns = (await SchemaInspector.GetColumnInfoAsync("pc_event_progression", OnSchema))
            .Select(c => c.Name).ToList();

        foreach (var col in ExtendedColumns)
        {
            columns.ShouldContain(col);
        }
    }

    [Fact]
    public async Task opting_out_produces_no_schema_delta()
    {
        using var store = CreateStore(OffSchema, extended: false);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var columns = (await SchemaInspector.GetColumnInfoAsync("pc_event_progression", OffSchema))
            .Select(c => c.Name).ToList();

        // Base columns present...
        columns.ShouldContain("name");
        columns.ShouldContain("last_seq_id");
        columns.ShouldContain("last_updated");

        // ...but none of the extended monitoring columns.
        foreach (var col in ExtendedColumns)
        {
            columns.ShouldNotContain(col);
        }
    }

    [Fact]
    public async Task daemon_write_is_read_back_into_shard_state()
    {
        using var store = CreateStore(OnSchema, extended: true);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var heartbeat = DateTimeOffset.UtcNow;

        // Simulate the projection daemon writing its runtime agent state for a shard.
        var op = new MarkExtendedProjectionProgress(
            store.Database.Events,
            shardName: "Instrumented:All",
            sequenceCeiling: 42,
            heartbeat: heartbeat,
            agentStatus: "Running",
            pauseReason: null,
            runningOnNode: 7);

        await ExecuteAsync(op);

        // The shard-state selector reads the extended columns back into ShardState.
        var all = await store.Database.AllProjectionProgress();
        var state = all.ShouldHaveSingleItem();

        state.ShardName.ShouldBe("Instrumented:All");
        state.Sequence.ShouldBe(42);
        state.AgentStatus.ShouldBe("Running");
        state.RunningOnNode.ShouldBe(7);
        state.LastHeartbeat.ShouldNotBeNull();
    }

    private static async Task ExecuteAsync(Internal.IStorageOperation op)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var batch = new SqlBatch(conn);
        var builder = new BatchBuilder(batch);
        op.ConfigureCommand(builder);
        builder.Compile();
        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
        await op.PostprocessAsync(reader, new List<Exception>(), CancellationToken.None);
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        // Drop every base table in the schema so each run migrates from a clean slate.
        cmd.CommandText = $"""
            DECLARE @sql nvarchar(max) = N'';
            -- Drop foreign keys first so tables can be dropped in any order.
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
}
