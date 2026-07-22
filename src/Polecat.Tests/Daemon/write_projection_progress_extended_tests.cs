using System;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal.Operations;
using Polecat.Tests.Harness;
using Weasel.SqlServer;

namespace Polecat.Tests.Daemon;

/// <summary>
///     marten#5001 / CritterWatch#750: PolecatDatabase.WriteExtendedProgressionAsync persists the async
///     daemon's extended telemetry (heartbeat / agent_status / pause_reason / running_on_node) that the
///     shared ExtendedProgressionWriter drives. Mirrors Marten's telemetry-only
///     mt_mark_event_progression_extended: it decorates an EXISTING progression row's extended columns and
///     never touches last_seq_id (no "clobber") and never INSERTs.
/// </summary>
public class write_projection_progress_extended_tests : OneOffConfigurationsContext
{
    [Fact]
    public async Task writes_extended_telemetry_including_running_on_node()
    {
        ConfigureStore(opts => opts.Events.EnableExtendedProgressionTracking = true);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var shardName = new ShardName("ExtendedWrite");
        // Establish the progression row + last_seq_id (owned by the batch-commit path).
        await RecordProgressAsync(shardName, ceiling: 42, upsert: true, extended: true);

        // A heartbeat/transition publication carrying a DIFFERENT sequence than the committed row.
        var state = new ShardState(shardName.Identity, 999)
        {
            AgentStatus = "Running",
            LastHeartbeat = DateTimeOffset.UtcNow,
            RunningOnNode = 5
        };

        await theDatabase.WriteExtendedProgressionAsync(state, CancellationToken.None);

        var progress = await theDatabase.AllProjectionProgress();
        var row = progress.Single(x => x.ShardName == shardName.Identity);

        row.AgentStatus.ShouldBe("Running");
        row.LastHeartbeat.ShouldNotBeNull();
        row.RunningOnNode.ShouldBe(5);

        // Telemetry-only: last_seq_id stays the committed 42, NOT the ShardState's 999.
        row.Sequence.ShouldBe(42);
    }

    [Fact]
    public async Task is_a_no_op_when_no_progression_row_exists_yet()
    {
        ConfigureStore(opts => opts.Events.EnableExtendedProgressionTracking = true);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var shardName = new ShardName("NeverCommitted");
        var state = new ShardState(shardName.Identity, 7) { AgentStatus = "Running", RunningOnNode = 3 };

        // No row exists yet — the zero-row UPDATE must not throw and must not create a row.
        await theDatabase.WriteExtendedProgressionAsync(state, CancellationToken.None);

        var progress = await theDatabase.AllProjectionProgress();
        progress.ShouldNotContain(x => x.ShardName == shardName.Identity);
    }

    private async Task RecordProgressAsync(ShardName shardName, long ceiling, bool upsert, bool extended)
    {
        var op = new RecordProgressionOperation(
            theDatabase.Events.ProgressionTableName,
            shardName.Identity,
            ceiling,
            extended,
            upsert);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var batch = new SqlBatch(conn);
        var builder = new BatchBuilder(batch);
        op.ConfigureCommand(builder);
        builder.Compile();
        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
        await op.PostprocessAsync(reader, new List<Exception>(), CancellationToken.None);
    }
}
