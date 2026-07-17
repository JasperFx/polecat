using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal.Operations;
using Polecat.Tests.Harness;
using Weasel.SqlServer;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #324 (jasperfx#435): the extended-tracking branch of
///     <c>ReadProjectionProgressAsync</c>. Under <c>EnableExtendedProgressionTracking</c> the
///     progression table gains the <c>heartbeat</c> + <c>agent_status</c> columns; the targeted read
///     surfaces them as <c>ProjectionProgressRow.LastHeartbeat</c> / <c>.AgentStatus</c>. This confirms
///     issue 324's open question — those two fields ARE available from <c>pc_event_progression</c> (once
///     extended tracking is on), so the record does not need trimming on the JasperFx side.
/// </summary>
public class read_projection_progress_extended_tests : OneOffConfigurationsContext
{
    [Fact]
    public async Task reads_heartbeat_when_extended_tracking_is_enabled()
    {
        ConfigureStore(opts => opts.Events.EnableExtendedProgressionTracking = true);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var shardName = new ShardName("ExtendedRead");
        // RecordProgressionOperation stamps heartbeat = SYSDATETIMEOFFSET() under extended tracking.
        await RecordProgressAsync(shardName, ceiling: 99, upsert: true, extended: true);

        var row = await theDatabase.ReadProjectionProgressAsync(shardName.Identity, null, default);

        row.ShouldNotBeNull();
        row!.Sequence.ShouldBe(99);
        row.LastHeartbeat.ShouldNotBeNull();
        // agent_status is written by the daemon's heartbeat path, not by the plain progression write,
        // so it stays null here — the read must tolerate a null column, which is the point.
        row.AgentStatus.ShouldBeNull();
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
