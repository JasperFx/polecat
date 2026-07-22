using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

// marten#5001 (Polecat parity): the async daemon's ExtendedProgressionWriter publishes a ShardState
// carrying the telemetry the agents already compute (heartbeat / agent_status / pause_reason /
// running_on_node) and drives IEventDatabase.WriteExtendedProgressionAsync to persist it onto the shard's
// existing progression row. Polecat left that method as the JasperFx no-op default, so running_on_node —
// and every other extended telemetry column — stayed NULL forever, exactly the #5001 symptom reported
// against Marten.
//
// This pins the Polecat write-path half: given a published ShardState carrying the telemetry (the running
// node is stamped onto it by the ShardStateTracker under managed distribution), WriteExtendedProgressionAsync
// must UPDATE the extended columns of the existing row, and must never touch last_seq_id / last_updated —
// that progress is owned by the projection batch commit, so a telemetry write can never roll it back.
public class Bug_5001_running_on_node_persisted : OneOffConfigurationsContext
{
    [Fact]
    public async Task write_extended_progression_persists_running_on_node_and_telemetry()
    {
        ConfigureStore(opts => opts.Events.EnableExtendedProgressionTracking = true);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var table = theDatabase.Events.ProgressionTableName;
        const string shard = "Bug5001:All";

        // The progression row is owned by the projection batch commit; pre-create it as the daemon would,
        // so the telemetry-only extended write has an existing row to decorate.
        await using (var seed = await OpenConnectionAsync())
        {
            var insert = seed.CreateCommand();
            insert.CommandText = $"INSERT INTO {table} (name, last_seq_id) VALUES (@name, 42);";
            insert.Parameters.AddWithValue("@name", shard);
            await insert.ExecuteNonQueryAsync();
        }

        // #5001: the daemon publishes a ShardState carrying the running node + telemetry and drives this.
        await ((IEventDatabase)theDatabase).WriteExtendedProgressionAsync(new ShardState(shard, 42)
        {
            AgentStatus = "Running",
            LastHeartbeat = DateTimeOffset.UtcNow,
            RunningOnNode = 7
        });

        await using var read = await OpenConnectionAsync();
        var cmd = read.CreateCommand();
        cmd.CommandText =
            $"SELECT running_on_node, agent_status, heartbeat, last_seq_id FROM {table} WHERE name = @name;";
        cmd.Parameters.AddWithValue("@name", shard);

        await using var reader = await cmd.ExecuteReaderAsync();
        (await reader.ReadAsync()).ShouldBeTrue();

        reader.IsDBNull(0).ShouldBeFalse("running_on_node must be persisted by WriteExtendedProgressionAsync");
        reader.GetInt32(0).ShouldBe(7);

        // the sibling telemetry columns write on the same call
        reader.GetString(1).ShouldBe("Running");
        reader.IsDBNull(2).ShouldBeFalse("heartbeat must be persisted");

        // a telemetry write must never roll committed progress backwards
        reader.GetInt64(3).ShouldBe(42);
    }
}
