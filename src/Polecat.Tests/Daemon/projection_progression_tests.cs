using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon.Progress;
using Polecat.Exceptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

[Collection("integration")]
public class projection_progression_tests : IntegrationContext
{
    public projection_progression_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        // Clean progression table for each test
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM [dbo].[pc_event_progression];";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task insert_progression()
    {
        var shardName = new ShardName("TestProjection");
        var range = CreateRange(shardName, 0, 12);

        var op = new InsertProjectionProgress(theStore.Database.Events, range);
        await ExecuteOperationAsync(op);

        var progress = await theStore.Database.ProjectionProgressFor(shardName);
        progress.ShouldBe(12);
    }

    [Fact]
    public async Task update_happy_path()
    {
        var shardName = new ShardName("UpdateTest");

        // Insert at 12
        var insertRange = CreateRange(shardName, 0, 12);
        var insertOp = new InsertProjectionProgress(theStore.Database.Events, insertRange);
        await ExecuteOperationAsync(insertOp);

        // Update from 12 to 50
        var updateRange = CreateRange(shardName, 12, 50);
        var updateOp = new UpdateProjectionProgress(theStore.Database.Events, updateRange);
        await ExecuteOperationAsync(updateOp);

        var progress = await theStore.Database.ProjectionProgressFor(shardName);
        progress.ShouldBe(50);
    }

    [Fact]
    public async Task update_wrong_floor_throws()
    {
        var shardName = new ShardName("ConcurrencyTest");

        // Insert at 12
        var insertRange = CreateRange(shardName, 0, 12);
        var insertOp = new InsertProjectionProgress(theStore.Database.Events, insertRange);
        await ExecuteOperationAsync(insertOp);

        // Update with wrong floor (5 instead of 12)
        var updateRange = CreateRange(shardName, 5, 50);
        var updateOp = new UpdateProjectionProgress(theStore.Database.Events, updateRange);

        await Should.ThrowAsync<ProgressionProgressOutOfOrderException>(
            ExecuteOperationAsync(updateOp));
    }

    [Fact]
    public async Task fetch_all_projections()
    {
        var shard1 = new ShardName("Projection1");
        var shard2 = new ShardName("Projection2");
        var shard3 = new ShardName("Projection3");

        await ExecuteOperationAsync(
            new InsertProjectionProgress(theStore.Database.Events, CreateRange(shard1, 0, 10)));
        await ExecuteOperationAsync(
            new InsertProjectionProgress(theStore.Database.Events, CreateRange(shard2, 0, 20)));
        await ExecuteOperationAsync(
            new InsertProjectionProgress(theStore.Database.Events, CreateRange(shard3, 0, 30)));

        var all = await theStore.Database.AllProjectionProgress();

        all.Count.ShouldBeGreaterThanOrEqualTo(3);
        all.ShouldContain(s => s.ShardName == shard1.Identity && s.Sequence == 10);
        all.ShouldContain(s => s.ShardName == shard2.Identity && s.Sequence == 20);
        all.ShouldContain(s => s.ShardName == shard3.Identity && s.Sequence == 30);
    }

    [Fact]
    public async Task nonexistent_shard_returns_zero()
    {
        var shardName = new ShardName("DoesNotExist");
        var progress = await theStore.Database.ProjectionProgressFor(shardName);
        progress.ShouldBe(0);
    }

    private static EventRange CreateRange(ShardName shardName, long floor, long ceiling)
    {
        return new EventRange(shardName, floor, ceiling, null!);
    }

    private async Task ExecuteOperationAsync(Internal.IStorageOperation op)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        op.ConfigureCommand(cmd);
        await op.PostprocessAsync(cmd, CancellationToken.None);
    }
}
