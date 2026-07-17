using JasperFx.Events.Projections;
using Polecat.Internal.Operations;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Progression writes and the read APIs over them (<c>ProjectionProgressFor</c> /
///     <c>AllProjectionProgress</c>). Rows are written through <see cref="RecordProgressionOperation" />
///     — the operation <c>PolecatProjectionBatch</c> actually uses — so these exercise the production
///     write path rather than a parallel one (polecat#323).
/// </summary>
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

        await RecordProgressAsync(shardName, ceiling: 12, upsert: true);

        var progress = await theStore.Database.ProjectionProgressFor(shardName);
        progress.ShouldBe(12);
    }

    [Fact]
    public async Task update_happy_path()
    {
        var shardName = new ShardName("UpdateTest");

        // Floor == 0, so the row may not exist yet — upsert.
        await RecordProgressAsync(shardName, ceiling: 12, upsert: true);

        // Floor > 0, so the row is already there — plain update.
        await RecordProgressAsync(shardName, ceiling: 50, upsert: false);

        var progress = await theStore.Database.ProjectionProgressFor(shardName);
        progress.ShouldBe(50);
    }

    [Fact]
    public async Task upsert_is_idempotent_for_an_existing_row()
    {
        var shardName = new ShardName("UpsertTwice");

        await RecordProgressAsync(shardName, ceiling: 12, upsert: true);
        await RecordProgressAsync(shardName, ceiling: 30, upsert: true);

        // The MERGE matches the existing row rather than inserting a duplicate.
        var progress = await theStore.Database.ProjectionProgressFor(shardName);
        progress.ShouldBe(30);

        var all = await theStore.Database.AllProjectionProgress();
        all.Count(x => x.ShardName == shardName.Identity).ShouldBe(1);
    }

    [Fact]
    public async Task fetch_all_projections()
    {
        var shard1 = new ShardName("Projection1");
        var shard2 = new ShardName("Projection2");
        var shard3 = new ShardName("Projection3");

        await RecordProgressAsync(shard1, 10, upsert: true);
        await RecordProgressAsync(shard2, 20, upsert: true);
        await RecordProgressAsync(shard3, 30, upsert: true);

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

    // #324 (jasperfx#435): the targeted single-cell read.
    [Fact]
    public async Task read_single_progression_row()
    {
        var shardName = new ShardName("ReadOne");
        await RecordProgressAsync(shardName, ceiling: 42, upsert: true);

        var row = await theStore.Database.ReadProjectionProgressAsync(shardName.Identity, null, default);

        row.ShouldNotBeNull();
        row!.ProjectionName.ShouldBe(shardName.Identity);
        row.TenantId.ShouldBeNull();
        row.Sequence.ShouldBe(42);
        // The default store has no extended progression tracking, so these columns don't exist / aren't read.
        row.AgentStatus.ShouldBeNull();
        row.LastHeartbeat.ShouldBeNull();
    }

    // A missing (projection, tenant) pair is the meaningful "not observed yet" answer: null, never 0.
    [Fact]
    public async Task read_returns_null_for_unknown_projection()
    {
        var row = await theStore.Database.ReadProjectionProgressAsync("NoSuchProjection:All", null, default);
        row.ShouldBeNull();
    }

    // A non-null tenantId composes the trailing ":{tenant}" segment onto the identity, matching the
    // ShardName grammar the daemon writes (range.ShardName.Identity).
    [Fact]
    public async Task read_composes_the_tenant_suffix()
    {
        var tenantShard = ShardName.Compose("TenantRead", "All", "Red");
        tenantShard.Identity.ShouldBe("TenantRead:All:Red");
        await RecordProgressAsync(tenantShard, ceiling: 7, upsert: true);

        var row = await theStore.Database.ReadProjectionProgressAsync("TenantRead:All", "Red", default);
        row.ShouldNotBeNull();
        row!.ProjectionName.ShouldBe("TenantRead:All");
        row.TenantId.ShouldBe("Red");
        row.Sequence.ShouldBe(7);

        // The store-global lookup (null tenant) must NOT match the tenant-suffixed row.
        var global = await theStore.Database.ReadProjectionProgressAsync("TenantRead:All", null, default);
        global.ShouldBeNull();
    }

    private async Task RecordProgressAsync(ShardName shardName, long ceiling, bool upsert)
    {
        var events = theStore.Database.Events;
        var op = new RecordProgressionOperation(
            events.ProgressionTableName,
            shardName.Identity,
            ceiling,
            events.EnableExtendedProgressionTracking,
            upsert);

        await ExecuteOperationAsync(op);
    }

    private async Task ExecuteOperationAsync(Internal.IStorageOperation op)
    {
        await using var conn = await OpenConnectionAsync();
        await using var batch = new Microsoft.Data.SqlClient.SqlBatch(conn);
        var builder = new Weasel.SqlServer.BatchBuilder(batch);
        op.ConfigureCommand(builder);
        builder.Compile();
        await using var reader = await batch.ExecuteReaderAsync(CancellationToken.None);
        await op.PostprocessAsync(reader, new List<Exception>(), CancellationToken.None);
    }
}
