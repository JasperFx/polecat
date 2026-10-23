using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #813 / jasperfx#809 — <c>AsyncOptions.SubscribeFromFirstRun()</c> end to end on SQL Server. The
///     compliance suite (<c>ProjectionFloorCompliance</c>) pins the storage seam; this pins what the seam
///     is FOR: the first start of a projection pins the high water as its floor, history below it is
///     never projected, and the floor outlives a rebuild's teardown, so a rebuild replays from it rather
///     than from 0.
/// </summary>
public class subscribe_from_first_run_tests : IAsyncLifetime
{
    private const string Schema = "subscribe_first_run";

    public async ValueTask InitializeAsync() => await TestSchema.DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore(bool enableFloors = true) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Events.EnableProjectionFloors = enableFloors;

        opts.Projections.Add(new SingleStreamProjection<QuestParty, Guid>(), ProjectionLifecycle.Async,
            asyncConfiguration: o => o.SubscribeFromFirstRun());
    });

    private static async Task<Guid> StartQuestAsync(DocumentStore store, string name)
    {
        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id, new QuestStarted(name));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return id;
    }

    private static async Task<QuestParty?> LoadAsync(DocumentStore store, Guid id)
    {
        await using var query = store.QuerySession();
        return await query.LoadAsync<QuestParty>(id, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task history_before_the_first_run_is_skipped_and_the_floor_survives_a_rebuild()
    {
        var token = TestContext.Current.CancellationToken;
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        var before = await StartQuestAsync(store, "before the first run");
        var highWater = await store.Database.FetchHighestEventSequenceNumber(token);

        var projectionName = store.Options.Projections.AllShards().Single().Name.Name;
        IEventDatabase database = store.Database;

        // First run: the floor is pinned at the high water the daemon saw.
        using (var daemon = await store.BuildProjectionDaemonAsync())
        {
            await daemon.StartAllAsync();
            await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), token);
        }

        (await database.FetchProjectionFloorAsync(projectionName, null, token)).ShouldBe(highWater);
        (await LoadAsync(store, before)).ShouldBeNull();

        // A later start honours the stored floor and projects everything after it.
        var after = await StartQuestAsync(store, "after the first run");
        await store.WaitForProjectionAsync();

        (await LoadAsync(store, after)).ShouldNotBeNull().Name.ShouldBe("after the first run");
        (await LoadAsync(store, before)).ShouldBeNull();

        // A rebuild tears the progression row and the documents down, but not the floor, so it
        // replays from the floor rather than from 0.
        using (var daemon = await store.BuildProjectionDaemonAsync())
        {
            await daemon.RebuildProjectionAsync(projectionName, token);
        }

        (await database.FetchProjectionFloorAsync(projectionName, null, token)).ShouldBe(highWater);
        (await LoadAsync(store, after)).ShouldNotBeNull();
        (await LoadAsync(store, before)).ShouldBeNull();
    }

    [Fact]
    public async Task a_database_without_the_floors_table_reads_as_no_floor()
    {
        // AutoCreate.None against a schema that was never migrated: the daemon reads the stored floor on
        // every shard start, so a missing table has to mean "no floor", not a failed start.
        var token = TestContext.Current.CancellationToken;
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.None;
            opts.Events.EnableProjectionFloors = true;
        });

        IEventDatabase database = store.Database;
        (await database.FetchProjectionFloorAsync("Anything", null, token)).ShouldBeNull();
    }

    /// <summary>
    ///     Opt-in in Polecat 5 (opt-out planned for 6): a store that has not asked for floors gets no
    ///     table and no schema delta from upgrading, every shard still starts (no floor), and the floor
    ///     writes refuse, naming the switch, rather than failing on a missing table.
    /// </summary>
    [Fact]
    public async Task floors_are_opt_in_no_table_no_floor_and_the_writes_name_the_switch()
    {
        var token = TestContext.Current.CancellationToken;
        using var store = CreateStore(enableFloors: false);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        await using (var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(token);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT OBJECT_ID('[{Schema}].[pc_event_projection_floors]')";
            (await cmd.ExecuteScalarAsync(token)).ShouldBe(DBNull.Value);
        }

        IEventDatabase database = store.Database;
        (await database.FetchProjectionFloorAsync("QuestParty", null, token)).ShouldBeNull();

        var trySet = await Should.ThrowAsync<NotSupportedException>(
            () => database.TrySetProjectionFloorAsync("QuestParty", null, 10, token));
        trySet.Message.ShouldContain(nameof(EventStoreOptions.EnableProjectionFloors));

        await Should.ThrowAsync<NotSupportedException>(
            () => database.SetProjectionFloorAsync("QuestParty", null, 10, token));
    }
}
