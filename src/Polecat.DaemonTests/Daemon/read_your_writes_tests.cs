using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon.Coordination;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     marten#6004: read-your-writes. <c>session.LastAppendedPosition</c> says where a commit's events are, and
///     <c>store.WaitForProjectionsAsync</c> waits for the named projections to process that position, and nothing
///     past it, by polling <c>pc_event_progression</c>.
/// </summary>
public class read_your_writes_tests : OneOffConfigurationsContext
{
    // OneOffConfigurationsContext drops the schema named after the class before every test
    private const string Schema = nameof(read_your_writes_tests);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<DocumentStore> createStoreAsync()
    {
        ConfigureStore(opts => opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        return theStore;
    }

    private DocumentStore otherNode()
    {
        // A second store over the same schema runs no daemon, which is what any other node looks like
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
        });
        Disposables.Add(store);
        return store;
    }

    private static async Task<EventStorePosition> appendAsync(DocumentStore store, int count)
    {
        await using var session = store.LightweightSession();
        for (var i = 0; i < count; i++)
        {
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted($"quest-{i}"));
        }

        await session.SaveChangesAsync(Ct);

        return session.LastAppendedPosition.ShouldNotBeNull();
    }

    private static async Task<long> scalarAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = new SqlCommand(sql, conn);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(Ct));
    }

    private static async Task markProgressAsync(string name, long sequence)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = new SqlCommand($"""
            MERGE [{Schema}].[pc_event_progression] AS target
            USING (SELECT @name AS name) AS source ON target.name = source.name
            WHEN MATCHED THEN UPDATE SET last_seq_id = @seq, last_updated = SYSDATETIMEOFFSET()
            WHEN NOT MATCHED THEN INSERT (name, last_seq_id, last_updated) VALUES (@name, @seq, SYSDATETIMEOFFSET());
            """, conn);
        cmd.Parameters.AddWithValue("name", name);
        cmd.Parameters.AddWithValue("seq", sequence);
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static string shardOf(DocumentStore store)
        => store.Options.Projections.AllShards().Single().Name.Identity;

    [Fact]
    public async Task the_session_records_the_highest_sequence_a_commit_appended()
    {
        var store = await createStoreAsync();

        await using var session = store.LightweightSession();
        session.LastAppendedPosition.ShouldBeNull();

        session.Events.StartStream(Guid.NewGuid(), new QuestStarted("one"), new QuestEnded("one"));
        session.Events.StartStream(Guid.NewGuid(), new QuestStarted("two"));
        await session.SaveChangesAsync(Ct);

        var first = session.LastAppendedPosition.ShouldNotBeNull();
        first.Sequence.ShouldBe(await scalarAsync($"select max(seq_id) from [{Schema}].[pc_events]"));
        first.DatabaseIdentifier.ShouldBe(store.Database.Identifier);
        first.TenantId.ShouldBe(session.TenantId);

        session.Events.StartStream(Guid.NewGuid(), new QuestStarted("three"));
        await session.SaveChangesAsync(Ct);

        session.LastAppendedPosition!.Sequence.ShouldBeGreaterThan(first.Sequence);
    }

    [Fact]
    public async Task the_old_name_still_answers_the_same_position()
    {
        var store = await createStoreAsync();

        await using var session = store.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new QuestStarted("one"));
        await session.SaveChangesAsync(Ct);

        // LastAppendedSequence was the JasperFx 3.5.0 name, and now forwards to LastAppendedPosition
#pragma warning disable CS0618
        session.LastAppendedSequence.ShouldBe(session.LastAppendedPosition.ShouldNotBeNull());
#pragma warning restore CS0618
    }

    [Fact]
    public async Task reached_once_the_projection_passes_the_position_seen_from_another_node()
    {
        var store = await createStoreAsync();
        var position = await appendAsync(store, 3);

        using var daemon = await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        var result = await otherNode().WaitForProjectionsAsync(position, [typeof(QuestParty)],
            TimeSpan.FromSeconds(30), Ct);

        result.Reached.ShouldBeTrue(result.ToString());
        await daemon.StopAllAsync();

        await using var query = store.QuerySession();
        (await query.Query<QuestParty>().CountAsync(Ct)).ShouldBe(3);
    }

    [Fact]
    public async Task a_busy_store_does_not_hold_the_caller()
    {
        var store = await createStoreAsync();
        var position = await appendAsync(store, 2);

        // Another writer commits after the caller, and the projection has only got as far as the caller
        await appendAsync(store, 20);
        await markProgressAsync(shardOf(store), position.Sequence);

        var result = await store.WaitForProjectionsAsync(position, [typeof(QuestParty)], TimeSpan.FromSeconds(5), Ct);

        result.Reached.ShouldBeTrue(result.ToString());

        // The store-wide wait would still be waiting for the other writer's events
        await Should.ThrowAsync<TimeoutException>(() =>
            store.Database.WaitForNonStaleProjectionDataAsync(TimeSpan.FromMilliseconds(500)));
    }

    [Fact]
    public async Task a_timeout_is_a_result_with_how_far_each_shard_got()
    {
        var store = await createStoreAsync();
        var position = await appendAsync(store, 4);

        var nothing = await store.WaitForProjectionsAsync(position, [typeof(QuestParty)],
            TimeSpan.FromMilliseconds(300), Ct);
        nothing.TimedOut.ShouldBeTrue();
        nothing.Shards.Single().Sequence.ShouldBeNull();

        await markProgressAsync(shardOf(store), position.Sequence - 1);

        var behind = await store.WaitForProjectionsAsync(position, [shardOf(store).Split(':')[0]],
            TimeSpan.FromMilliseconds(300), Ct);
        behind.TimedOut.ShouldBeTrue();
        behind.Shards.Single().Sequence.ShouldBe(position.Sequence - 1);
    }
}

/// <summary>
///     marten#6004 under <c>UseTenantPartitionedEvents</c>: each tenant numbers its own events, so the position is
///     (tenant, sequence) and the wait reads that tenant's progression row.
/// </summary>
[Collection("tenant-partitioning")]
public class read_your_writes_per_tenant_tests : IAsyncLifetime
{
    private const string Schema = "pt_ryw6004";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async Task<DocumentStore> createStoreAsync()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Async);
            opts.DaemonSettings.AsyncMode = DaemonMode.HotCold;
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return store;
    }

    private static async Task<EventStorePosition> appendAsync(DocumentStore store, string tenant, int count)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        for (var i = 0; i < count; i++)
        {
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted($"{tenant}-{i}"));
        }

        await session.SaveChangesAsync(Ct);
        return session.LastAppendedPosition.ShouldNotBeNull();
    }

    [Fact]
    public async Task each_tenant_is_waited_on_by_its_own_row()
    {
        using var store = await createStoreAsync();
        var red = await appendAsync(store, "Red", 6);
        var blue = await appendAsync(store, "Blue", 2);

        red.TenantId.ShouldBe("Red");
        blue.TenantId.ShouldBe("Blue");

        // No daemon yet, so Red's row being well past Blue's sequence must not count for Blue
        var shard = store.Options.Projections.AllShards().Single().Name;
        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(Ct);
            await using var cmd = new SqlCommand(
                $"insert into [{Schema}].[pc_event_progression] (name, last_seq_id, last_updated) values (@name, @seq, SYSDATETIMEOFFSET())",
                conn);
            cmd.Parameters.AddWithValue("name", shard.ForTenant("Red").Identity);
            cmd.Parameters.AddWithValue("seq", red.Sequence);
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        red.Sequence.ShouldBeGreaterThan(blue.Sequence);

        (await store.WaitForProjectionsAsync(red, [typeof(QuestParty)], TimeSpan.FromSeconds(5), Ct)).Reached
            .ShouldBeTrue();

        var forBlue = await store.WaitForProjectionsAsync(blue, [typeof(QuestParty)], TimeSpan.FromMilliseconds(300), Ct);
        forBlue.TimedOut.ShouldBeTrue(forBlue.ToString());
        forBlue.Shards.Single().Shard.Identity.ShouldBe(shard.ForTenant("Blue").Identity);
    }

    [Fact]
    public async Task reached_for_each_tenant_once_the_daemon_runs()
    {
        using var store = await createStoreAsync();
        var red = await appendAsync(store, "Red", 6);
        var blue = await appendAsync(store, "Blue", 2);

        var coordinator = new ProjectionCoordinator(store, NullLoggerFactory.Instance);
        await coordinator.StartAsync(Ct);
        try
        {
            var forRed = await store.WaitForProjectionsAsync(red, [typeof(QuestParty)], TimeSpan.FromSeconds(30), Ct);
            forRed.Reached.ShouldBeTrue(forRed.ToString());

            var forBlue = await store.WaitForProjectionsAsync(blue, [typeof(QuestParty)], TimeSpan.FromSeconds(30), Ct);
            forBlue.Reached.ShouldBeTrue(forBlue.ToString());
        }
        finally
        {
            await coordinator.StopAsync(CancellationToken.None);
        }

        await using var query = store.QuerySession("Blue");
        (await query.Query<QuestParty>().CountAsync(Ct)).ShouldBe(2);
    }
}
