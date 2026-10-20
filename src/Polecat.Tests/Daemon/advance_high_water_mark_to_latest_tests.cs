using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #786: <c>Advanced.AdvanceHighWaterMarkToLatestAsync</c>, parity with Marten and Fisher, for the
///     operator action that jump-starts a daemon whose mark is held behind a gap that will never fill.
/// </summary>
public class advance_high_water_mark_to_latest_tests : OneOffConfigurationsContext
{
    private const string Schema = "advance_hwm";

    public override async ValueTask InitializeAsync()
    {
        ConfigureStore(opts => opts.DatabaseSchemaName = Schema);
        await base.InitializeAsync();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        await executeAsync($"DELETE FROM [{Schema}].[pc_events]; DELETE FROM [{Schema}].[pc_streams]; DELETE FROM [{Schema}].[pc_event_progression];");
    }

    [Fact]
    public async Task advances_past_a_gap_to_the_highest_committed_sequence()
    {
        var ct = TestContext.Current.CancellationToken;
        await startStreamAsync("Before");
        var hole = await startStreamAsync("Rolled back");
        await startStreamAsync("After");

        // A hole between two committed appends, which is what a rolled-back append leaves behind and
        // what the detector holds the mark under until it goes stale
        await executeAsync($"DELETE FROM [{Schema}].[pc_events] WHERE stream_id = '{hole}';");

        await theStore.Advanced.AdvanceHighWaterMarkToLatestAsync(ct);

        (await readMarkAsync("HighWaterMark")).ShouldBe(await maxSeqIdAsync());
    }

    [Fact]
    public async Task never_moves_the_mark_backwards()
    {
        var ct = TestContext.Current.CancellationToken;
        await startStreamAsync("One");
        var max = await maxSeqIdAsync();

        await executeAsync($"""
            INSERT INTO [{Schema}].[pc_event_progression] (name, last_seq_id, last_updated)
            VALUES ('HighWaterMark', {max + 100}, SYSDATETIMEOFFSET());
            """);

        await theStore.Advanced.AdvanceHighWaterMarkToLatestAsync(ct);

        (await readMarkAsync("HighWaterMark")).ShouldBe(max + 100);
    }

    [Fact]
    public async Task reports_what_it_did_and_is_idempotent()
    {
        var ct = TestContext.Current.CancellationToken;
        await startStreamAsync("One");
        await startStreamAsync("Two");
        var max = await maxSeqIdAsync();

        var first = await theStore.Advanced.advanceHighWaterMarkAsync(theDatabase, null, ct);
        first.Single().PreviousMark.ShouldBe(0);
        first.Single().NewMark.ShouldBe(max);
        first.Single().CommittedCeiling.ShouldBe(max);

        var second = await theStore.Advanced.advanceHighWaterMarkAsync(theDatabase, null, ct);
        second.Single().PreviousMark.ShouldBe(max);
        second.Single().NewMark.ShouldBe(max);
    }

    [Fact]
    public async Task the_tenant_overload_advances_the_databases_mark_when_events_are_not_partitioned()
    {
        // Marten's and Fisher's behavior: without per-tenant sequences there is one mark per database
        await startStreamAsync("One");

        await theStore.Advanced.AdvanceHighWaterMarkToLatestAsync("anybody", TestContext.Current.CancellationToken);

        (await readMarkAsync("HighWaterMark")).ShouldBe(await maxSeqIdAsync());
    }

    [Fact]
    public async Task an_empty_store_writes_nothing()
    {
        await theStore.Advanced.AdvanceHighWaterMarkToLatestAsync(TestContext.Current.CancellationToken);

        (await readMarkAsync("HighWaterMark")).ShouldBeNull();
    }

    private async Task<Guid> startStreamAsync(string name)
    {
        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(id, new QuestStarted(name));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return id;
    }

    private async Task<long> maxSeqIdAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT MAX(seq_id) FROM [{Schema}].[pc_events];";
        return (long)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private async Task<long?> readMarkAsync(string name)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT last_seq_id FROM [{Schema}].[pc_event_progression] WHERE name = @name;";
        cmd.Parameters.AddWithValue("@name", name);
        return await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken) as long?;
    }

    private async Task executeAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}

/// <summary>
///     #786 under per-tenant partitioned events, where the daemon keeps one mark per tenant.
/// </summary>
[Collection("tenant-partitioning")]
public class advance_high_water_mark_to_latest_partitioned_tests : IAsyncLifetime
{
    private const string Schema = "pt_hw_advance";

    public async ValueTask InitializeAsync()
    {
        await executeAsync($"""
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{Schema}';
            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = '{Schema}';
            SELECT @sql = @sql + 'DROP SEQUENCE [' + s.name + '].[' + sq.name + '];'
            FROM sys.sequences sq JOIN sys.schemas s ON sq.schema_id = s.schema_id WHERE s.name = '{Schema}';
            EXEC sp_executesql @sql;
            """);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore createStore() => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
    });

    [Fact]
    public async Task advances_every_tenants_own_mark_and_leaves_the_global_row_alone()
    {
        var ct = TestContext.Current.CancellationToken;
        using var store = createStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: ct);

        await appendAsync(store, "Red", 3);
        await appendAsync(store, "Blue", 5);

        await store.Advanced.AdvanceHighWaterMarkToLatestAsync(ct);

        (await readMarkAsync("HighWaterMark:Red")).ShouldBe(await maxSeqIdAsync("Red"));
        (await readMarkAsync("HighWaterMark:Blue")).ShouldBe(await maxSeqIdAsync("Blue"));
        (await readMarkAsync("HighWaterMark")).ShouldBeNull();
    }

    [Fact]
    public async Task the_tenant_overload_advances_only_that_tenant()
    {
        var ct = TestContext.Current.CancellationToken;
        using var store = createStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: ct);

        await appendAsync(store, "Red", 3);
        await appendAsync(store, "Blue", 5);

        await store.Advanced.AdvanceHighWaterMarkToLatestAsync("Blue", ct);

        (await readMarkAsync("HighWaterMark:Blue")).ShouldBe(await maxSeqIdAsync("Blue"));
        (await readMarkAsync("HighWaterMark:Red")).ShouldBeNull();
    }

    private static async Task appendAsync(DocumentStore store, string tenant, int count)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        var events = new object[count];
        events[0] = new QuestStarted($"{tenant} Quest");
        for (var i = 1; i < count; i++) events[i] = new MonsterSlain($"M{i}", i);
        session.Events.StartStream(Guid.NewGuid(), events);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<long> maxSeqIdAsync(string tenant)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT MAX(seq_id) FROM [{Schema}].[pc_events] WHERE tenant_id = @t;";
        cmd.Parameters.AddWithValue("@t", tenant);
        return (long)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private static async Task<long?> readMarkAsync(string name)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT last_seq_id FROM [{Schema}].[pc_event_progression] WHERE name = @name;";
        cmd.Parameters.AddWithValue("@name", name);
        return await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken) as long?;
    }

    private static async Task executeAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
