using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon.HighWater;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #786 / jasperfx#1031: the store-agnostic <see cref="IEventStore.PreviewHighWaterAdvanceAsync" /> and
///     <see cref="IEventStore.AdvanceHighWaterMarkToLatestAsync" />, reached through <see cref="IEventStore" />
///     the way CritterWatch reaches them.
/// </summary>
public class high_water_advance_contract_tests : OneOffConfigurationsContext
{
    private const string Schema = "hw_advance_contract";

    private CancellationToken Token => TestContext.Current.CancellationToken;

    public override async ValueTask InitializeAsync()
    {
        ConfigureStore(opts => opts.DatabaseSchemaName = Schema);
        await base.InitializeAsync();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        await executeAsync($"DELETE FROM [{Schema}].[pc_events]; DELETE FROM [{Schema}].[pc_streams]; DELETE FROM [{Schema}].[pc_event_progression];");
    }

    private IEventStore store => theStore;

    private async Task<IEventDatabase> databaseAsync() => (await store.AllDatabases()).Single();

    /// <summary>
    ///     Three appends with the middle one removed, so one sequence number between them has no committed
    ///     event, and the stored mark set to the first append. Anchoring the mark matters: the IDENTITY does
    ///     not restart when rows are deleted, so a mark of zero would also count every earlier test's numbers.
    /// </summary>
    private async Task<(long Floor, long Max)> appendWithAHoleAsync()
    {
        var before = await startStreamAsync("Before");
        var hole = await startStreamAsync("Rolled back");
        await startStreamAsync("After");
        await executeAsync($"DELETE FROM [{Schema}].[pc_events] WHERE stream_id = '{hole}';");

        var floor = await seqIdOfAsync(before);
        await executeAsync($"""
            INSERT INTO [{Schema}].[pc_event_progression] (name, last_seq_id, last_updated)
            VALUES ('HighWaterMark', {floor}, SYSDATETIMEOFFSET());
            """);

        return (floor, await maxSeqIdAsync());
    }

    [Fact]
    public async Task the_preview_reports_the_mark_the_ceiling_and_the_hole_and_writes_nothing()
    {
        var (floor, max) = await appendWithAHoleAsync();

        var preview = (await store.PreviewHighWaterAdvanceAsync(await databaseAsync(), null, Token)).Single();

        preview.DatabaseIdentifier.ShouldBe(theDatabase.Identifier);
        preview.TenantId.ShouldBeNull();
        preview.CurrentMark.ShouldBe(floor);
        preview.CommittedCeiling.ShouldBe(max);
        preview.MissingSequenceCount.ShouldBe(1);
        preview.WouldAdvance.ShouldBeTrue();

        (await readMarkAsync("HighWaterMark")).ShouldBe(floor);
    }

    [Fact]
    public async Task an_advance_against_the_previewed_mark_moves_it_and_counts_what_it_skipped()
    {
        var (floor, max) = await appendWithAHoleAsync();
        var database = await databaseAsync();
        var preview = (await store.PreviewHighWaterAdvanceAsync(database, null, Token)).Single();

        var advance = (await store.AdvanceHighWaterMarkToLatestAsync(database, null, preview.CurrentMark, Token)).Single();

        advance.Outcome.ShouldBe(HighWaterAdvanceOutcome.Advanced);
        advance.PreviousMark.ShouldBe(floor);
        advance.NewMark.ShouldBe(max);
        advance.SkippedSequenceCount.ShouldBe(1);
        (await readMarkAsync("HighWaterMark")).ShouldBe(max);
    }

    [Fact]
    public async Task a_stale_expectation_is_refused_and_writes_nothing()
    {
        var (floor, max) = await appendWithAHoleAsync();
        var database = await databaseAsync();

        // The operator looked at an older mark; the daemon has since moved it on by itself
        var advance = (await store.AdvanceHighWaterMarkToLatestAsync(database, null, floor - 1, Token)).Single();

        advance.Outcome.ShouldBe(HighWaterAdvanceOutcome.RefusedStaleExpectation);
        advance.PreviousMark.ShouldBe(floor);
        advance.NewMark.ShouldBe(floor);
        advance.SkippedSequenceCount.ShouldBe(0);
        (await readMarkAsync("HighWaterMark")).ShouldBe(floor);
        max.ShouldBeGreaterThan(floor);
    }

    [Fact]
    public async Task a_mark_at_the_ceiling_is_left_alone_and_never_moves_backwards()
    {
        var (_, max) = await appendWithAHoleAsync();
        await executeAsync($"UPDATE [{Schema}].[pc_event_progression] SET last_seq_id = {max + 10} WHERE name = 'HighWaterMark';");

        var advance = (await store.AdvanceHighWaterMarkToLatestAsync(await databaseAsync(), null, null, Token)).Single();

        advance.Outcome.ShouldBe(HighWaterAdvanceOutcome.AlreadyAtCeiling);
        advance.NewMark.ShouldBe(max + 10);
        (await readMarkAsync("HighWaterMark")).ShouldBe(max + 10);
    }

    [Fact]
    public async Task a_named_tenant_is_refused_where_the_database_keeps_one_mark()
    {
        var (floor, _) = await appendWithAHoleAsync();
        var database = await databaseAsync();

        await Should.ThrowAsync<ArgumentException>(
            () => store.PreviewHighWaterAdvanceAsync(database, "red", Token));
        await Should.ThrowAsync<ArgumentException>(
            () => store.AdvanceHighWaterMarkToLatestAsync(database, "red", null, Token));

        (await readMarkAsync("HighWaterMark")).ShouldBe(floor);
    }

    private async Task<Guid> startStreamAsync(string name)
    {
        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(id, new QuestStarted(name));
        await session.SaveChangesAsync(Token);
        return id;
    }

    private async Task<long> seqIdOfAsync(Guid streamId)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT MAX(seq_id) FROM [{Schema}].[pc_events] WHERE stream_id = @id;";
        cmd.Parameters.AddWithValue("@id", streamId);
        return (long)(await cmd.ExecuteScalarAsync(Token))!;
    }

    private async Task<long> maxSeqIdAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT MAX(seq_id) FROM [{Schema}].[pc_events];";
        return (long)(await cmd.ExecuteScalarAsync(Token))!;
    }

    private async Task<long?> readMarkAsync(string name)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT last_seq_id FROM [{Schema}].[pc_event_progression] WHERE name = @name;";
        cmd.Parameters.AddWithValue("@name", name);
        return await cmd.ExecuteScalarAsync(Token) as long?;
    }

    private async Task executeAsync(string sql)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Token);
    }
}

/// <summary>
///     #786 / jasperfx#1031 under per-tenant partitioned events, where the database keeps one mark per tenant.
/// </summary>
[Collection("tenant-partitioning")]
public class high_water_advance_contract_partitioned_tests : IAsyncLifetime
{
    private const string Schema = "pt_hw_contract";

    private static CancellationToken Token => TestContext.Current.CancellationToken;

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
    public async Task a_null_tenant_previews_and_advances_every_tenants_own_mark()
    {
        using var store = createStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);
        await appendAsync(store, "Blue", 5);
        await appendAsync(store, "Red", 3);

        IEventStore eventStore = store;
        var database = (await eventStore.AllDatabases()).Single();

        var previews = await eventStore.PreviewHighWaterAdvanceAsync(database, null, Token);
        previews.Select(x => x.TenantId).ShouldBe(["Blue", "Red"]);
        previews.Single(x => x.TenantId == "Red").CommittedCeiling.ShouldBe(await maxSeqIdAsync("Red"));

        var advances = await eventStore.AdvanceHighWaterMarkToLatestAsync(database, null, null, Token);
        advances.ShouldAllBe(x => x.Outcome == HighWaterAdvanceOutcome.Advanced);

        (await readMarkAsync("HighWaterMark:Blue")).ShouldBe(await maxSeqIdAsync("Blue"));
        (await readMarkAsync("HighWaterMark:Red")).ShouldBe(await maxSeqIdAsync("Red"));
        (await readMarkAsync("HighWaterMark")).ShouldBeNull();
    }

    [Fact]
    public async Task an_expectation_needs_a_named_tenant_and_then_guards_only_that_one()
    {
        using var store = createStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);
        await appendAsync(store, "Blue", 5);
        await appendAsync(store, "Red", 3);

        IEventStore eventStore = store;
        var database = (await eventStore.AllDatabases()).Single();

        await Should.ThrowAsync<ArgumentException>(
            () => eventStore.AdvanceHighWaterMarkToLatestAsync(database, null, 0, Token));

        var red = (await eventStore.AdvanceHighWaterMarkToLatestAsync(database, "Red", 0, Token)).Single();
        red.TenantId.ShouldBe("Red");
        red.Outcome.ShouldBe(HighWaterAdvanceOutcome.Advanced);

        (await readMarkAsync("HighWaterMark:Red")).ShouldBe(await maxSeqIdAsync("Red"));
        (await readMarkAsync("HighWaterMark:Blue")).ShouldBeNull();
    }

    private static async Task appendAsync(DocumentStore store, string tenant, int count)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        var events = new object[count];
        events[0] = new QuestStarted($"{tenant} Quest");
        for (var i = 1; i < count; i++) events[i] = new MonsterSlain($"M{i}", i);
        session.Events.StartStream(Guid.NewGuid(), events);
        await session.SaveChangesAsync(Token);
    }

    private static async Task<long> maxSeqIdAsync(string tenant)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT MAX(seq_id) FROM [{Schema}].[pc_events] WHERE tenant_id = @t;";
        cmd.Parameters.AddWithValue("@t", tenant);
        return (long)(await cmd.ExecuteScalarAsync(Token))!;
    }

    private static async Task<long?> readMarkAsync(string name)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT last_seq_id FROM [{Schema}].[pc_event_progression] WHERE name = @name;";
        cmd.Parameters.AddWithValue("@name", name);
        return await cmd.ExecuteScalarAsync(Token) as long?;
    }

    private static async Task executeAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Token);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Token);
    }
}
