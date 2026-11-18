using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon.HighWater;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Polecat.Events.Daemon;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #697 — the per-tenant high-water path under <c>UseTenantPartitionedEvents</c>. Three separate
///     defects lived here, and each one alone is enough to stop every async projection on such a store:
///     the height was read from the tenant's SEQUENCE rather than from its committed events, there was
///     no contiguity walk, and <c>MarkHighWaterForTenantAsync</c> was never implemented, so JasperFx's
///     coordinator persisted nothing through the interface's no-op default.
/// </summary>
[Collection("tenant-partitioning")]
public class per_tenant_high_water_persistence_tests : IAsyncLifetime
{
    private const string Schema = "pt_hw_persist";

    public async ValueTask InitializeAsync()
    {
        await DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
        });
    }

    private static PolecatHighWaterDetector DetectorFor(DocumentStore store) =>
        new(store.Options.EventGraph, store.Options.ConnectionString, store.Options.DaemonSettings,
            NullLogger<PolecatHighWaterDetector>.Instance, store.Options.ResiliencePipeline);

    /// <summary>
    ///     The writer, reached the way the daemon reaches it. Called through
    ///     <see cref="IHighWaterDetector" /> ON PURPOSE: the base declares this member with a
    ///     <c>Task.CompletedTask</c> default, so a test that called the class method directly would pass
    ///     even if the override failed to satisfy the interface and the coordinator kept hitting the
    ///     no-op. That is precisely the #697 failure, so the interface is the only honest call site.
    /// </summary>
    [Fact]
    public async Task the_coordinators_per_tenant_write_reaches_the_progression_table()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        await AppendAsync(store, "Red", 3);

        IHighWaterDetector detector = DetectorFor(store);
        var stamp = DateTimeOffset.UtcNow.AddMinutes(-7);

        await detector.MarkHighWaterForTenantAsync("Red", 3, stamp, TestContext.Current.CancellationToken);

        var (seq, updated) = await ReadProgressionAsync("HighWaterMark:Red");
        seq.ShouldBe(3);
        // jasperfx#449 — the row carries the POLL's timestamp, not the server clock at write time.
        // Implementing only the three-argument overload would still persist the sequence and silently
        // lose this, leaving per-tenant staleness unreportable.
        updated.ShouldNotBeNull();
        updated.Value.ShouldBe(stamp, TimeSpan.FromSeconds(1));

        // Idempotent on the same key: the MERGE updates rather than colliding on the primary key.
        await detector.MarkHighWaterForTenantAsync("Red", 5, stamp.AddMinutes(1), TestContext.Current.CancellationToken);
        (await ReadProgressionAsync("HighWaterMark:Red")).Sequence.ShouldBe(5);

        // And it is genuinely per tenant — the store-global row is untouched.
        (await ReadProgressionAsync("HighWaterMark")).Sequence.ShouldBeNull();
    }

    /// <summary>
    ///     The reader seeds a tenant's mark from its persisted row, which is the whole point of
    ///     persisting it: without this the mark restarts from scratch on every daemon restart.
    /// </summary>
    [Fact]
    public async Task a_persisted_mark_is_read_back_as_the_tenants_current_mark()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        await AppendAsync(store, "Red", 4);

        IHighWaterDetector detector = DetectorFor(store);
        await detector.MarkHighWaterForTenantAsync("Red", 2, DateTimeOffset.UtcNow, TestContext.Current.CancellationToken);

        var vector = await DetectorFor(store)
            .DetectForTenantsAsync(["Red"], TestContext.Current.CancellationToken);

        vector.TryGetStatistics("Red", out var red).ShouldBeTrue();
        red.LastMark.ShouldBe(2, "the persisted row is what the reader starts from");
        red.HighestSequence.ShouldBe(4);
        red.CurrentMark.ShouldBe(4, "nothing is missing between 2 and 4, so the mark walks to the height");
    }

    /// <summary>
    ///     The height is <c>MAX(seq_id)</c> over the tenant's own events, NOT its sequence's
    ///     current_value. Marten hit this as marten#4712.
    ///     <para>
    ///         The restart below reproduces the real-world shape — a tenant whose events arrived through
    ///         the shared sequence, or were renumbered by a bulk insert, leaves its own sequence at the
    ///         START value. SQL Server reports <c>current_value = 1</c> for such a sequence (not 0,
    ///         measured), so the old reader returned HighestSequence = CurrentMark = 1 for a fully
    ///         populated tenant. Equal values read as "caught up" everywhere downstream, which is how a
    ///         tenant with events silently never projected.
    ///     </para>
    /// </summary>
    [Fact]
    public async Task the_height_comes_from_committed_events_not_from_the_tenants_sequence()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        await AppendAsync(store, "Red", 4);

        var ordinal = await OrdinalForAsync("Red");
        await ExecuteAsync($"ALTER SEQUENCE [{Schema}].[pc_events_sequence_{ordinal}] RESTART WITH 1;");

        // Control: the sequence really does now under-report, so the fact below is discriminating
        // rather than passing for an unrelated reason.
        (await SequenceCurrentValueAsync($"pc_events_sequence_{ordinal}"))
            .ShouldBe(1, "the restarted sequence is the misleading source the reader must not use");

        var vector = await DetectorFor(store)
            .DetectForTenantsAsync(["Red"], TestContext.Current.CancellationToken);

        vector.TryGetStatistics("Red", out var red).ShouldBeTrue();
        red.HighestSequence.ShouldBe(4, "four events are committed, whatever the sequence says");
        red.CurrentMark.ShouldBe(4);
    }

    /// <summary>
    ///     The contiguity walk. A hole above the mark stops it at the last contiguous event instead of
    ///     jumping to the height — jumping strands whatever is still in flight in the hole, and for a
    ///     projection a stranded event is never applied at all.
    /// </summary>
    [Fact]
    public async Task the_mark_stops_at_a_gap_rather_than_jumping_to_the_height()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        await AppendAsync(store, "Red", 3);

        var ordinal = await OrdinalForAsync("Red");
        // seq_id 4 and 5 are missing: an append in flight, or one that rolled back.
        await InsertRawEventAsync(ordinal, "Red", 6);

        var vector = await DetectorFor(store)
            .DetectForTenantsAsync(["Red"], TestContext.Current.CancellationToken);

        vector.TryGetStatistics("Red", out var red).ShouldBeTrue();
        red.HighestSequence.ShouldBe(6, "6 is committed, so the height really is 6");
        red.CurrentMark.ShouldBe(3, "but only 1-3 are contiguous, so that is how far the mark may go");
    }

    /// <summary>
    ///     A LEADING gap — nothing committed immediately above the mark, but something committed higher.
    ///     There is no gap EDGE to stop at here (the rows above the mark are contiguous among
    ///     themselves), so a reader that only looked for an interior gap would jump the mark clean over
    ///     the missing range. The mark has to hold instead.
    /// </summary>
    [Fact]
    public async Task a_leading_gap_holds_the_mark_instead_of_stepping_over_it()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        await AppendAsync(store, "Red", 2);

        IHighWaterDetector writer = DetectorFor(store);
        await writer.MarkHighWaterForTenantAsync("Red", 2, DateTimeOffset.UtcNow, TestContext.Current.CancellationToken);

        var ordinal = await OrdinalForAsync("Red");
        // 3 and 4 are missing; 5 is the only row above the mark, so there is no interior gap to find.
        await InsertRawEventAsync(ordinal, "Red", 5);

        var vector = await DetectorFor(store)
            .DetectForTenantsAsync(["Red"], TestContext.Current.CancellationToken);

        vector.TryGetStatistics("Red", out var red).ShouldBeTrue();
        red.HighestSequence.ShouldBe(5);
        red.CurrentMark.ShouldBe(2, "3 and 4 have not landed, so the mark cannot pass them");
    }

    /// <summary>
    ///     A tenant that has NEVER persisted a mark must not be held by a "leading gap". Its events
    ///     simply start above seq_id 1 — which is what a failed first append leaves behind, since the
    ///     rolled-back transaction still consumed sequence values.
    ///     <para>
    ///         This is the stall that a naive gap-respecting rule creates, and it is permanent rather
    ///         than transient: holding is released by staleness, staleness is measured from the
    ///         progression row's <c>last_updated</c>, and a tenant that never persisted a mark has no
    ///         such row to age. It would trade #697's "never advances" for a quieter "never advances".
    ///     </para>
    /// </summary>
    [Fact]
    public async Task a_tenant_whose_events_start_above_one_is_not_held_at_zero()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        // Force the tenant's partition and sequence into existence, then discard those events so the
        // tenant's surviving events begin above 1 with no progression row — the failed-first-append shape.
        await AppendAsync(store, "Red", 2);
        var ordinal = await OrdinalForAsync("Red");
        await ExecuteAsync($"DELETE FROM [{Schema}].[pc_events] WHERE tenant_ordinal = {ordinal};");
        await InsertRawEventAsync(ordinal, "Red", 7);
        await InsertRawEventAsync(ordinal, "Red", 8);

        (await ReadProgressionAsync("HighWaterMark:Red")).Sequence
            .ShouldBeNull("the premise is a tenant that has never persisted a mark");

        var vector = await DetectorFor(store)
            .DetectForTenantsAsync(["Red"], TestContext.Current.CancellationToken);

        vector.TryGetStatistics("Red", out var red).ShouldBeTrue();
        red.LastMark.ShouldBe(0);
        red.HighestSequence.ShouldBe(8);
        red.CurrentMark.ShouldBe(8, "7 and 8 are contiguous with each other; nothing below them is owed");
    }

    /// <summary>
    ///     The escape hatch, on the path that actually runs. A gap that never fills must not hold a
    ///     tenant's mark forever — once the tenant's row is older than
    ///     <c>DaemonSettings.StaleSequenceThreshold</c> the mark skips the gap and says so.
    ///     <para>
    ///         Asserted through <see cref="PolecatHighWaterDetector.DetectForTenantsAsync" />, the
    ///         non-safe-zone entry point, on purpose: that is the only one JasperFx calls, so an escape
    ///         implemented solely in the safe-zone twin would be dead code and the stall would be real.
    ///     </para>
    /// </summary>
    [Fact]
    public async Task a_gap_that_never_fills_is_skipped_once_the_tenant_goes_stale()
    {
        using var store = CreateStore();
        store.Options.DaemonSettings.StaleSequenceThreshold = TimeSpan.FromSeconds(30);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        await AppendAsync(store, "Red", 3);

        var ordinal = await OrdinalForAsync("Red");
        await InsertRawEventAsync(ordinal, "Red", 9);

        IHighWaterDetector writer = DetectorFor(store);

        // Fresh mark: the gap after 3 is respected and the mark holds there.
        await writer.MarkHighWaterForTenantAsync("Red", 3, DateTimeOffset.UtcNow, TestContext.Current.CancellationToken);
        var fresh = await DetectorFor(store).DetectForTenantsAsync(["Red"], TestContext.Current.CancellationToken);
        fresh.TryGetStatistics("Red", out var before).ShouldBeTrue();
        before.CurrentMark.ShouldBe(3);
        before.IncludesSkipping.ShouldBeFalse();

        // Same data, but the tenant's row is now older than the threshold.
        await writer.MarkHighWaterForTenantAsync("Red", 3, DateTimeOffset.UtcNow.AddMinutes(-5),
            TestContext.Current.CancellationToken);
        var stale = await DetectorFor(store).DetectForTenantsAsync(["Red"], TestContext.Current.CancellationToken);
        stale.TryGetStatistics("Red", out var after).ShouldBeTrue();
        after.CurrentMark.ShouldBe(9, "the gap is never going to fill, so the mark moves past it");
        after.IncludesSkipping.ShouldBeTrue("and the skip is reported rather than silent");
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static async Task AppendAsync(DocumentStore store, string tenant, int count)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        var events = new object[count];
        events[0] = new QuestStarted($"{tenant} Quest");
        for (var i = 1; i < count; i++) events[i] = new MonsterSlain($"M{i}", i);
        session.Events.StartStream(Guid.NewGuid(), events);
        await session.SaveChangesAsync();
    }

    private static async Task<(long? Sequence, DateTimeOffset? Updated)> ReadProgressionAsync(string name)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT last_seq_id, last_updated FROM [{Schema}].[pc_event_progression] WHERE name = @name;";
        cmd.Parameters.AddWithValue("@name", name);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return (null, null);
        return (reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetDateTimeOffset(1));
    }

    private static async Task<int> OrdinalForAsync(string tenant)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT ordinal FROM [{Schema}].[pc_tenant_partitions] WHERE tenant_id = @t;";
        cmd.Parameters.AddWithValue("@t", tenant);
        return (int)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task<long> SequenceCurrentValueAsync(string name)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT CAST(current_value AS bigint) FROM sys.sequences WHERE name = @n AND schema_id = SCHEMA_ID(@s);";
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@s", Schema);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    /// <summary>
    ///     Writes one pc_events row at an exact seq_id, which is the only way to construct a hole: the
    ///     append path hands out sequence values contiguously by design.
    /// </summary>
    private static async Task InsertRawEventAsync(int ordinal, string tenant, long seqId)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            INSERT INTO [{Schema}].[pc_events]
                (seq_id, id, stream_id, version, data, type, tenant_id, tenant_ordinal, dotnet_type)
            VALUES (@seq, NEWID(), @stream, @seq, @data, 'quest_started', @tenant, @ordinal, NULL);
            """;
        cmd.Parameters.AddWithValue("@data", "{}");
        cmd.Parameters.AddWithValue("@seq", seqId);
        cmd.Parameters.AddWithValue("@stream", Guid.NewGuid());
        cmd.Parameters.AddWithValue("@tenant", tenant);
        cmd.Parameters.AddWithValue("@ordinal", ordinal);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
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

    private static async Task DropSequencesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'DROP SEQUENCE [' + s.name + '].[' + sq.name + '];'
            FROM sys.sequences sq
            JOIN sys.schemas s ON sq.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }
}
