using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Subscriptions;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     jasperfx#1025 (JasperFx 2.91.0): under per-tenant event partitioning the daemon rewinds a
///     subscription tenant by tenant. The progress half needed nothing from Polecat, because
///     <c>RewindSubscriptionProgressAsync</c> already treats an unregistered tenant identity
///     (<c>Name:All:tenant</c>) as the exact row to rewind. Marten needed marten#5700 for that half.
///     The timestamp half needed <c>FindEventStoreFloorAtTimeAsync(timestamp, tenantId)</c>, whose
///     interface default throws. Each tenant draws its own sequence, so a store-global floor is a
///     position in another tenant's numbering.
/// </summary>
[Collection("tenant-partitioning")]
public class per_tenant_subscription_rewind_tests : IAsyncLifetime
{
    private const string Schema = "pt_rewind";

    public async ValueTask InitializeAsync()
    {
        await DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await DropSequencesAsync(Schema);
        TenantRewindProbe.Reset();
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
            opts.Projections.Subscribe(new TenantRewindProbe());
        });
    }

    /// <summary>
    ///     Red appends 3 events, then (after the cut) Blue 2 and Red 2. Red's own sequence runs 1..5
    ///     and Blue's 1..2, so Red's floor at the cut is 4 while the store-global answer is 1, which is
    ///     Blue's first event.
    /// </summary>
    [Fact]
    public async Task the_floor_at_a_time_is_read_from_that_tenants_own_sequence()
    {
        var token = TestContext.Current.CancellationToken;
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        var cut = await AppendAroundACutAsync(store, token);

        (await ((IEventDatabase)store.Database).FindEventStoreFloorAtTimeAsync(cut, "Red", token)).ShouldBe(4);
        (await ((IEventDatabase)store.Database).FindEventStoreFloorAtTimeAsync(cut, "Blue", token)).ShouldBe(1);

        // A tenant with no events after the cut has no floor, and a null tenant is the store-global read.
        (await ((IEventDatabase)store.Database).FindEventStoreFloorAtTimeAsync(cut, "Green", token)).ShouldBeNull();
        (await ((IEventDatabase)store.Database).FindEventStoreFloorAtTimeAsync(cut, null, token))
            .ShouldBe(await store.Database.FindEventStoreFloorAtTimeAsync(cut, token));
    }

    /// <summary>
    ///     End to end through the daemon: rewinding Red to the cut restarts Red's agent from Red's own
    ///     floor (4), and Blue's agent is not touched. Before the override this threw
    ///     <see cref="NotSupportedException" /> from the interface default.
    /// </summary>
    /// <remarks>
    ///     The daemon reads a floor as the last sequence PROCESSED, so the replay starts after it: Red
    ///     replays seq 5 only. That is the same one-event boundary as the store-global timestamp rewind
    ///     (polecat#205, Marten semantics). Had the store-global floor (1, Blue's first event) been used,
    ///     Red would have replayed 2..5.
    /// </remarks>
    [Fact]
    public async Task a_per_tenant_timestamp_rewind_replays_only_that_tenant_from_its_own_floor()
    {
        var token = TestContext.Current.CancellationToken;
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: token);

        var cut = await AppendAroundACutAsync(store, token);

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();

        await WaitForAsync(() => TenantRewindProbe.CountFor("Red") == 5 && TenantRewindProbe.CountFor("Blue") == 2,
            "the subscription to deliver every tenant's events the first time");

        var name = store.Options.Projections.AllShards().Single().Name.Name;
        TenantRewindProbe.Reset();

        await daemon.RewindSubscriptionAsync(name, "Red", token, timestamp: cut);

        await WaitForAsync(() => TenantRewindProbe.CountFor("Red") >= 1,
            "Red's agent to replay its events after the cut");

        // Let anything a wrong floor would have replayed arrive before asserting it did not.
        await Task.Delay(1000, token);

        TenantRewindProbe.SequencesFor("Red").ShouldBe([5L]);
        TenantRewindProbe.CountFor("Blue").ShouldBe(0, "a rewind of Red must not restart Blue");

        await daemon.StopAllAsync();
    }

    // Returns a server-clock instant that falls between Red's first three events and the rest.
    private static async Task<DateTimeOffset> AppendAroundACutAsync(DocumentStore store, CancellationToken token)
    {
        await AppendAsync(store, "Red", 3, token);

        // pc_events.timestamp is stamped by SQL Server, so the cut must come from the same clock.
        await Task.Delay(50, token);
        DateTimeOffset cut;
        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(token);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT SYSDATETIMEOFFSET();";
            cut = (DateTimeOffset)(await cmd.ExecuteScalarAsync(token))!;
        }

        await Task.Delay(50, token);

        await AppendAsync(store, "Blue", 2, token);
        await AppendAsync(store, "Red", 2, token);

        return cut;
    }

    private static async Task AppendAsync(DocumentStore store, string tenant, int count, CancellationToken token)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        session.Events.StartStream(Guid.NewGuid(),
            Enumerable.Range(1, count).Select(i => (object)new MembersJoined(i, tenant, [$"{tenant}{i}"])).ToArray());
        await session.SaveChangesAsync(token);
    }

    private static async Task WaitForAsync(Func<bool> condition, string description)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        throw new TimeoutException($"Timed out waiting for {description}. Delivered: " +
                                   string.Join(", ", TenantRewindProbe.Delivered.Select(x => $"{x.Tenant}#{x.Sequence}")));
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

/// <summary>
///     Records the tenant and per-tenant sequence of every event it is handed.
/// </summary>
public class TenantRewindProbe : SubscriptionBase
{
    private static readonly ConcurrentQueue<(string Tenant, long Sequence)> _delivered = new();

    public static IReadOnlyList<(string Tenant, long Sequence)> Delivered => _delivered.ToList();

    public static void Reset() => _delivered.Clear();

    public static int CountFor(string tenant) => _delivered.Count(x => x.Tenant == tenant);

    public static long[] SequencesFor(string tenant)
        => _delivered.Where(x => x.Tenant == tenant).Select(x => x.Sequence).OrderBy(x => x).ToArray();

    public override Task<IChangeListener> ProcessEventsAsync(EventRange page, ISubscriptionController controller,
        IDocumentOperations operations, CancellationToken cancellationToken)
    {
        foreach (var e in page.Events)
        {
            _delivered.Enqueue((e.TenantId, e.Sequence));
        }

        return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
    }
}
