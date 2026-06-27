using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
/// #259 (parity with marten#4788/#4793): the pc_natural_key_X lookup table is maintained by the
/// auto-registered NaturalKeyProjection on the inline-append path, which drives off newly-appended
/// StreamActions. The async-daemon rebuild replays persisted events without appending streams, so
/// before the fix the table was never repopulated on rebuild and FetchForWriting by natural key
/// missed. The fix wipes the table on teardown and re-emits the upserts per rebuild page (routed
/// through the projection batch's per-tenant session). Reuses the OrderAggregate/OrderNumber/
/// NkOrderCreated natural-key types from natural_key_tests.cs.
/// </summary>
public class Bug_259_natural_key_rebuild : IAsyncLifetime
{
    private const string Schema = "natural_key_rebuild";
    private const string Table = "pc_natural_key_orderaggregate";

    public async Task InitializeAsync() => await DropSchemaTablesAsync(Schema);

    public Task DisposeAsync() => Task.CompletedTask;

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
        });
    }

    [Fact]
    public async Task natural_key_table_is_repopulated_on_rebuild()
    {
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        var orderNumber = new OrderNumber("ORD-1");
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkOrderCreated(orderNumber, "Alice"));
            await session.SaveChangesAsync();
        }

        // Inline append populated the lookup table + the natural-key fetch works.
        (await CountRowsAsync()).ShouldBe(1);
        await using (var query = store.LightweightSession())
        {
            (await query.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber)).Aggregate.ShouldNotBeNull();
        }

        // Simulate corruption / pre-rebuild state.
        await ExecuteAsync($"DELETE FROM [{Schema}].[{Table}];");
        (await CountRowsAsync()).ShouldBe(0);

        // Rebuild the snapshot projection through the async daemon.
        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        var projectionName = store.Options.Projections.All.Single().Name;
        await daemon.RebuildProjectionAsync(projectionName, CancellationToken.None);

        // The fix: the lookup table is repopulated, so the natural-key fetch succeeds again.
        (await CountRowsAsync()).ShouldBe(1);
        await using (var query = store.LightweightSession())
        {
            var stream = await query.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);
            stream.Aggregate.ShouldNotBeNull();
            stream.Aggregate!.OrderNum.ShouldBe(orderNumber);
        }
    }

    [Fact]
    public async Task natural_key_table_is_repopulated_for_multiple_streams_on_rebuild()
    {
        using var store = CreateStore();

        var orders = Enumerable.Range(0, 5)
            .Select(i => (Id: Guid.NewGuid(), Number: new OrderNumber($"ORD-{i}")))
            .ToList();

        await using (var session = store.LightweightSession())
        {
            foreach (var (oid, num) in orders)
            {
                session.Events.StartStream(oid, new NkOrderCreated(num, "Customer"));
            }

            await session.SaveChangesAsync();
        }

        (await CountRowsAsync()).ShouldBe(5);

        await ExecuteAsync($"DELETE FROM [{Schema}].[{Table}];");

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        var projectionName = store.Options.Projections.All.Single().Name;
        await daemon.RebuildProjectionAsync(projectionName, CancellationToken.None);

        (await CountRowsAsync()).ShouldBe(5);

        await using var query = store.LightweightSession();
        foreach (var (_, num) in orders)
        {
            (await query.Events.FetchForWriting<OrderAggregate, OrderNumber>(num)).Aggregate.ShouldNotBeNull();
        }
    }

    [Fact]
    public async Task rebuild_teardown_wipes_table_and_excludes_archived_streams()
    {
        // Teardown wipe + the archived-stream delta in one test: an active stream's natural-key row is
        // re-emitted on rebuild, while an archived stream's row is NOT (the rebuild event loader filters
        // is_archived = 0, so its events are never replayed). Proving the archived row disappears also
        // proves teardown wiped the table first — rebuild only ever INSERTs, so a surviving row could
        // only mean teardown left it.
        using var store = CreateStore();

        var activeId = Guid.NewGuid();
        var archivedId = Guid.NewGuid();
        var activeNumber = new OrderNumber("ORD-active");

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(activeId, new NkOrderCreated(activeNumber, "Alice"));
            session.Events.StartStream(archivedId, new NkOrderCreated(new OrderNumber("ORD-archived"), "Bob"));
            await session.SaveChangesAsync();
        }

        (await CountRowsAsync()).ShouldBe(2); // both rows present after inline append

        await using (var session = store.LightweightSession())
        {
            session.Events.ArchiveStream(archivedId);
            await session.SaveChangesAsync();
        }

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        var projectionName = store.Options.Projections.All.Single().Name;
        await daemon.RebuildProjectionAsync(projectionName, CancellationToken.None);

        // Only the active stream's row remains: teardown wiped both, rebuild re-emitted only the
        // non-archived stream's events.
        (await CountRowsAsync()).ShouldBe(1);
        await using var query = store.LightweightSession();
        (await query.Events.FetchForWriting<OrderAggregate, OrderNumber>(activeNumber)).Aggregate.ShouldNotBeNull();
    }

    private Task<int> CountRowsAsync() => ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[{Table}];");

    private static async Task<int> ScalarAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
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
}
