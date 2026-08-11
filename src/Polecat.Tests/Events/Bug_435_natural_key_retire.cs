using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     polecat#435 (parity with marten#5041/#5044): a stream has exactly one <em>current</em> natural
///     key, but <c>NaturalKeyProjection</c> only ever upserted. An event that renamed an aggregate
///     therefore left the row carrying the previous value behind, still pointing at the same stream, so
///     three things went wrong at once — the superseded alias kept resolving forever, the table
///     accumulated one dead row per rename, and (because <c>natural_key_value</c> is the primary key)
///     the retired value permanently squatted on its slot so no other stream could ever claim it.
///     <para>
///         The fix queues a stream-scoped retire ahead of each upsert. Reuses the
///         OrderAggregate/OrderNumber/Nk* types from <c>natural_key_tests.cs</c>.
///     </para>
/// </summary>
public class Bug_435_natural_key_retire : IAsyncLifetime
{
    private const string Schema = "natural_key_retire";
    private const string Table = "pc_natural_key_orderaggregate";

    public async ValueTask InitializeAsync() => await DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

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
    public async Task the_superseded_key_stops_resolving_and_its_row_is_gone()
    {
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        var oldKey = new OrderNumber("ORD-OLD");
        var newKey = new OrderNumber("ORD-NEW");

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkOrderCreated(oldKey, "Eve"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await KeysAsync()).ShouldBe(["ORD-OLD"]);

        await using (var session = store.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(oldKey,
                TestContext.Current.CancellationToken);
            stream.AppendOne(new NkOrderNumberChanged(newKey));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // One row, carrying only the current value.
        (await KeysAsync()).ShouldBe(["ORD-NEW"]);

        await using (var query = store.LightweightSession())
        {
            var byNew = await query.Events.FetchForWriting<OrderAggregate, OrderNumber>(newKey,
                TestContext.Current.CancellationToken);
            byNew.Id.ShouldBe(streamId);

            await Should.ThrowAsync<InvalidOperationException>(
                query.Events.FetchForWriting<OrderAggregate, OrderNumber>(oldKey,
                    TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    public async Task a_retired_key_can_be_claimed_by_another_stream()
    {
        // The sharpest consequence of the leak: natural_key_value is the PRIMARY KEY, so a value that
        // was never retired is unavailable to every other stream for the life of the table.
        using var store = CreateStore();

        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        var contested = new OrderNumber("ORD-CONTESTED");

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(first, new NkOrderCreated(contested, "Eve"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(contested,
                TestContext.Current.CancellationToken);
            stream.AppendOne(new NkOrderNumberChanged(new OrderNumber("ORD-RENAMED")));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // The second stream takes the value the first one gave up.
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(second, new NkOrderCreated(contested, "Mallory"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var query = store.LightweightSession())
        {
            var resolved = await query.Events.FetchForWriting<OrderAggregate, OrderNumber>(contested,
                TestContext.Current.CancellationToken);
            resolved.Id.ShouldBe(second);
        }
    }

    [Fact]
    public async Task repeated_renames_never_accumulate_dead_rows()
    {
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkOrderCreated(new OrderNumber("ORD-0"), "Eve"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        for (var i = 1; i <= 4; i++)
        {
            await using var session = store.LightweightSession();
            var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(
                new OrderNumber($"ORD-{i - 1}"), TestContext.Current.CancellationToken);
            stream.AppendOne(new NkOrderNumberChanged(new OrderNumber($"ORD-{i}")));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await KeysAsync()).ShouldBe(["ORD-4"]);
    }

    [Fact]
    public async Task a_create_then_rename_in_one_batch_lands_on_the_newest_value()
    {
        // Ordering check: retire and upsert are queued in pairs, so the LAST pair in the batch wins.
        // A retire that ran after its own upsert, or a batch that reordered them, would leave the
        // table empty or holding the intermediate value.
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId,
                new NkOrderCreated(new OrderNumber("ORD-FIRST"), "Eve"),
                new NkOrderNumberChanged(new OrderNumber("ORD-SECOND")),
                new NkOrderNumberChanged(new OrderNumber("ORD-THIRD")));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await KeysAsync()).ShouldBe(["ORD-THIRD"]);

        await using var query = store.LightweightSession();
        var resolved = await query.Events.FetchForWriting<OrderAggregate, OrderNumber>(
            new OrderNumber("ORD-THIRD"), TestContext.Current.CancellationToken);
        resolved.Id.ShouldBe(streamId);
    }

    [Fact]
    public async Task another_streams_key_is_never_retired()
    {
        // The delete is scoped to the renaming stream. A value legitimately owned by a different
        // stream must survive, whatever the renamer does.
        using var store = CreateStore();

        var mine = Guid.NewGuid();
        var theirs = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(mine, new NkOrderCreated(new OrderNumber("ORD-MINE"), "Eve"));
            session.Events.StartStream(theirs, new NkOrderCreated(new OrderNumber("ORD-THEIRS"), "Bob"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(
                new OrderNumber("ORD-MINE"), TestContext.Current.CancellationToken);
            stream.AppendOne(new NkOrderNumberChanged(new OrderNumber("ORD-MINE-2")));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await KeysAsync()).ShouldBe(["ORD-MINE-2", "ORD-THEIRS"]);

        await using var query = store.LightweightSession();
        var resolved = await query.Events.FetchForWriting<OrderAggregate, OrderNumber>(
            new OrderNumber("ORD-THEIRS"), TestContext.Current.CancellationToken);
        resolved.Id.ShouldBe(theirs);
    }

    [Fact]
    public async Task a_rebuild_reproduces_only_the_current_key()
    {
        // #259 established that a rebuild replays through the same operation builder. Without the
        // retire it therefore reproduced exactly the same set of dead rows after teardown.
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkOrderCreated(new OrderNumber("ORD-A"), "Eve"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession())
        {
            var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(
                new OrderNumber("ORD-A"), TestContext.Current.CancellationToken);
            stream.AppendOne(new NkOrderNumberChanged(new OrderNumber("ORD-B")));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        var projectionName = store.Options.Projections.All.Single().Name;
        await daemon.RebuildProjectionAsync(projectionName, CancellationToken.None);

        (await KeysAsync()).ShouldBe(["ORD-B"]);

        await using var query = store.LightweightSession();
        (await query.Events.FetchForWriting<OrderAggregate, OrderNumber>(new OrderNumber("ORD-B"),
            TestContext.Current.CancellationToken)).Id.ShouldBe(streamId);
    }

    [Fact]
    public async Task archiving_a_stream_does_not_retire_its_row()
    {
        // The retire is queued only alongside an upsert, so the archive path is untouched: an archived
        // stream keeps its lookup row rather than losing it. (Whether that row also ends up flagged
        // is_archived is the separate concern of the NaturalKeyArchiveOperation path and is not
        // asserted here — #435 only widened the upsert path.)
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkOrderCreated(new OrderNumber("ORD-ARCH"), "Eve"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession())
        {
            session.Events.ArchiveStream(streamId);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await KeysAsync()).ShouldBe(["ORD-ARCH"]);
    }

    private static async Task<string[]> KeysAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT natural_key_value FROM [{Schema}].[{Table}] ORDER BY natural_key_value;";
        await using var reader = await cmd.ExecuteReaderAsync();

        var keys = new List<string>();
        while (await reader.ReadAsync())
        {
            keys.Add(reader.GetString(0));
        }

        return keys.ToArray();
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
