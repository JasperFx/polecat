using System.Data;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Internal.Operations;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Tests.Events;

/// <summary>#891: the follow-ups from #865, around a natural key's life once its stream starts to change.</summary>
public class natural_key_lifecycle_tests : OneOffConfigurationsContext
{
    private const string Red = "Red";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string Table => "pc_natural_key_orderaggregate";

    private async Task ConfigureAsync(TenancyStyle tenancy = TenancyStyle.Conjoined,
        StreamIdentity identity = StreamIdentity.AsGuid)
    {
        ConfigureStore(opts =>
        {
            opts.Events.TenancyStyle = tenancy;
            opts.Events.StreamIdentity = identity;
            opts.Projections.Add<SingleStreamProjection<OrderAggregate, Guid>>(ProjectionLifecycle.Inline);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
    }

    private IDocumentSession SessionFor(string tenant)
        => theStore.LightweightSession(new SessionOptions { TenantId = tenant });

    private async Task<int> KeyRowsAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{nameof(natural_key_lifecycle_tests)}].[{Table}]";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private async Task<string?> CustomerResolvedByAsync(OrderNumber number)
    {
        await using var session = SessionFor(Red);
        var order = await session.Events.FetchLatest<OrderAggregate, OrderNumber>(number, Ct);
        return order?.CustomerName;
    }

    private async Task RebuildAsync()
    {
        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        var projection = theStore.Options.Projections.All.Single().Name;
        await daemon.RebuildProjectionAsync(projection, CancellationToken.None);
    }

    private async Task<Guid> OrderAsync(OrderNumber number, string customer = "First")
    {
        var streamId = Guid.NewGuid();
        await using var session = SessionFor(Red);
        session.Events.StartStream(streamId, new NkOrderCreated(number, customer));
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task CompactAsync(Guid streamId)
    {
        await using var session = SessionFor(Red);
        await session.Events.CompactStreamAsync<OrderAggregate>(streamId);
        await session.SaveChangesAsync(Ct);
    }

    /// <summary>
    ///     Item 2. Compaction deleted the only event carrying the key, so a rebuild, which starts by
    ///     emptying the table, had nothing to put back and the stream could no longer be found by its key.
    /// </summary>
    [Fact]
    public async Task a_rebuild_keeps_the_key_of_a_compacted_stream()
    {
        await ConfigureAsync();
        var number = new OrderNumber("ORD-COMPACTED");
        var streamId = await OrderAsync(number);

        await CompactAsync(streamId);
        (await KeyRowsAsync()).ShouldBe(1);

        await RebuildAsync();

        (await KeyRowsAsync()).ShouldBe(1);
        (await CustomerResolvedByAsync(number)).ShouldBe("First");
    }

    [Fact]
    public async Task a_rebuild_keeps_the_key_of_a_compacted_stream_that_is_not_conjoined()
    {
        await ConfigureAsync(TenancyStyle.Single);
        var number = new OrderNumber("ORD-COMPACTED-SINGLE");
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkOrderCreated(number, "First"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.CompactStreamAsync<OrderAggregate>(streamId);
            await session.SaveChangesAsync(Ct);
        }

        await RebuildAsync();

        (await KeyRowsAsync()).ShouldBe(1);
    }

    /// <summary>The marker is the oldest thing left, so what follows it still has the last word.</summary>
    [Fact]
    public async Task a_rename_after_compaction_survives_a_rebuild()
    {
        await ConfigureAsync();
        var original = new OrderNumber("ORD-BEFORE");
        var renamed = new OrderNumber("ORD-AFTER");
        var streamId = await OrderAsync(original);

        await CompactAsync(streamId);

        await using (var session = SessionFor(Red))
        {
            session.Events.Append(streamId, new NkOrderCreated(renamed, "Second"));
            await session.SaveChangesAsync(Ct);
        }

        await RebuildAsync();

        (await KeyRowsAsync()).ShouldBe(1);
        (await CustomerResolvedByAsync(renamed)).ShouldNotBeNull();
        (await CustomerResolvedByAsync(original)).ShouldBeNull();
    }

    /// <summary>Item 3. The upsert was queued after the tombstone, so it ran after the delete meant to take it.</summary>
    [Fact]
    public async Task starting_and_tombstoning_a_stream_in_one_save_leaves_no_key()
    {
        await ConfigureAsync();
        var number = new OrderNumber("ORD-ONE-SAVE");
        var streamId = Guid.NewGuid();

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream(streamId, new NkOrderCreated(number, "First"));
            session.Events.TombstoneStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        (await KeyRowsAsync()).ShouldBe(0);

        await OrderAsync(number, "Second");
        (await CustomerResolvedByAsync(number)).ShouldBe("Second");
    }

    [Fact]
    public async Task appending_to_and_tombstoning_a_stream_in_one_save_leaves_no_key()
    {
        await ConfigureAsync();
        var streamId = await OrderAsync(new OrderNumber("ORD-APPENDED"));

        await using (var session = SessionFor(Red))
        {
            session.Events.Append(streamId, new NkOrderCreated(new OrderNumber("ORD-RENAMED"), "Second"));
            session.Events.TombstoneStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        (await KeyRowsAsync()).ShouldBe(0);
    }

    [Fact]
    public async Task only_the_tombstoned_stream_loses_its_key_in_that_save()
    {
        await ConfigureAsync();
        var kept = new OrderNumber("ORD-KEPT");
        var streamId = Guid.NewGuid();

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream(streamId, new NkOrderCreated(new OrderNumber("ORD-GONE"), "First"));
            session.Events.StartStream(Guid.NewGuid(), new NkOrderCreated(kept, "Kept"));
            session.Events.TombstoneStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        (await KeyRowsAsync()).ShouldBe(1);
        (await CustomerResolvedByAsync(kept)).ShouldBe("Kept");
    }

    /// <summary>
    ///     Without conjoined tenancy the table has a foreign key, so the write this skips used to fail the
    ///     save outright rather than leave a row.
    /// </summary>
    [Fact]
    public async Task starting_and_tombstoning_a_stream_in_one_save_with_a_foreign_key()
    {
        await ConfigureAsync(TenancyStyle.Single);
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkOrderCreated(new OrderNumber("ORD-FK"), "First"));
            session.Events.TombstoneStream(streamId);
            await session.SaveChangesAsync(Ct);
        }

        (await KeyRowsAsync()).ShouldBe(0);
    }

    private async Task<IReadOnlyList<string>> IndexColumnsAsync(string index)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID('[{nameof(natural_key_lifecycle_tests)}].[{Table}]') AND i.name = '{index}'
            ORDER BY ic.key_ordinal
            """;

        var columns = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) columns.Add(reader.GetString(0));
        return columns;
    }

    [Theory]
    [InlineData(TenancyStyle.Conjoined, StreamIdentity.AsGuid, "stream_id", new[] { "stream_id", "tenant_id" })]
    [InlineData(TenancyStyle.Conjoined, StreamIdentity.AsString, "stream_key", new[] { "stream_key", "tenant_id" })]
    [InlineData(TenancyStyle.Single, StreamIdentity.AsGuid, "stream_id", new[] { "stream_id" })]
    [InlineData(TenancyStyle.Single, StreamIdentity.AsString, "stream_key", new[] { "stream_key" })]
    public async Task the_stream_column_is_indexed(TenancyStyle tenancy, StreamIdentity identity, string column,
        string[] expected)
    {
        await ConfigureAsync(tenancy, identity);

        (await IndexColumnsAsync($"ix_{Table}_{column}")).ShouldBe(expected);
    }

    /// <summary>Item 4: an existing table gets the index from a first delta that touches nothing else.</summary>
    [Fact]
    public async Task an_existing_table_gets_the_index_additively()
    {
        await ConfigureAsync();
        var number = new OrderNumber("ORD-EXISTING");
        await OrderAsync(number);

        var index = $"ix_{Table}_stream_id";

        await using (var conn = await OpenConnectionAsync())
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"DROP INDEX [{index}] ON [{nameof(natural_key_lifecycle_tests)}].[{Table}]";
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        var migration = await theDatabase.CreateMigrationAsync(Ct);
        var delta = migration.Deltas.Single(x => x.SchemaObject.Identifier.Name == Table);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        var writer = new StringWriter();
        migration.WriteAllUpdates(writer, theDatabase.Migrator, AutoCreate.CreateOrUpdate);
        writer.ToString().ShouldContain(index);
        writer.ToString().ShouldNotContain("DROP TABLE");

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await IndexColumnsAsync(index)).ShouldBe(["stream_id", "tenant_id"]);
        (await KeyRowsAsync()).ShouldBe(1);
        (await CustomerResolvedByAsync(number)).ShouldBe("First");

        var after = await theDatabase.CreateMigrationAsync(Ct);
        after.Deltas.Single(x => x.SchemaObject.Identifier.Name == Table)
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_index_is_in_the_generated_script()
    {
        await ConfigureAsync();

        theStore.Advanced.ToDatabaseScript().ShouldContain($"ix_{Table}_stream_id");
    }

    /// <summary>
    ///     The note at the end of #891: AddParameters binds a string as nvarchar, and against a varchar
    ///     column that scanned, 230 logical reads to 2 on 50,000 rows.
    /// </summary>
    [Theory]
    [InlineData(StreamIdentity.AsString)]
    [InlineData(StreamIdentity.AsGuid)]
    public void a_tombstone_binds_a_string_as_varchar(StreamIdentity identity)
    {
        ConfigureStore(opts => opts.Events.StreamIdentity = identity);

        object streamId = identity == StreamIdentity.AsString ? "a-stream" : Guid.NewGuid();
        var operation = new TombstoneStreamOperation(theStore.Events, streamId, "tenant");

        using var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        operation.ConfigureCommand(builder);
        builder.Compile();

        var parameters = batch.BatchCommands.Cast<SqlBatchCommand>().SelectMany(x => x.Parameters.Cast<SqlParameter>())
            .ToList();

        parameters.Count.ShouldBe(2);
        parameters[0].SqlDbType.ShouldBe(identity == StreamIdentity.AsString
            ? SqlDbType.VarChar
            : SqlDbType.UniqueIdentifier);
        parameters[1].SqlDbType.ShouldBe(SqlDbType.VarChar);
    }

    [Fact]
    public async Task a_tombstone_of_a_string_stream_finds_it_through_a_varchar_variable()
    {
        await ConfigureAsync(identity: StreamIdentity.AsString);

        await using (var session = SessionFor(Red))
        {
            session.Events.StartStream("a-stream", new NkOrderCreated(new OrderNumber("ORD-STRING"), "First"));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = SessionFor(Red))
        {
            session.Events.TombstoneStream("a-stream");
            await session.SaveChangesAsync(Ct);
        }

        (await KeyRowsAsync()).ShouldBe(0);

        await using var query = theStore.QuerySession(new SessionOptions { TenantId = Red });
        (await query.Events.FetchStreamAsync("a-stream", token: Ct)).ShouldBeEmpty();
    }
}
