using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     #373 (follow-up to #370): <c>StreamState.AggregateType</c> is resolved from the alias persisted in
///     <c>pc_streams.type</c>. These pin the two sources that resolution draws on and the boundary where
///     it honestly gives up.
/// </summary>
public class aggregate_type_resolution_tests : IAsyncLifetime
{
    private const string Schema = "aggregate_type_resolution";

    public async ValueTask InitializeAsync() => await DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore(bool registerProjection = false)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            if (registerProjection)
            {
                opts.Projections.Add<SingleStreamProjection<UnregisteredTag, Guid>>(ProjectionLifecycle.Live);
            }
        });
    }

    /// <summary>
    ///     The gap #373 closes. <c>StartStream&lt;T&gt;</c> accepts any type, but before this the ONLY
    ///     source for resolving the alias back was the registered projections — so an aggregate tagged onto
    ///     a stream without a projection was unresolvable even in the very process that had just written it.
    /// </summary>
    [Fact]
    public async Task the_writing_process_resolves_a_type_that_is_not_a_registered_projection()
    {
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<UnregisteredTag>(streamId, new AtrThingHappened("one"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = store.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId, TestContext.Current.CancellationToken);

        state.ShouldNotBeNull();
        state.AggregateType.ShouldBe(typeof(UnregisteredTag));
    }

    /// <summary>
    ///     The other source, and the one that survives a process restart: a store that did not write the
    ///     stream has an empty alias registry, so resolution falls through to the registered projections.
    /// </summary>
    [Fact]
    public async Task a_fresh_store_resolves_through_its_registered_projections()
    {
        var streamId = Guid.NewGuid();

        using (var writer = CreateStore())
        {
            await using var session = writer.LightweightSession();
            session.Events.StartStream<UnregisteredTag>(streamId, new AtrThingHappened("one"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // A brand new store — nothing has populated its alias registry, exactly like another process or a
        // later run reading rows it did not write.
        using var reader = CreateStore(registerProjection: true);
        await using var query = reader.QuerySession();

        (await query.Events.FetchStreamStateAsync(streamId, TestContext.Current.CancellationToken))!.AggregateType.ShouldBe(typeof(UnregisteredTag));
    }

    /// <summary>
    ///     The boundary, stated honestly: a reader with neither the alias registered nor a matching
    ///     projection cannot resolve the type — and must still answer the metadata read rather than throw.
    ///     A stream tagged by a deployment that knew a type this one does not is not an error.
    /// </summary>
    [Fact]
    public async Task an_unresolvable_alias_yields_a_null_type_rather_than_an_exception()
    {
        var streamId = Guid.NewGuid();

        using (var writer = CreateStore())
        {
            await using var session = writer.LightweightSession();
            session.Events.StartStream<UnregisteredTag>(streamId, new AtrThingHappened("one"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using var reader = CreateStore();
        await using var query = reader.QuerySession();

        var state = await query.Events.FetchStreamStateAsync(streamId, TestContext.Current.CancellationToken);
        state.ShouldNotBeNull();
        state.AggregateType.ShouldBeNull();

        // Everything else on the row still reads.
        state.Id.ShouldBe(streamId);
        state.Version.ShouldBe(1);
    }

    /// <summary>
    ///     #373: the batched read goes through the same resolver as the standalone one, so the two cannot
    ///     disagree about what a given stream is tagged with.
    /// </summary>
    [Fact]
    public async Task the_batched_read_agrees_with_the_standalone_read()
    {
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<UnregisteredTag>(streamId, new AtrThingHappened("one"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = store.QuerySession();
        var standalone = await query.Events.FetchStreamStateAsync(streamId, TestContext.Current.CancellationToken);

        var batch = query.CreateBatchQuery();
        var fetcher = batch.Events.FetchStreamState(streamId);
        await batch.Execute(TestContext.Current.CancellationToken);

        (await fetcher)!.AggregateType.ShouldBe(standalone!.AggregateType);
        (await fetcher)!.AggregateType.ShouldBe(typeof(UnregisteredTag));
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString);
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

public record AtrThingHappened(string What);

/// <summary>
///     Only ever used as a <c>StartStream&lt;T&gt;</c> tag. Deliberately NOT registered as a projection by
///     default — that is the whole point of the #373 cases.
/// </summary>
public class UnregisteredTag
{
    public Guid Id { get; set; }

    public void Apply(AtrThingHappened e)
    {
    }
}
