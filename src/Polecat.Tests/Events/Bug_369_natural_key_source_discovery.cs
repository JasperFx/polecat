using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     #369 / jasperfx#569 (sibling of marten#5052). <c>NaturalKeyEventMapping.Extractor</c> widened from
///     <c>Func&lt;object /* event data */, object?&gt;</c> to <c>Func&lt;IEvent, object?&gt;</c>, which is
///     what makes an <c>IEvent&lt;T&gt;</c> <c>[NaturalKeySource]</c> handler bindable at all.
///     <para>
///     Before the widening, discovery silently dropped those handlers: no mapping, no error, no log — so
///     <c>pc_natural_key_X</c> was simply never written for that event type and a lookup after a key
///     change came back null, live and on rebuild alike. Polecat maintains its own lookup table off the
///     same <c>NaturalKeyDefinition</c>, so it had the same exposure, and its rebuild re-emit path
///     (#259) is its own code — hence a Polecat-side regression test rather than relying on upstream's.
///     </para>
/// </summary>
public class Bug_369_natural_key_source_discovery : IAsyncLifetime
{
    private const string Schema = "bug_369_natural_key";
    private const string Table = "pc_natural_key_nkproduct";

    public async ValueTask InitializeAsync() => await DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore(Action<SingleStreamProjection<NkProduct, Guid>>? configure = null)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            var projection = new SingleStreamProjection<NkProduct, Guid>();
            configure?.Invoke(projection);
            opts.Projections.Add(projection, ProjectionLifecycle.Inline);
        });
    }

    // The regression the widened contract exists for: the key source's parameter is IEvent<T>, which
    // yielded no extractor at all before jasperfx#571, so the rename never reached the lookup table.
    [Fact]
    public async Task natural_key_is_maintained_when_the_handler_takes_IEvent()
    {
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkProductRegistered(streamId, "PROD-001"));
            await session.SaveChangesAsync();
        }

        await using (var session = store.LightweightSession())
        {
            session.Events.Append(streamId, new NkProductCodeChanged(streamId, "PROD-999"));
            await session.SaveChangesAsync();
        }

        // Inline append: the IEvent<T>-sourced rename is what used to go missing.
        await using (var query = store.LightweightSession())
        {
            var product = await query.Events.FetchLatest<NkProduct, NkProductCode>(new NkProductCode("PROD-999"));
            product.ShouldNotBeNull();
            product.Id.ShouldBe(streamId);
            product.Code.Value.ShouldBe("PROD-999");
        }
    }

    // Polecat's rebuild re-emits the natural-key upserts per page (#259) through the same
    // QueueOperationForEvent, so the widened contract has to hold on that path too.
    [Fact]
    public async Task natural_key_from_an_IEvent_handler_survives_a_rebuild()
    {
        using var store = CreateStore();

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkProductRegistered(streamId, "PROD-001"));
            session.Events.Append(streamId, new NkProductCodeChanged(streamId, "PROD-999"));
            await session.SaveChangesAsync();
        }

        await ExecuteAsync($"DELETE FROM [{Schema}].[{Table}];");
        (await CountRowsAsync()).ShouldBe(0);

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.RebuildProjectionAsync(store.Options.Projections.All.Single().Name, CancellationToken.None);

        await using var query = store.LightweightSession();
        var product = await query.Events.FetchLatest<NkProduct, NkProductCode>(new NkProductCode("PROD-999"));
        product.ShouldNotBeNull();
        product!.Id.ShouldBe(streamId);
    }

    // jasperfx#571 made NaturalKeyBuilder reachable — its constructor was internal and nothing ever
    // constructed one, so SetBy/SetByEvent were dead code. This is the supported escape hatch when
    // discovery cannot bind a handler, and an explicit registration replaces the discovered mapping.
    [Fact]
    public async Task natural_key_is_maintained_through_an_explicit_registration()
    {
        using var store = CreateStore(p => p.NaturalKeyFor(x => x
            .SetBy<NkProductRegistered>(e => new NkProductCode(e.Code))
            .SetByEvent<NkProductCodeChanged>(e => new NkProductCode(e.Data.NewCode))));

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new NkProductRegistered(streamId, "PROD-001"));
            session.Events.Append(streamId, new NkProductCodeChanged(streamId, "PROD-777"));
            await session.SaveChangesAsync();
        }

        await using var query = store.LightweightSession();
        var product = await query.Events.FetchLatest<NkProduct, NkProductCode>(new NkProductCode("PROD-777"));
        product.ShouldNotBeNull();
        product!.Id.ShouldBe(streamId);
    }

    private static Task<int> CountRowsAsync() => ScalarAsync($"SELECT COUNT(*) FROM [{Schema}].[{Table}];");

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

public sealed record NkProductCode(string Value);

public sealed record NkProductRegistered(Guid ProductId, string Code);

public sealed record NkProductCodeChanged(Guid ProductId, string NewCode);

/// <summary>
///     The key is a pure function of the event: a static <c>[NaturalKeySource]</c> returning the natural
///     key type and taking <c>IEvent&lt;T&gt;</c>. That is the highest-ranked extraction strategy under
///     jasperfx#571 — nothing is fabricated and no user aggregation code has to run to work out the key —
///     and it is exactly the shape that could not bind before the contract widened.
/// </summary>
public partial class NkProduct
{
    public Guid Id { get; set; }

    [NaturalKey]
    public NkProductCode Code { get; set; } = null!;

    [NaturalKeySource]
    public static NkProductCode KeyOnRegistration(IEvent<NkProductRegistered> e) => new(e.Data.Code);

    [NaturalKeySource]
    public static NkProductCode KeyOnRename(IEvent<NkProductCodeChanged> e) => new(e.Data.NewCode);

    public void Apply(NkProductRegistered e)
    {
        Id = e.ProductId;
        Code = new NkProductCode(e.Code);
    }

    public void Apply(NkProductCodeChanged e)
    {
        Code = new NkProductCode(e.NewCode);
    }
}
