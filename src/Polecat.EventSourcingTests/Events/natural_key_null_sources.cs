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

#region sample_polecat_natural_key_sources_and_null

public record ShipmentCode(string Value);

public record ShipmentBooked(ShipmentCode Code);

public record ShipmentUpdated(ShipmentCode? NewCode, string Notes);

public record ShipmentCodeWithdrawn;

public class Shipment
{
    public Guid Id { get; set; }

    [NaturalKey]
    public ShipmentCode? Code { get; set; }

    public string Notes { get; set; } = string.Empty;

    [NaturalKeySource]
    public void Apply(ShipmentBooked e) => Code = e.Code;

    // Polecat reads the key off the event here, so a ShipmentUpdated whose NewCode is null means
    // "this event carries no key", and the stream keeps its code
    [NaturalKeySource]
    public void Apply(ShipmentUpdated e)
    {
        if (e.NewCode != null) Code = e.NewCode;
        Notes = e.Notes;
    }

    // A static source that returns the key type is explicit, so its null removes the stream's key
    [NaturalKeySource]
    public static ShipmentCode? KeyFor(ShipmentCodeWithdrawn e) => null;

    public void Apply(ShipmentCodeWithdrawn e) => Code = null;
}

#endregion

/// <summary>
///     jasperfx#1287 (marten#6153, polecat#1137): what a null from a natural key source means. Only an explicit
///     source -- a <c>[NaturalKeySource]</c> static method returning the key type, or a <c>NaturalKeyFor()</c>
///     registration -- removes the stream's key. A null from a source discovery inferred, here a conditional
///     <c>Apply</c>, means the event carried no key. <c>NaturalKeyCompliance</c> covers the inline append path;
///     these cover what it does not: another stream claiming a removed key, a key that is not removed staying
///     unclaimable, and the rebuild path, which replays the same events through the same rule.
/// </summary>
public class natural_key_null_sources : IAsyncLifetime
{
    private const string Schema = "natural_key_null_sources";
    private const string Table = "pc_natural_key_shipment";

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
            opts.Projections.Add<SingleStreamProjection<Shipment, Guid>>(ProjectionLifecycle.Inline);
        });
    }

    [Fact]
    public async Task an_inferred_null_keeps_the_key_from_another_stream()
    {
        using var store = CreateStore();
        var code = new ShipmentCode($"SHP-{Guid.NewGuid():N}");
        var streamId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Shipment>(streamId, new ShipmentBooked(code));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession())
        {
            session.Events.Append(streamId, new ShipmentUpdated(null, "rerouted"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // The key is still the first stream's, so a second stream claiming it is refused
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Shipment>(Guid.NewGuid(), new ShipmentBooked(code));
            await Should.ThrowAsync<DuplicateNaturalKeyException>(() => session.SaveChangesAsync(TestContext.Current.CancellationToken));
        }

        await using var query = store.LightweightSession();
        var stream = await query.Events.FetchForWriting<Shipment, ShipmentCode>(code, TestContext.Current.CancellationToken);
        stream.Id.ShouldBe(streamId);
        stream.Aggregate!.Notes.ShouldBe("rerouted");
    }

    [Fact]
    public async Task an_explicit_null_frees_the_key_for_another_stream()
    {
        using var store = CreateStore();
        var code = new ShipmentCode($"SHP-{Guid.NewGuid():N}");
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Shipment>(first, new ShipmentBooked(code));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session = store.LightweightSession())
        {
            session.Events.Append(first, new ShipmentCodeWithdrawn());
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await CountRowsAsync()).ShouldBe(0);

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Shipment>(second, new ShipmentBooked(code));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = store.LightweightSession();
        var latest = await query.Events.FetchLatest<Shipment, ShipmentCode>(code, TestContext.Current.CancellationToken);
        latest.ShouldNotBeNull();
        latest.Id.ShouldBe(second);
    }

    [Fact]
    public async Task an_explicit_null_in_the_same_session_as_the_key_leaves_no_key()
    {
        using var store = CreateStore();
        var code = new ShipmentCode($"SHP-{Guid.NewGuid():N}");

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Shipment>(Guid.NewGuid(), new ShipmentBooked(code), new ShipmentCodeWithdrawn());
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await CountRowsAsync()).ShouldBe(0);

        await using var query = store.LightweightSession();
        (await query.Events.FetchLatest<Shipment, ShipmentCode>(code, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    [Fact]
    public async Task a_rebuild_applies_the_same_rule()
    {
        using var store = CreateStore();
        var keptCode = new ShipmentCode($"SHP-{Guid.NewGuid():N}");
        var withdrawnCode = new ShipmentCode($"SHP-{Guid.NewGuid():N}");
        var kept = Guid.NewGuid();
        var withdrawn = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Shipment>(kept, new ShipmentBooked(keptCode), new ShipmentUpdated(null, "held"));
            session.Events.StartStream<Shipment>(withdrawn, new ShipmentBooked(withdrawnCode), new ShipmentCodeWithdrawn());
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await CountRowsAsync()).ShouldBe(1);

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        var projectionName = store.Options.Projections.All.Single().Name;
        await daemon.RebuildProjectionAsync(projectionName, TestContext.Current.CancellationToken);

        (await CountRowsAsync()).ShouldBe(1);

        await using var query = store.LightweightSession();
        var stream = await query.Events.FetchForWriting<Shipment, ShipmentCode>(keptCode, TestContext.Current.CancellationToken);
        stream.Id.ShouldBe(kept);
        (await query.Events.FetchLatest<Shipment, ShipmentCode>(withdrawnCode, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    /// <summary>
    ///     polecat#1137: a natural key lookup reads the lookup table, which the session writes only when it
    ///     commits, so a key appended in the same session isn't found until SaveChangesAsync. FetchLatest returns
    ///     null and FetchForWriting throws, as it does for a key nothing has claimed. The docs used to say the
    ///     lookup saw the session's uncommitted data.
    /// </summary>
    [Fact]
    public async Task a_lookup_by_natural_key_does_not_see_the_sessions_pending_events()
    {
        using var store = CreateStore();

        // Commit one stream first, so the lookup table exists
        await using (var warm = store.LightweightSession())
        {
            warm.Events.StartStream<Shipment>(Guid.NewGuid(), new ShipmentBooked(new ShipmentCode($"SHP-{Guid.NewGuid():N}")));
            await warm.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var code = new ShipmentCode($"SHP-{Guid.NewGuid():N}");
        var streamId = Guid.NewGuid();

        await using var session = store.LightweightSession();
        session.Events.StartStream<Shipment>(streamId, new ShipmentBooked(code));

        (await session.Events.FetchLatest<Shipment, ShipmentCode>(code, TestContext.Current.CancellationToken)).ShouldBeNull();
        await Should.ThrowAsync<InvalidOperationException>(() =>
            session.Events.FetchForWriting<Shipment, ShipmentCode>(code, TestContext.Current.CancellationToken));

        // After the commit, the same key finds the stream
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = store.LightweightSession();
        (await query.Events.FetchLatest<Shipment, ShipmentCode>(code, TestContext.Current.CancellationToken))!.Id.ShouldBe(streamId);
    }

    [Fact]
    public async Task a_null_key_refusal_says_to_check_for_a_missing_key()
    {
        using var store = CreateStore();
        await using var session = store.LightweightSession();

        var ex = await Should.ThrowAsync<ArgumentNullException>(() =>
            session.Events.FetchForWriting<Shipment, ShipmentCode>(new ShipmentCode(null!), TestContext.Current.CancellationToken));

        ex.Message.ShouldContain(typeof(ShipmentCode).FullName!);
        ex.Message.ShouldContain("check for a missing key before fetching");
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
