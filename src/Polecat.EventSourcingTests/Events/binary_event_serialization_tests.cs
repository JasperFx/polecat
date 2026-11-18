using System.Text;
using System.Text.Json;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Events;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

#region sample_polecat_binary_event_serializer

/// <summary>
///     A deliberately non-JSON binary format, so a test can prove a row really did travel through the
///     binary path: the bytes are a length-prefixed UTF-8 blob that no JSON parser would accept, and a
///     four-byte magic header lets the assertions recognize it on the wire.
/// </summary>
public sealed class TestBinaryEventSerializer : IEventBinarySerializer
{
    public static readonly byte[] Magic = "PCB1"u8.ToArray();

    /// <summary>How many times Serialize/Deserialize were called — proves the path was taken.</summary>
    public int SerializeCount;

    public int DeserializeCount;

    public byte[] Serialize(Type type, object data)
    {
        Interlocked.Increment(ref SerializeCount);
        var payload = JsonSerializer.SerializeToUtf8Bytes(data, type);
        var buffer = new byte[Magic.Length + payload.Length];
        Magic.CopyTo(buffer, 0);
        payload.CopyTo(buffer, Magic.Length);
        return buffer;
    }

    public object Deserialize(Type type, byte[] data)
    {
        Interlocked.Increment(ref DeserializeCount);
        if (data.Length < Magic.Length || !data.AsSpan(0, Magic.Length).SequenceEqual(Magic))
        {
            throw new InvalidOperationException("Not a payload written by this serializer.");
        }

        return JsonSerializer.Deserialize(data.AsSpan(Magic.Length), type)!;
    }
}

#endregion

public record BinaryPayloadRecorded(string Name, int Amount);

public record JsonPayloadRecorded(string Name, int Amount);

[BinaryEvent]
public record AttributeMarkedRecorded(string Name);

public class BinaryLedger
{
    public Guid Id { get; set; }
    public int Total { get; set; }
    public List<string> Names { get; set; } = new();
}

public partial class BinaryLedgerProjection : SingleStreamProjection<BinaryLedger, Guid>
{
    public void Apply(BinaryLedger ledger, BinaryPayloadRecorded e)
    {
        ledger.Total += e.Amount;
        ledger.Names.Add(e.Name);
    }

    public void Apply(BinaryLedger ledger, JsonPayloadRecorded e)
    {
        ledger.Total += e.Amount;
        ledger.Names.Add(e.Name);
    }
}

/// <summary>
///     polecat#388: pluggable binary event serialization at parity with Marten's
///     <c>IEventBinarySerializer</c> (marten#4515). The design point under test throughout is the
///     <b>coexistence</b> one: an additive nullable <c>bdata</c> column with <c>bdata IS NULL</c> as
///     the per-row discriminator, so JSON and binary events live in the same <c>pc_events</c> table
///     and the feature is switchable on an existing store with no data migration.
/// </summary>
public class binary_event_serialization_tests : OneOffConfigurationsContext
{
    private readonly TestBinaryEventSerializer _serializer = new();

    private async Task ConfigureAndApply(Action<StoreOptions> configure)
    {
        ConfigureStore(configure);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    private Task ConfigureBinaryStore(Action<StoreOptions>? extra = null) => ConfigureAndApply(opts =>
    {
        opts.Events.UseBinarySerializer<BinaryPayloadRecorded>(_serializer);
        extra?.Invoke(opts);
    });

    private async Task<(string data, byte[]? bdata)> ReadRawRowAsync(long sequence)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT CONVERT(nvarchar(max), data), bdata FROM {theStore.Options.EventGraph.EventsTableName} WHERE seq_id = @seq";
        var p = cmd.CreateParameter();
        p.ParameterName = "@seq";
        p.Value = sequence;
        cmd.Parameters.Add(p);

        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        (await reader.ReadAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
        var data = reader.GetString(0);
        var bdata = reader.IsDBNull(1) ? null : (byte[])reader.GetValue(1);
        return (data, bdata);
    }

    [Fact]
    public async Task pc_events_carries_a_nullable_bdata_column()
    {
        await ConfigureAndApply(_ => { });

        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT c.is_nullable, TYPE_NAME(c.system_type_id), c.max_length
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(@table) AND c.name = 'bdata'
            """;
        var p = cmd.CreateParameter();
        p.ParameterName = "@table";
        p.Value = theStore.Options.EventGraph.EventsTableName;
        cmd.Parameters.Add(p);

        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        (await reader.ReadAsync(TestContext.Current.CancellationToken))
            .ShouldBeTrue("pc_events should carry the bdata column even with no serializer configured");
        reader.GetBoolean(0).ShouldBeTrue("bdata must be nullable — NULL is the JSON-row discriminator");
        reader.GetString(1).ShouldBe("varbinary");
        reader.GetInt16(2).ShouldBe((short)-1); // varbinary(max)
    }

    [Fact]
    public async Task an_unconfigured_store_writes_json_and_leaves_bdata_null()
    {
        await ConfigureAndApply(_ => { });

        await using var session = theStore.LightweightSession();
        var streamId = session.Events.StartStream(new JsonPayloadRecorded("plain", 3)).Id;
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        var events = await session.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        var (data, bdata) = await ReadRawRowAsync(events[0].Sequence);

        bdata.ShouldBeNull();
        data.ShouldContain("plain");
    }

    [Fact]
    public async Task a_binary_event_round_trips_through_bdata_and_leaves_data_as_a_placeholder()
    {
        await ConfigureBinaryStore();

        Guid streamId;
        await using (var session = theStore.LightweightSession())
        {
            streamId = session.Events.StartStream(new BinaryPayloadRecorded("binary", 7)).Id;
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        _serializer.SerializeCount.ShouldBe(1);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);
        events[0].Data.ShouldBeOfType<BinaryPayloadRecorded>().ShouldBe(new BinaryPayloadRecorded("binary", 7));
        _serializer.DeserializeCount.ShouldBeGreaterThan(0);

        // On the wire: the payload is in bdata, and `data` holds only the placeholder — so the
        // saving the feature exists for is real, not just a round trip that happens to work.
        var (data, bdata) = await ReadRawRowAsync(events[0].Sequence);
        bdata.ShouldNotBeNull();
        bdata!.AsSpan(0, TestBinaryEventSerializer.Magic.Length).SequenceEqual(TestBinaryEventSerializer.Magic)
            .ShouldBeTrue();
        Encoding.UTF8.GetString(bdata).ShouldContain("binary");
        data.ShouldBe("{}");
        data.ShouldNotContain("binary");
    }

    [Fact]
    public async Task json_and_binary_events_coexist_in_one_stream()
    {
        // The coexistence property is the whole reason this design is low-risk to adopt: only the
        // opted-in type changes format, and per-ROW dispatch means one stream can hold both.
        await ConfigureBinaryStore();

        Guid streamId;
        await using (var session = theStore.LightweightSession())
        {
            streamId = session.Events.StartStream(
                new JsonPayloadRecorded("first", 1),
                new BinaryPayloadRecorded("second", 2),
                new JsonPayloadRecorded("third", 3)).Id;
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(3);
        events.Select(x => x.Data).ShouldBe([
            new JsonPayloadRecorded("first", 1),
            new BinaryPayloadRecorded("second", 2),
            new JsonPayloadRecorded("third", 3)
        ]);

        (await ReadRawRowAsync(events[0].Sequence)).bdata.ShouldBeNull();
        (await ReadRawRowAsync(events[1].Sequence)).bdata.ShouldNotBeNull();
        (await ReadRawRowAsync(events[2].Sequence)).bdata.ShouldBeNull();
    }

    [Fact]
    public async Task rows_written_before_the_serializer_was_configured_still_read_as_json()
    {
        // The "switch it on for an existing store with no migration" claim, exercised directly:
        // append as JSON, then reconfigure the same schema with the serializer registered and read
        // the pre-existing row back.
        await ConfigureAndApply(_ => { });

        Guid streamId;
        await using (var session = theStore.LightweightSession())
        {
            streamId = session.Events.StartStream(new BinaryPayloadRecorded("legacy-json", 5)).Id;
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Reconfigure WITHOUT dropping the schema — same tables, now with binary registered.
        ConfigureStore(opts => opts.Events.UseBinarySerializer<BinaryPayloadRecorded>(_serializer));

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events[0].Data.ShouldBe(new BinaryPayloadRecorded("legacy-json", 5));
        _serializer.DeserializeCount.ShouldBe(0, "a bdata IS NULL row must not go through the binary path");

        // And the next append for that type does use binary — the two formats sit side by side.
        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(streamId, new BinaryPayloadRecorded("now-binary", 6));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query2 = theStore.QuerySession();
        var after = await query2.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        after.Count.ShouldBe(2);
        after.Select(x => ((BinaryPayloadRecorded)x.Data).Name).ShouldBe(["legacy-json", "now-binary"]);
        (await ReadRawRowAsync(after[0].Sequence)).bdata.ShouldBeNull();
        (await ReadRawRowAsync(after[1].Sequence)).bdata.ShouldNotBeNull();
    }

    [Fact]
    public async Task the_binary_event_attribute_resolves_against_the_default_serializer()
    {
        await ConfigureAndApply(opts => opts.Events.DefaultBinarySerializer = _serializer);

        Guid streamId;
        await using (var session = theStore.LightweightSession())
        {
            streamId = session.Events.StartStream(
                new AttributeMarkedRecorded("attributed"),
                new JsonPayloadRecorded("not-attributed", 1)).Id;
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events[0].Data.ShouldBe(new AttributeMarkedRecorded("attributed"));
        events[1].Data.ShouldBe(new JsonPayloadRecorded("not-attributed", 1));

        (await ReadRawRowAsync(events[0].Sequence)).bdata.ShouldNotBeNull();
        (await ReadRawRowAsync(events[1].Sequence)).bdata
            .ShouldBeNull("[BinaryEvent] is per type — an unmarked type stays on the JSON path");
    }

    [Fact]
    public async Task an_explicit_registration_wins_over_the_attribute_and_the_default()
    {
        var explicitSerializer = new TestBinaryEventSerializer();
        await ConfigureAndApply(opts =>
        {
            opts.Events.DefaultBinarySerializer = _serializer;
            opts.Events.UseBinarySerializer<AttributeMarkedRecorded>(explicitSerializer);
        });

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(new AttributeMarkedRecorded("explicit"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        explicitSerializer.SerializeCount.ShouldBe(1);
        _serializer.SerializeCount.ShouldBe(0);
    }

    [Fact]
    public async Task a_marked_type_with_no_serializer_configured_throws_rather_than_silently_writing_json()
    {
        // A silent fallback would leave a store whose write amplification does not match its
        // configuration — which is exactly the problem the feature exists to fix.
        await ConfigureAndApply(_ => { });

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(new AttributeMarkedRecorded("unconfigured"));

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await session.SaveChangesAsync(TestContext.Current.CancellationToken));
        ex.Message.ShouldContain("[BinaryEvent]");
        ex.Message.ShouldContain("DefaultBinarySerializer");
    }

    [Fact]
    public async Task binary_events_flow_through_an_inline_projection()
    {
        await ConfigureBinaryStore(opts =>
            opts.Projections.Add<BinaryLedgerProjection>(ProjectionLifecycle.Inline));

        Guid streamId;
        await using (var session = theStore.LightweightSession())
        {
            streamId = session.Events.StartStream(
                new BinaryPayloadRecorded("b", 10),
                new JsonPayloadRecorded("j", 5)).Id;
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        var ledger = await query.LoadAsync<BinaryLedger>(streamId, TestContext.Current.CancellationToken);
        ledger.ShouldNotBeNull();
        ledger!.Total.ShouldBe(15);
        ledger.Names.ShouldBe(["b", "j"]);
    }

    [Fact]
    public async Task binary_events_flow_through_an_async_projection_via_the_daemon()
    {
        // The daemon's event loader is a separate reader from FetchStreamAsync, with its own SELECT
        // and its own deserialization — so it needs its own coverage for the bdata dispatch.
        await ConfigureBinaryStore(opts =>
            opts.Projections.Add<BinaryLedgerProjection>(ProjectionLifecycle.Async));

        Guid streamId;
        await using (var session = theStore.LightweightSession())
        {
            streamId = session.Events.StartStream(
                new BinaryPayloadRecorded("b1", 4),
                new BinaryPayloadRecorded("b2", 6),
                new JsonPayloadRecorded("j1", 1)).Id;
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using var daemon = await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(30));

        await using var query = theStore.QuerySession();
        var ledger = await query.LoadAsync<BinaryLedger>(streamId, TestContext.Current.CancellationToken);
        ledger.ShouldNotBeNull();
        ledger!.Total.ShouldBe(11);
        ledger.Names.ShouldBe(["b1", "b2", "j1"]);
    }

    [Fact]
    public async Task binary_events_hydrate_through_the_event_linq_provider()
    {
        // A third distinct reader (EventListHandler) — same dispatch, separately covered.
        await ConfigureBinaryStore();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(new BinaryPayloadRecorded("via-linq", 42));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents()
            .Where(e => e.EventTypeName == "binary_payload_recorded")
            .ToListAsync(TestContext.Current.CancellationToken);

        events.Count.ShouldBe(1);
        events[0].Data.ShouldBe(new BinaryPayloadRecorded("via-linq", 42));
    }

    [Fact]
    public async Task masking_a_binary_event_rewrites_bdata_not_just_the_json_column()
    {
        // If masking only rewrote `data`, the original payload would stay readable in bdata — for a
        // GDPR masking operation that is the entire point missed.
        await ConfigureBinaryStore();
        theStore.Events.AddMaskingRuleForProtectedInformation<BinaryPayloadRecorded>(
            e => e with { Name = "****" });

        Guid streamId;
        await using (var session = theStore.LightweightSession())
        {
            streamId = session.Events.StartStream(new BinaryPayloadRecorded("secret", 9)).Id;
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeStream(streamId),
            TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events[0].Data.ShouldBeOfType<BinaryPayloadRecorded>().Name.ShouldBe("****");

        var (data, bdata) = await ReadRawRowAsync(events[0].Sequence);
        bdata.ShouldNotBeNull("the masked row must stay binary");
        Encoding.UTF8.GetString(bdata!).ShouldNotContain("secret");
        data.ShouldBe("{}");
    }
}
