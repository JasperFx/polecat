using JasperFx;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     #1139: a read by natural key on a store that had never written an event queried the
///     <c>pc_natural_key_*</c> table before anything created it, and failed with "Invalid object name". The
///     by-key reads now ensure the event store schema on first use, as the stream reads do (#219), so a fresh
///     store answers the same way it does once the table exists: nothing found. Each fact drops the schema and
///     builds a new store, so each one is that store's first event store access.
/// </summary>
public class Bug_1139_natural_key_read_on_a_fresh_store : IAsyncLifetime
{
    private const string Schema = "natural_key_fresh_store";

    public async ValueTask InitializeAsync() => await TestSchema.DropSchemaTablesAsync(Schema);

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
    public async Task fetch_latest_by_natural_key_finds_nothing()
    {
        using var store = CreateStore();
        await using var session = store.LightweightSession();

        (await session.Events.FetchLatest<Shipment, ShipmentCode>(new ShipmentCode("SHP-1"),
            TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    // #1131 (marten#6150): an unknown key is a new stream, as an unknown stream id is
    [Fact]
    public async Task fetch_for_writing_by_natural_key_starts_a_new_stream_for_an_unknown_key()
    {
        using var store = CreateStore();
        await using var session = store.LightweightSession();

        var stream = await session.Events.FetchForWriting<Shipment, ShipmentCode>(new ShipmentCode("SHP-1"),
            TestContext.Current.CancellationToken);
        stream.Aggregate.ShouldBeNull();
        stream.StartingVersion.ShouldBe(0);
    }

    [Fact]
    public async Task fetch_for_exclusive_writing_by_natural_key_starts_a_new_stream_for_an_unknown_key()
    {
        using var store = CreateStore();
        await using var session = store.LightweightSession();

        var stream = await session.Events.FetchForExclusiveWriting<Shipment, ShipmentCode>(new ShipmentCode("SHP-1"),
            TestContext.Current.CancellationToken);
        stream.Aggregate.ShouldBeNull();
        stream.StartingVersion.ShouldBe(0);
    }

    [Fact]
    public async Task the_key_resolves_once_a_stream_claims_it()
    {
        using var store = CreateStore();
        var code = new ShipmentCode("SHP-1");

        await using (var query = store.LightweightSession())
        {
            (await query.Events.FetchLatest<Shipment, ShipmentCode>(code, TestContext.Current.CancellationToken)).ShouldBeNull();
        }

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream<Shipment>(streamId, new ShipmentBooked(code));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var writer = store.LightweightSession();
        (await writer.Events.FetchForWriting<Shipment, ShipmentCode>(code, TestContext.Current.CancellationToken)).Id
            .ShouldBe(streamId);
    }
}
