using System.Text.Json.Serialization;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

// polecat#417 / marten#5144: the generic <T, TId> fetch overloads assumed TId was always a natural
// key, so a strong-typed identifier wrapping the stream identity failed with a confusing
// "no natural key definition found" error. A wrapper IS the stream identity, just wrapped.
public readonly record struct Stid5144PaymentId(Guid Value);

public readonly record struct Stid5144InvoiceId(string Value);

public record Stid5144Raised(decimal Amount);

public record Stid5144Settled(decimal Amount);

public partial class Stid5144Payment
{
    [JsonInclude] public Stid5144PaymentId Id { get; private set; }
    [JsonInclude] public decimal Outstanding { get; private set; }

    public static Stid5144Payment Create(IEvent<Stid5144Raised> e)
        => new() { Id = new Stid5144PaymentId(e.StreamId), Outstanding = e.Data.Amount };

    public void Apply(Stid5144Settled e) => Outstanding -= e.Amount;
}

public partial class Stid5144Invoice
{
    [JsonInclude] public Stid5144InvoiceId Id { get; private set; }
    [JsonInclude] public decimal Outstanding { get; private set; }

    public static Stid5144Invoice Create(IEvent<Stid5144Raised> e)
        => new() { Id = new Stid5144InvoiceId(e.StreamKey!), Outstanding = e.Data.Amount };

    public void Apply(Stid5144Settled e) => Outstanding -= e.Amount;
}

[Collection("integration")]
public class strong_typed_id_fetch_overload_tests : IntegrationContext
{
    public strong_typed_id_fetch_overload_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task fetch_for_writing_by_a_guid_backed_strong_typed_id()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "stid5144_guid";
            opts.Projections.Snapshot<Stid5144Payment>(SnapshotLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<Stid5144Payment>(streamId, new Stid5144Raised(100m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events
            .FetchForWriting<Stid5144Payment, Stid5144PaymentId>(
                new Stid5144PaymentId(streamId), TestContext.Current.CancellationToken);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.Id.Value.ShouldBe(streamId);
        stream.Aggregate.Outstanding.ShouldBe(100m);
    }

    [Fact]
    public async Task fetch_latest_by_a_guid_backed_strong_typed_id()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "stid5144_latest";
            opts.Projections.Snapshot<Stid5144Payment>(SnapshotLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<Stid5144Payment>(streamId,
                new Stid5144Raised(100m), new Stid5144Settled(40m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session2 = theStore.LightweightSession();
        var payment = await session2.Events
            .FetchLatest<Stid5144Payment, Stid5144PaymentId>(
                new Stid5144PaymentId(streamId), TestContext.Current.CancellationToken);

        payment.ShouldNotBeNull();
        payment.Outstanding.ShouldBe(60m);
    }

    [Fact]
    public async Task fetch_for_exclusive_writing_by_a_strong_typed_id()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "stid5144_excl";
            opts.Projections.Snapshot<Stid5144Payment>(SnapshotLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<Stid5144Payment>(streamId, new Stid5144Raised(60m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events
            .FetchForExclusiveWriting<Stid5144Payment, Stid5144PaymentId>(
                new Stid5144PaymentId(streamId), TestContext.Current.CancellationToken);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.Outstanding.ShouldBe(60m);
    }

    [Fact]
    public async Task fetch_for_writing_by_a_string_backed_strong_typed_id()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "stid5144_string";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<Stid5144Invoice>(SnapshotLifecycle.Inline);
        });

        var key = "invoice/" + Guid.NewGuid().ToString("N");
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream<Stid5144Invoice>(key, new Stid5144Raised(25m));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events
            .FetchForWriting<Stid5144Invoice, Stid5144InvoiceId>(
                new Stid5144InvoiceId(key), TestContext.Current.CancellationToken);

        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.Id.Value.ShouldBe(key);
        stream.Aggregate.Outstanding.ShouldBe(25m);
    }
}
