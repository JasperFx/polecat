using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Events;

/// <summary>
/// Regression test for https://github.com/JasperFx/marten/issues/4214
/// FetchForWriting throws InvalidCastException when using UseIdentityMapForAggregates
/// with strongly typed IDs.
/// </summary>
[Collection("integration")]
public class Bug_4214_identity_map_strong_typed_ids : IntegrationContext
{
    public Bug_4214_identity_map_strong_typed_ids(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task fetch_for_writing_with_identity_map_and_strong_typed_guid_id_inline()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "bug4214_inline";
            opts.Projections.Add(new SingleStreamProjection<Payment, PaymentId>(), ProjectionLifecycle.Inline);
            opts.Projections.UseIdentityMapForAggregates = true;
        });

        await using var session = theStore.LightweightSession();

        var id = Guid.NewGuid();
        session.Events.StartStream(id,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));

        await session.SaveChangesAsync();

        // This threw InvalidCastException before the fix
        var stream = await session.Events.FetchForWriting<Payment>(id);
        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task fetch_for_writing_with_identity_map_and_strong_typed_guid_id_live()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "bug4214_live";
            opts.Projections.Add(new SingleStreamProjection<Payment, PaymentId>(), ProjectionLifecycle.Live);
            opts.Projections.UseIdentityMapForAggregates = true;
        });

        await using var session = theStore.LightweightSession();

        var id = Guid.NewGuid();
        session.Events.StartStream(id,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));

        await session.SaveChangesAsync();

        var stream = await session.Events.FetchForWriting<Payment>(id);
        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate!.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task fetch_for_writing_twice_with_identity_map_and_strong_typed_guid_id_inline()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "bug4214_twice";
            opts.Projections.Add(new SingleStreamProjection<Payment, PaymentId>(), ProjectionLifecycle.Inline);
            opts.Projections.UseIdentityMapForAggregates = true;
        });

        await using var session = theStore.LightweightSession();

        var id = Guid.NewGuid();
        session.Events.StartStream(id,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));

        await session.SaveChangesAsync();

        // First fetch stores in identity map
        var stream1 = await session.Events.FetchForWriting<Payment>(id);
        stream1.Aggregate.ShouldNotBeNull();

        stream1.AppendOne(new PaymentCanceled(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        // Second fetch should retrieve from identity map without cast error
        var stream2 = await session.Events.FetchForWriting<Payment>(id);
        stream2.Aggregate.ShouldNotBeNull();
        stream2.Aggregate!.State.ShouldBe(PaymentState.Canceled);
    }
}
