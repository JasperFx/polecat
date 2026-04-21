using System.Text.Json.Serialization;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using StronglyTypedIds;

namespace Polecat.Tests.Projections;

[StronglyTypedId(Template.Guid)]
public readonly partial struct PaymentId;

public enum PaymentState
{
    Created,
    Initialized,
    Canceled,
    Verified
}

public record PaymentCreated(DateTimeOffset CreatedAt);
public record PaymentCanceled(DateTimeOffset CanceledAt);
public record PaymentVerified(DateTimeOffset VerifiedAt);

public class Payment
{
    [JsonInclude] public PaymentId Id { get; private set; }
    [JsonInclude] public DateTimeOffset CreatedAt { get; private set; }
    [JsonInclude] public PaymentState State { get; private set; }

    public static Payment Create(IEvent<PaymentCreated> @event)
    {
        return new Payment
        {
            Id = new PaymentId(@event.StreamId),
            CreatedAt = @event.Data.CreatedAt,
            State = PaymentState.Created
        };
    }

    public void Apply(PaymentCanceled @event)
    {
        State = PaymentState.Canceled;
    }

    public void Apply(PaymentVerified @event)
    {
        State = PaymentState.Verified;
    }
}

[Collection("integration")]
public class using_guid_based_strong_typed_id_for_aggregate_identity : IntegrationContext
{
    public using_guid_based_strong_typed_id_for_aggregate_identity(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_utilize_strong_typed_id_in_aggregate_stream()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "guid_stid";
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        var payment = await session.Events.AggregateStreamAsync<Payment>(streamId);

        payment.ShouldNotBeNull();
        payment.Id.Value.ShouldBe(streamId);
        payment.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task can_utilize_strong_typed_id_with_inline_aggregations()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "guid_stid_inline";
            opts.Projections.Snapshot<Payment>(SnapshotLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var payment = await query.LoadAsync<Payment>(streamId);

        payment.ShouldNotBeNull();
        payment.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task can_utilize_strong_typed_id_with_explicit_projection_registration()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "guid_stid_explicit";
            opts.Projections.Add(new SingleStreamProjection<Payment, PaymentId>(),
                ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var payment = await query.LoadAsync<Payment>(streamId);

        payment.ShouldNotBeNull();
        payment.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task use_fetch_for_writing_with_strong_typed_id()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "guid_stid_ffw";
            opts.Projections.Add(new SingleStreamProjection<Payment, PaymentId>(),
                ProjectionLifecycle.Inline);
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<Payment>(streamId);

        stream.ShouldNotBeNull();
        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.Id.Value.ShouldBe(streamId);
        stream.Aggregate.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task can_utilize_strong_typed_id_with_async_aggregation()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "guid_stid_async";
            opts.Projections.Snapshot<Payment>(SnapshotLifecycle.Async);
        });

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        await theStore.WaitForProjectionAsync();

        await using var query = theStore.QuerySession();
        var payment = await query.LoadAsync<Payment>(streamId);

        payment.ShouldNotBeNull();
        payment.State.ShouldBe(PaymentState.Verified);

        // Append more events and verify async daemon catches up
        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(streamId, new PaymentCanceled(DateTimeOffset.UtcNow));
        await session2.SaveChangesAsync();

        await theStore.WaitForProjectionAsync();

        await using var query2 = theStore.QuerySession();
        var updated = await query2.LoadAsync<Payment>(streamId);

        updated.ShouldNotBeNull();
        updated.State.ShouldBe(PaymentState.Canceled);
    }
}
