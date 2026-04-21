using System.Text.Json.Serialization;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using StronglyTypedIds;

namespace Polecat.Tests.Projections;

[StronglyTypedId(Template.String)]
public readonly partial struct Payment2Id;

public class Payment2
{
    [JsonInclude] public Payment2Id Id { get; private set; }
    [JsonInclude] public DateTimeOffset CreatedAt { get; private set; }
    [JsonInclude] public PaymentState State { get; private set; }

    public static Payment2 Create(IEvent<PaymentCreated> @event)
    {
        return new Payment2
        {
            Id = new Payment2Id(@event.StreamKey!),
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
public class using_string_based_strong_typed_id_for_aggregate_identity : IntegrationContext
{
    public using_string_based_strong_typed_id_for_aggregate_identity(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_utilize_strong_typed_id_in_aggregate_stream()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "str_stid";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
        });

        var id = Guid.NewGuid().ToString();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream<Payment2>(id,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        var payment = await session.Events.AggregateStreamAsync<Payment2>(id);

        payment.ShouldNotBeNull();
        payment.Id.Value.ShouldBe(id);
        payment.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task can_utilize_strong_typed_id_with_inline_aggregations()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "str_stid_inline";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<Payment2, Payment2Id>(SnapshotLifecycle.Inline);
        });

        var id = Guid.NewGuid().ToString();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream<Payment2>(id,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var payment = await query.LoadAsync<Payment2>(id);

        payment.ShouldNotBeNull();
        payment.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task can_utilize_strong_typed_id_with_explicit_projection_registration()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "str_stid_explicit";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add(new SingleStreamProjection<Payment2, Payment2Id>(),
                ProjectionLifecycle.Inline);
        });

        var id = Guid.NewGuid().ToString();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream<Payment2>(id,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var payment = await query.LoadAsync<Payment2>(id);

        payment.ShouldNotBeNull();
        payment.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task use_fetch_for_writing_with_strong_typed_id()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "str_stid_ffw";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add(new SingleStreamProjection<Payment2, Payment2Id>(),
                ProjectionLifecycle.Inline);
        });

        var id = Guid.NewGuid().ToString();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream<Payment2>(id,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var stream = await session2.Events.FetchForWriting<Payment2>(id);

        stream.ShouldNotBeNull();
        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.Id.Value.ShouldBe(id);
        stream.Aggregate.State.ShouldBe(PaymentState.Verified);
    }

    [Fact]
    public async Task can_utilize_strong_typed_id_with_async_aggregation()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "str_stid_async";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<Payment2, Payment2Id>(SnapshotLifecycle.Async);
        });

        var id = Guid.NewGuid().ToString();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream<Payment2>(id,
            new PaymentCreated(DateTimeOffset.UtcNow),
            new PaymentVerified(DateTimeOffset.UtcNow));
        await session.SaveChangesAsync();

        await theStore.WaitForProjectionAsync();

        await using var query = theStore.QuerySession();
        var payment = await query.LoadAsync<Payment2>(id);

        payment.ShouldNotBeNull();
        payment.State.ShouldBe(PaymentState.Verified);

        // Append more events and verify async daemon catches up
        await using var session2 = theStore.LightweightSession();
        session2.Events.Append(id, new PaymentCanceled(DateTimeOffset.UtcNow));
        await session2.SaveChangesAsync();

        await theStore.WaitForProjectionAsync();

        await using var query2 = theStore.QuerySession();
        var updated = await query2.LoadAsync<Payment2>(id);

        updated.ShouldNotBeNull();
        updated.State.ShouldBe(PaymentState.Canceled);
    }
}
