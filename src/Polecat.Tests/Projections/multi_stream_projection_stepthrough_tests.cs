using System.Text.Json;
using JasperFx.Descriptors;
using JasperFx.Events;
using JasperFx.Events.Grouping;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Projections;

// #346: MultiStreamProjection stepthrough parity. IEventStore.RunMultiStreamProjectionAsync drives a
// registered aggregation projection's REAL execution path — slice → group → EnrichEventsAsync → evolve
// — over a fixed in-memory event list, fanning out across every aggregate identity the projection
// touches and returning one timeline per identity. The fold lives in JasperFx.Events; Polecat delegates
// through ISteppableAggregation.
public class multi_stream_projection_stepthrough_tests : OneOffConfigurationsContext
{
    private static readonly JsonSerializerOptions s_camelCase = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static EventRecord Record<T>(string streamId, string typeName, T data, long sequence, long version)
        => new(
            EventId: Guid.NewGuid(),
            Sequence: sequence,
            StreamVersion: version,
            StreamId: streamId,
            EventTypeName: typeName,
            Data: JsonSerializer.SerializeToElement(data, s_camelCase),
            Metadata: null,
            Timestamp: DateTimeOffset.UtcNow,
            TenantId: "*DEFAULT*",
            Tags: null!);

    private async Task<DocumentStore> StoreWith(ProjectionSourceRegistration register)
    {
        ConfigureStore(opts => register(opts));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
        return theStore;
    }

    private delegate void ProjectionSourceRegistration(StoreOptions opts);

    [Fact]
    public async Task multi_stream_replay_produces_one_timeline_per_identity()
    {
        var store = await StoreWith(opts =>
            opts.Projections.Add(new CustomerSummaryProjection(), ProjectionLifecycle.Inline));

        var alice = Guid.NewGuid();
        var bob = Guid.NewGuid();

        // Events span multiple physical streams and two customer identities — exactly the multi-stream
        // fan-out the daemon slicer performs.
        var events = new List<EventRecord>
        {
            Record(Guid.NewGuid().ToString(), "customer_created", new CustomerCreated(alice, "Alice"), 1, 1),
            Record(Guid.NewGuid().ToString(), "customer_created", new CustomerCreated(bob, "Bob"), 2, 1),
            Record(Guid.NewGuid().ToString(), "order_placed", new OrderPlaced(alice, 100m), 3, 1),
            Record(Guid.NewGuid().ToString(), "order_placed", new OrderPlaced(bob, 200m), 4, 1),
            Record(Guid.NewGuid().ToString(), "order_placed", new OrderPlaced(bob, 150m), 5, 1),
            Record(Guid.NewGuid().ToString(), "payment_received", new PaymentReceived(alice, 40m), 6, 1),
        };

        IEventStore es = store;
        var result = await es.RunMultiStreamProjectionAsync(nameof(CustomerSummary), events, CancellationToken.None);

        result.ProjectionName.ShouldBe(nameof(CustomerSummary));
        result.AggregatesByIdentity.Count.ShouldBe(2);

        var aliceTimeline = result.AggregatesByIdentity[alice.ToString()];
        var aliceFinal = aliceTimeline.FinalState!.Value;
        aliceFinal.GetProperty("name").GetString().ShouldBe("Alice");
        aliceFinal.GetProperty("orderCount").GetInt32().ShouldBe(1);
        aliceFinal.GetProperty("totalSpent").GetDecimal().ShouldBe(100m);
        aliceFinal.GetProperty("totalPaid").GetDecimal().ShouldBe(40m);

        var bobFinal = result.AggregatesByIdentity[bob.ToString()].FinalState!.Value;
        bobFinal.GetProperty("name").GetString().ShouldBe("Bob");
        bobFinal.GetProperty("orderCount").GetInt32().ShouldBe(2);
        bobFinal.GetProperty("totalSpent").GetDecimal().ShouldBe(350m);
    }

    [Fact]
    public async Task multi_stream_timeline_captures_per_event_before_after_steps()
    {
        var store = await StoreWith(opts =>
            opts.Projections.Add(new CustomerSummaryProjection(), ProjectionLifecycle.Inline));

        var alice = Guid.NewGuid();
        var events = new List<EventRecord>
        {
            Record(Guid.NewGuid().ToString(), "customer_created", new CustomerCreated(alice, "Alice"), 1, 1),
            Record(Guid.NewGuid().ToString(), "order_placed", new OrderPlaced(alice, 100m), 2, 1),
        };

        IEventStore es = store;
        var result = await es.RunMultiStreamProjectionAsync(nameof(CustomerSummary), events, CancellationToken.None);

        var steps = result.AggregatesByIdentity[alice.ToString()].Steps;
        steps.Count.ShouldBe(2);
        steps[0].Error.ShouldBeNull();
        // Create step: no state before, state after
        steps[0].Before.ShouldBeNull();
        steps[0].After!.Value.GetProperty("name").GetString().ShouldBe("Alice");
        // Apply step: order count goes 0 -> 1
        steps[1].Before!.Value.GetProperty("orderCount").GetInt32().ShouldBe(0);
        steps[1].After!.Value.GetProperty("orderCount").GetInt32().ShouldBe(1);
    }

    [Fact]
    public async Task single_stream_replay_produces_exactly_one_timeline()
    {
        var store = await StoreWith(opts =>
            opts.Projections.Add<SingleStreamProjection<QuestParty, Guid>>(ProjectionLifecycle.Inline));

        var streamId = Guid.NewGuid();
        var events = new List<EventRecord>
        {
            Record(streamId.ToString(), "quest_started", new QuestStarted("replay"), 1, 1),
            Record(streamId.ToString(), "members_joined", new MembersJoined(1, "Town", new[] { "Pippin" }), 2, 2),
        };

        IEventStore es = store;
        var result = await es.RunMultiStreamProjectionAsync(nameof(QuestParty), events, CancellationToken.None);

        result.AggregatesByIdentity.Count.ShouldBe(1);
        var final = result.AggregatesByIdentity[streamId.ToString()].FinalState!.Value;
        final.GetProperty("name").GetString().ShouldBe("replay");
    }

    [Fact]
    public async Task enrich_events_async_runs_through_the_real_path_per_group()
    {
        var projection = new EnrichingCustomerSummaryProjection();
        var store = await StoreWith(opts => opts.Projections.Add(projection, ProjectionLifecycle.Inline));

        var alice = Guid.NewGuid();
        var bob = Guid.NewGuid();
        var events = new List<EventRecord>
        {
            Record(Guid.NewGuid().ToString(), "customer_created", new CustomerCreated(alice, "Alice"), 1, 1),
            Record(Guid.NewGuid().ToString(), "customer_created", new CustomerCreated(bob, "Bob"), 2, 1),
        };

        IEventStore es = store;
        await es.RunMultiStreamProjectionAsync(nameof(EnrichingCustomerSummary), events, CancellationToken.None);

        // The user override ran through the real fold — invoked once per sliced GROUP (the daemon's
        // per-group enrichment contract), not per event and not per identity. Both identities' slices
        // land in one group here, so a single enrich call covers them: 1 (not 2 events, not 2 ids).
        projection.EnrichCalls.ShouldBe(1);
    }

    [Fact]
    public async Task unknown_projection_throws_argument_exception()
    {
        var store = await StoreWith(opts =>
            opts.Projections.Add(new CustomerSummaryProjection(), ProjectionLifecycle.Inline));

        IEventStore es = store;
        await Should.ThrowAsync<ArgumentException>(() =>
            es.RunMultiStreamProjectionAsync("does_not_exist", new List<EventRecord>(), CancellationToken.None));
    }
}

// Multi-stream projection whose user EnrichEventsAsync override is expected to run through the real
// fold once per sliced group, proving parity with the async daemon's enrichment path.
public partial class EnrichingCustomerSummary
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public partial class EnrichingCustomerSummaryProjection : MultiStreamProjection<EnrichingCustomerSummary, Guid>
{
    public int EnrichCalls;

    public EnrichingCustomerSummaryProjection()
    {
        Identity<CustomerCreated>(e => e.CustomerId);
    }

    public static EnrichingCustomerSummary Create(CustomerCreated e) => new() { Name = e.Name };

    public override Task EnrichEventsAsync(SliceGroup<EnrichingCustomerSummary, Guid> group,
        IQuerySession querySession, CancellationToken cancellation)
    {
        Interlocked.Increment(ref EnrichCalls);
        return Task.CompletedTask;
    }
}
