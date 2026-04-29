using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
/// Tests that verify self-aggregating types with source-generated evolvers
/// are automatically discovered and registered in StoreOptions.Projections
/// even without explicit Add&lt;SingleStreamProjection&lt;T, Guid&gt;&gt;() registration.
/// </summary>
[Collection("integration")]
public class auto_discover_aggregate_types : IntegrationContext
{
    public auto_discover_aggregate_types(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public void self_aggregating_types_are_auto_discovered()
    {
        // These types have source-generated evolvers via Evolve(IEvent)
        // but are NOT explicitly registered via Projections.Snapshot<T>()
        var aggregateTypes = theStore.Options.Projections.AllAggregateTypes().ToArray();

        // MutableIEventEvolveAggregate has a generated evolver
        aggregateTypes.ShouldContain(typeof(MutableIEventEvolveAggregate));
    }

    [Fact]
    public async Task auto_discovered_type_works_for_live_aggregation()
    {
        // No explicit Snapshot<T>() registration — relies on auto-discovery
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new AEvent(), new BEvent(), new CEvent());
        await theSession.SaveChangesAsync();

        var aggregate = await theSession.Events.AggregateStreamAsync<MutableIEventEvolveAggregate>(streamId);
        aggregate.ShouldNotBeNull();
        aggregate.ACount.ShouldBe(1);
        aggregate.BCount.ShouldBe(1);
        aggregate.CCount.ShouldBe(1);
    }
}
