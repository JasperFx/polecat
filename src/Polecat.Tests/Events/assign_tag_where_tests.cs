#nullable enable
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

public record RegionId(Guid Value);

public record OrderPlaced(string OrderNumber, decimal Amount);
public record OrderShipped(string OrderNumber);
public record OrderCancelled(string OrderNumber, string Reason);

public class assign_tag_where_tests : OneOffConfigurationsContext
{
    private RegionId _eastRegion = null!;
    private RegionId _westRegion = null!;

    private async Task SetupStoreAsync()
    {
        _eastRegion = new RegionId(Guid.NewGuid());
        _westRegion = new RegionId(Guid.NewGuid());

        ConfigureStore(opts =>
        {
            opts.Events.RegisterTagType<RegionId>("region");
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task assign_tag_where_by_event_type_name()
    {
        await SetupStoreAsync();

        // Append events WITHOUT tags
        await using var session1 = theStore.LightweightSession();
        var stream1 = Guid.NewGuid();
        session1.Events.Append(stream1,
            new OrderPlaced("ORD-1", 100m),
            new OrderShipped("ORD-1"));
        await session1.SaveChangesAsync();

        // Now retroactively tag all OrderPlaced events with a region
        await using var session2 = theStore.LightweightSession();
        var orderPlacedTypeName = theStore.Options.EventGraph.EventMappingFor(typeof(OrderPlaced)).EventTypeName;
        session2.Events.AssignTagWhere(
            e => e.EventTypeName == orderPlacedTypeName,
            _eastRegion);
        await session2.SaveChangesAsync();

        // Query by tag - should find only the OrderPlaced event
        await using var session3 = theStore.LightweightSession();
        var query = new EventTagQuery().Or<RegionId>(_eastRegion);
        var events = await session3.Events.QueryByTagsAsync(query);

        events.Count.ShouldBe(1);
        events[0].Data.ShouldBeOfType<OrderPlaced>().OrderNumber.ShouldBe("ORD-1");
    }

    [Fact]
    public async Task assign_tag_where_by_stream_id()
    {
        await SetupStoreAsync();

        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        await using var session1 = theStore.LightweightSession();
        session1.Events.Append(stream1,
            new OrderPlaced("ORD-1", 100m),
            new OrderShipped("ORD-1"));
        session1.Events.Append(stream2,
            new OrderPlaced("ORD-2", 200m));
        await session1.SaveChangesAsync();

        // Tag all events in stream1 only
        await using var session2 = theStore.LightweightSession();
        session2.Events.AssignTagWhere(
            e => e.StreamId == stream1,
            _eastRegion);
        await session2.SaveChangesAsync();

        // Query - should find only the 2 events from stream1
        await using var session3 = theStore.LightweightSession();
        var query = new EventTagQuery().Or<RegionId>(_eastRegion);
        var events = await session3.Events.QueryByTagsAsync(query);

        events.Count.ShouldBe(2);
        events[0].Data.ShouldBeOfType<OrderPlaced>().OrderNumber.ShouldBe("ORD-1");
        events[1].Data.ShouldBeOfType<OrderShipped>().OrderNumber.ShouldBe("ORD-1");
    }

    [Fact]
    public async Task assign_tag_where_with_compound_predicate()
    {
        await SetupStoreAsync();

        var stream1 = Guid.NewGuid();

        await using var session1 = theStore.LightweightSession();
        session1.Events.Append(stream1,
            new OrderPlaced("ORD-1", 100m),
            new OrderShipped("ORD-1"),
            new OrderCancelled("ORD-1", "changed mind"));
        await session1.SaveChangesAsync();

        // Tag events that are of type OrderPlaced or OrderCancelled
        await using var session2 = theStore.LightweightSession();
        var placedType = theStore.Options.EventGraph.EventMappingFor(typeof(OrderPlaced)).EventTypeName;
        var cancelledType = theStore.Options.EventGraph.EventMappingFor(typeof(OrderCancelled)).EventTypeName;

        session2.Events.AssignTagWhere(
            e => e.EventTypeName == placedType || e.EventTypeName == cancelledType,
            _eastRegion);
        await session2.SaveChangesAsync();

        // Query - should find 2 events (placed + cancelled, NOT shipped)
        await using var session3 = theStore.LightweightSession();
        var query = new EventTagQuery().Or<RegionId>(_eastRegion);
        var events = await session3.Events.QueryByTagsAsync(query);

        events.Count.ShouldBe(2);
        events.Select(e => e.Data.GetType()).ShouldContain(typeof(OrderPlaced));
        events.Select(e => e.Data.GetType()).ShouldContain(typeof(OrderCancelled));
        events.Select(e => e.Data.GetType()).ShouldNotContain(typeof(OrderShipped));
    }

    [Fact]
    public async Task assign_tag_where_is_idempotent()
    {
        await SetupStoreAsync();

        var stream1 = Guid.NewGuid();

        await using var session1 = theStore.LightweightSession();
        session1.Events.Append(stream1, new OrderPlaced("ORD-1", 100m));
        await session1.SaveChangesAsync();

        var placedType = theStore.Options.EventGraph.EventMappingFor(typeof(OrderPlaced)).EventTypeName;

        // Assign the same tag twice - should not fail or duplicate
        await using var session2 = theStore.LightweightSession();
        session2.Events.AssignTagWhere(
            e => e.EventTypeName == placedType, _eastRegion);
        await session2.SaveChangesAsync();

        await using var session3 = theStore.LightweightSession();
        session3.Events.AssignTagWhere(
            e => e.EventTypeName == placedType, _eastRegion);
        await session3.SaveChangesAsync();

        // Should still just find 1 event
        await using var session4 = theStore.LightweightSession();
        var query = new EventTagQuery().Or<RegionId>(_eastRegion);
        var events = await session4.Events.QueryByTagsAsync(query);
        events.Count.ShouldBe(1);
    }

    [Fact]
    public async Task assign_tag_where_does_not_affect_unmatched_events()
    {
        await SetupStoreAsync();

        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        await using var session1 = theStore.LightweightSession();
        session1.Events.Append(stream1, new OrderPlaced("ORD-1", 100m));
        session1.Events.Append(stream2, new OrderPlaced("ORD-2", 200m));
        await session1.SaveChangesAsync();

        // Only tag events in stream1
        await using var session2 = theStore.LightweightSession();
        session2.Events.AssignTagWhere(
            e => e.StreamId == stream1, _eastRegion);
        await session2.SaveChangesAsync();

        // Tag events in stream2 with different region
        await using var session3 = theStore.LightweightSession();
        session3.Events.AssignTagWhere(
            e => e.StreamId == stream2, _westRegion);
        await session3.SaveChangesAsync();

        // Verify east only has stream1's event
        await using var session4 = theStore.LightweightSession();
        var eastEvents = await session4.Events.QueryByTagsAsync(
            new EventTagQuery().Or<RegionId>(_eastRegion));
        eastEvents.Count.ShouldBe(1);
        eastEvents[0].Data.ShouldBeOfType<OrderPlaced>().OrderNumber.ShouldBe("ORD-1");

        // Verify west only has stream2's event
        var westEvents = await session4.Events.QueryByTagsAsync(
            new EventTagQuery().Or<RegionId>(_westRegion));
        westEvents.Count.ShouldBe(1);
        westEvents[0].Data.ShouldBeOfType<OrderPlaced>().OrderNumber.ShouldBe("ORD-2");
    }

    [Fact]
    public async Task assign_tag_where_throws_for_unregistered_tag_type()
    {
        await SetupStoreAsync();

        await using var session = theStore.LightweightSession();
        Should.Throw<InvalidOperationException>(() =>
        {
            session.Events.AssignTagWhere(e => e.Sequence > 0, new StudentId(Guid.NewGuid()));
        });
    }
}
