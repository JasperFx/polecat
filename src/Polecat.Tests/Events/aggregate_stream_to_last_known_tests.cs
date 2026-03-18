using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// An aggregate that can be "deleted" — ShouldDelete returns true when the aggregate
// should be considered removed, causing the aggregator to return null.
public class DeletableAggregate
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool IsDeleted { get; set; }

    public static DeletableAggregate Create(ItemCreated e) => new() { Name = e.Name };

    public void Apply(ItemUpdated e) => Name = e.NewName;

    public bool ShouldDelete(ItemDeleted e) => true;
}

public record ItemCreated(string Name);
public record ItemUpdated(string NewName);
public record ItemDeleted;

[Collection("integration")]
public class aggregate_stream_to_last_known_tests : IntegrationContext
{
    public aggregate_stream_to_last_known_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task returns_last_known_state_after_deletion()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new ItemCreated("Widget"),
            new ItemUpdated("Super Widget"),
            new ItemDeleted());
        await theSession.SaveChangesAsync();

        // Normal aggregate should return null (deleted)
        await using var query = theStore.QuerySession();
        var normal = await query.Events.AggregateStreamAsync<DeletableAggregate>(streamId);
        normal.ShouldBeNull();

        // ToLastKnown should return the state before deletion
        var lastKnown = await query.Events.AggregateStreamToLastKnownAsync<DeletableAggregate>(streamId);
        lastKnown.ShouldNotBeNull();
        lastKnown!.Name.ShouldBe("Super Widget");
    }

    [Fact]
    public async Task returns_null_when_stream_is_empty()
    {
        var streamId = Guid.NewGuid();

        await using var query = theStore.QuerySession();
        var result = await query.Events.AggregateStreamToLastKnownAsync<DeletableAggregate>(streamId);
        result.ShouldBeNull();
    }

    [Fact]
    public async Task returns_aggregate_when_not_deleted()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new ItemCreated("Gadget"),
            new ItemUpdated("Mega Gadget"));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var result = await query.Events.AggregateStreamToLastKnownAsync<DeletableAggregate>(streamId);
        result.ShouldNotBeNull();
        result!.Name.ShouldBe("Mega Gadget");
    }
}
