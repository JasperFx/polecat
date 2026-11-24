using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// An aggregate that can be "deleted" — ShouldDelete returns true when the aggregate
// should be considered removed, causing the aggregator to return null.
public partial class DeletableAggregate
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool IsDeleted { get; set; }

    public static DeletableAggregate Create(ItemCreated e) => new() { Name = e.Name };

    public void Apply(ItemUpdated e) => Name = e.NewName;

    public bool ShouldDelete(ItemDeleted e) => true;
}

/// <summary>#1143: counts its Create calls, to show the walk back no longer rebuilds once per trailing event.</summary>
public partial class CountingDeletableAggregate
{
    public static int Created;

    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Updates { get; set; }

    public static CountingDeletableAggregate Create(ItemCreated e)
    {
        Interlocked.Increment(ref Created);
        return new CountingDeletableAggregate { Name = e.Name };
    }

    public void Apply(ItemUpdated e)
    {
        Name = e.NewName;
        Updates++;
    }

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
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Normal aggregate should return null (deleted)
        await using var query = theStore.QuerySession();
        var normal = await query.Events.AggregateStreamAsync<DeletableAggregate>(streamId, token: TestContext.Current.CancellationToken);
        normal.ShouldBeNull();

        // ToLastKnown should return the state before deletion
        var lastKnown = await query.Events.AggregateStreamToLastKnownAsync<DeletableAggregate>(streamId, token: TestContext.Current.CancellationToken);
        lastKnown.ShouldNotBeNull();
        lastKnown!.Name.ShouldBe("Super Widget");
    }

    [Fact]
    public async Task returns_null_when_stream_is_empty()
    {
        var streamId = Guid.NewGuid();

        await using var query = theStore.QuerySession();
        var result = await query.Events.AggregateStreamToLastKnownAsync<DeletableAggregate>(streamId, token: TestContext.Current.CancellationToken);
        result.ShouldBeNull();
    }

    [Fact]
    public async Task returns_aggregate_when_not_deleted()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new ItemCreated("Gadget"),
            new ItemUpdated("Mega Gadget"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var result = await query.Events.AggregateStreamToLastKnownAsync<DeletableAggregate>(streamId, token: TestContext.Current.CancellationToken);
        result.ShouldNotBeNull();
        result!.Name.ShouldBe("Mega Gadget");
    }

    [Fact]
    public async Task a_version_past_the_end_of_the_stream_finds_nothing()
    {
        // #1143 (marten#6194): as in Marten, a version past the end of the stream is no version of it
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new ItemCreated("Widget"), new ItemUpdated("Super Widget"));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.Events.AggregateStreamAsync<DeletableAggregate>(streamId, 3,
            token: TestContext.Current.CancellationToken)).ShouldBeNull();
        (await query.Events.AggregateStreamToLastKnownAsync<DeletableAggregate>(streamId, 3,
            token: TestContext.Current.CancellationToken)).ShouldBeNull();

        (await query.Events.AggregateStreamAsync<DeletableAggregate>(streamId, 2,
            token: TestContext.Current.CancellationToken))!.Name.ShouldBe("Super Widget");
        (await query.Events.AggregateStreamToLastKnownAsync<DeletableAggregate>(streamId, 1,
            token: TestContext.Current.CancellationToken))!.Name.ShouldBe("Widget");
    }

    [Fact]
    public async Task does_not_rebuild_from_the_start_for_every_event_it_walks_back()
    {
        // #1143 (marten#6194): this rebuilt the aggregate from the first event once for every trailing event that
        // left no aggregate, quadratic in the length of that run
        var streamId = Guid.NewGuid();
        var events = new List<object> { new ItemCreated("Widget"), new ItemUpdated("Super Widget") };
        for (var i = 0; i < 20; i++) events.Add(new ItemDeleted());

        theSession.Events.StartStream(streamId, events.ToArray());
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        Interlocked.Exchange(ref CountingDeletableAggregate.Created, 0);

        var lastKnown = await query.Events.AggregateStreamToLastKnownAsync<CountingDeletableAggregate>(streamId,
            token: TestContext.Current.CancellationToken);
        lastKnown.ShouldNotBeNull();
        lastKnown.Name.ShouldBe("Super Widget");
        lastKnown.Updates.ShouldBe(1);

        // Once on the way forward, once to build the last known aggregate. It was 21.
        CountingDeletableAggregate.Created.ShouldBeLessThanOrEqualTo(2);
    }
}
