#nullable enable
using JasperFx.Events;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Events;

public class querying_event_data_with_linq : OneOffConfigurationsContext
{
    private readonly MembersJoined joined1 = new(1, "Fal Dara", ["Rand", "Matt", "Perrin", "Thom"]);
    private readonly MembersDeparted departed1 = new(5, "Fal Dara", ["Thom"]);

    private readonly MembersJoined joined2 = new(1, "Tower", ["Nynaeve", "Egwene"]);
    private readonly MembersDeparted departed2 = new(10, "Tower", ["Matt"]);

    private async Task SetupStoreAsync()
    {
        ConfigureStore(opts => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task can_query_against_event_type()
    {
        await SetupStoreAsync();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1, departed1);
        session.Events.StartStream(Guid.NewGuid(), joined2, departed2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Events.QueryRawEventDataOnly<MembersJoined>().CountAsync();
        count.ShouldBe(2);

        var allJoined = await query.Events.QueryRawEventDataOnly<MembersJoined>().ToListAsync();
        allJoined.Count.ShouldBe(2);
        allJoined.SelectMany(x => x.Members).Distinct()
            .OrderBy(x => x)
            .ShouldBe(new[] { "Egwene", "Matt", "Nynaeve", "Perrin", "Rand", "Thom" });
    }

    [Fact]
    public async Task can_fetch_all_events()
    {
        await SetupStoreAsync();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1, departed1);
        session.Events.StartStream(Guid.NewGuid(), joined2, departed2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Events.QueryAllRawEvents().ToListAsync();

        results.Count.ShouldBe(4);
    }

    [Fact]
    public async Task can_query_against_event_metadata_sequence()
    {
        await SetupStoreAsync();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1, departed1);
        session.Events.StartStream(Guid.NewGuid(), joined2, departed2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Events.QueryAllRawEvents()
            .CountAsync(x => x.Sequence <= 2);
        count.ShouldBe(2);
    }

    [Fact]
    public async Task can_fetch_by_version()
    {
        await SetupStoreAsync();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1, departed1);
        session.Events.StartStream(Guid.NewGuid(), joined2, departed2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Events.QueryAllRawEvents()
            .CountAsync(x => x.Version == 1);
        count.ShouldBe(2);
    }

    [Fact]
    public async Task can_search_by_stream()
    {
        await SetupStoreAsync();

        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(stream1, joined1, departed1);
        session.Events.StartStream(stream2, joined2, departed2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var count = await query.Events.QueryAllRawEvents()
            .CountAsync(x => x.StreamId == stream1);
        count.ShouldBe(2);
    }

    [Fact]
    public async Task can_filter_by_event_type_name()
    {
        await SetupStoreAsync();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1, departed1);
        session.Events.StartStream(Guid.NewGuid(), joined2, departed2);
        await session.SaveChangesAsync();

        var joinedTypeName = theStore.Options.EventGraph.EventMappingFor(typeof(MembersJoined)).EventTypeName;

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents()
            .Where(x => x.EventTypeName == joinedTypeName)
            .ToListAsync();

        events.Count.ShouldBe(2);
        events.ShouldAllBe(e => e.Data is MembersJoined);
    }

    [Fact]
    public async Task can_query_with_order_by_and_take()
    {
        await SetupStoreAsync();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1, departed1);
        session.Events.StartStream(Guid.NewGuid(), joined2, departed2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents()
            .OrderBy(x => x.Sequence)
            .Take(2)
            .ToListAsync();

        events.Count.ShouldBe(2);
        events[0].Sequence.ShouldBeLessThan(events[1].Sequence);
    }

    [Fact]
    public async Task can_fetch_all_events_after_timestamp()
    {
        await SetupStoreAsync();

        var before = DateTimeOffset.UtcNow.AddSeconds(-5);

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1, departed1);
        session.Events.StartStream(Guid.NewGuid(), joined2, departed2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var results = await query.Events.QueryAllRawEvents()
            .Where(x => x.Timestamp > before)
            .ToListAsync();

        results.Count.ShouldBe(4);
    }

    [Fact]
    public async Task can_query_any()
    {
        await SetupStoreAsync();

        await using var query1 = theStore.QuerySession();
        var anyBefore = await query1.Events.QueryRawEventDataOnly<MembersJoined>().AnyAsync();
        anyBefore.ShouldBeFalse();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1);
        await session.SaveChangesAsync();

        await using var query2 = theStore.QuerySession();
        var anyAfter = await query2.Events.QueryRawEventDataOnly<MembersJoined>().AnyAsync();
        anyAfter.ShouldBeTrue();
    }

    [Fact]
    public async Task can_query_first_or_default()
    {
        await SetupStoreAsync();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1, departed1);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var first = await query.Events.QueryAllRawEvents()
            .OrderBy(x => x.Sequence)
            .FirstOrDefaultAsync();

        first.ShouldNotBeNull();
        first.Data.ShouldBeOfType<MembersJoined>();
    }

    [Fact]
    public async Task can_select_scalar_property()
    {
        await SetupStoreAsync();

        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(stream1, joined1, departed1);
        session.Events.StartStream(stream2, joined2, departed2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var streamIds = await query.Events.QueryAllRawEvents()
            .Select(x => x.StreamId)
            .Distinct()
            .ToListAsync();

        streamIds.Count.ShouldBe(2);
        streamIds.ShouldContain(stream1);
        streamIds.ShouldContain(stream2);
    }

    [Fact]
    public async Task can_query_event_data_with_where_on_data_properties()
    {
        await SetupStoreAsync();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), joined1, departed1);
        session.Events.StartStream(Guid.NewGuid(), joined2, departed2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryRawEventDataOnly<MembersJoined>()
            .Where(x => x.Day == 1)
            .ToListAsync();

        events.Count.ShouldBe(2);
    }
}
