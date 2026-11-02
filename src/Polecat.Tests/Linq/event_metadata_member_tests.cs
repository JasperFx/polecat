using JasperFx.Events;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #875: the metadata columns of an event are looked up by member name alone, so a member of the same
///     name further along a chain read the column. <c>((Body)e.Data).Id</c> is the event body's own id and
///     was the event's. No error, and an ordering or an aggregate that looked fine.
/// </summary>
public class event_metadata_member_tests: OneOffConfigurationsContext
{
    public record NamedBody(Guid Id, long Version, DateTimeOffset Timestamp);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<IQuerySession> seededAsync()
    {
        ConfigureStore(_ => { });
        await theStore.Advanced.CleanAllEventDataAsync(Ct);

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new NamedBody(Guid.NewGuid(), 500, DateTimeOffset.UnixEpoch));
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    [Fact]
    public async Task ordering_by_a_body_member_named_like_a_column_is_refused()
    {
        await using var query = await seededAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Events.QueryAllRawEvents().OrderBy(e => ((NamedBody)e.Data).Id).ToListAsync(Ct));

        ex.Message.ShouldContain("Id");
    }

    [Fact]
    public async Task aggregating_a_body_member_named_like_a_column_is_refused()
    {
        await using var query = await seededAsync();

        // This answered 1, the event's version, where the body says 500.
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Events.QueryAllRawEvents().MaxAsync(e => ((NamedBody)e.Data).Version, Ct));
    }

    [Fact]
    public async Task a_member_of_the_generic_event_type_still_reads_its_column()
    {
        await using var query = await seededAsync();

        var ordered = await query.Events.QueryAllRawEvents()
            .OrderBy(e => ((IEvent<NamedBody>)e).Version).ToListAsync(Ct);

        ordered.ShouldHaveSingleItem();
    }

    [Fact]
    public async Task a_part_of_a_metadata_column_still_translates()
    {
        await using var query = await seededAsync();

        var events = await query.Events.QueryAllRawEvents().Where(e => e.Timestamp.Year > 2000).ToListAsync(Ct);

        events.ShouldHaveSingleItem();
    }
}
