using JasperFx;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
/// #219: like document tables, the event store schema (pc_streams / pc_events / pc_event_progression)
/// should be created on the fly on first usage — unless on-the-fly migration is disabled with
/// AutoCreate.None. Previously only ApplyAllDatabaseChangesOnStartup created the event tables, so a
/// first append/query on a fresh database failed with "invalid object name".
/// </summary>
public class on_the_fly_event_store_tests : OneOffConfigurationsContext
{
    public record ThingHappened(string Name);

    [Fact]
    public async Task appending_events_on_a_fresh_database_creates_the_event_store_schema()
    {
        ConfigureStore(_ => { }); // no ApplyAllDatabaseChangesOnStartup

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new ThingHappened("first"));
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
    }

    [Fact]
    public async Task querying_events_on_a_fresh_database_creates_the_event_store_schema()
    {
        ConfigureStore(_ => { });

        // No append yet — a query on a fresh DB must still not blow up on missing tables.
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(Guid.NewGuid());
        events.Count.ShouldBe(0);
    }

    [Fact]
    public async Task auto_create_none_does_not_create_the_event_store_on_the_fly()
    {
        // When the user opts out with AutoCreate.None, on-the-fly creation must NOT happen.
        ConfigureStore(opts => opts.AutoCreateSchemaObjects = AutoCreate.None);

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Guid.NewGuid(), new ThingHappened("x"));

        // The append should fail because the tables were never created and we did not auto-create.
        await Should.ThrowAsync<Exception>(session.SaveChangesAsync());
    }
}
