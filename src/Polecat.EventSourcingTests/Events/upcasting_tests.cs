using JasperFx.Events;
using JasperFx.Events.Upcasting;
using Polecat.Events;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// The event as it was originally written, and as rows in pc_events still record it.
public record RoomBooked(Guid RoomId, string Guest);

// The event this deployment wants to see.
public record RoomReserved(Guid RoomId, string Guest, string Source);

public record RoomCleaned(Guid RoomId);

/// <summary>
///     Polecat-local upcasting coverage (#561 / jasperfx#752) for the read paths the shared
///     <c>UpcastingCompliance</c> suite does not reach.
/// </summary>
/// <remarks>
///     <para>
///         The suite covers stream reads, live aggregation, <c>FetchForWriting</c>, the async daemon,
///         and the marten#4680 authority rule. It cannot cover the paths below, because each one is a
///         Polecat surface with no counterpart in the shared contract: the raw-event LINQ query, the
///         batched stream fetch, and the DCB tag query. All three hydrate rows through their own copy
///         of the read loop rather than through <c>PcEventsRowReader</c>, which is exactly why they
///         are worth pinning — a seam added to one loop and forgotten in another is the
///         characteristic failure here, and nothing upstream would catch it.
///     </para>
///     <para>
///         <c>QueryRawEventDataOnly&lt;T&gt;</c> is deliberately absent: it does not build an
///         <see cref="IEvent" /> at all and upcasting does not apply to it. See the remarks on that
///         method.
///     </para>
/// </remarks>
public class upcasting_tests : OneOffConfigurationsContext
{
    private static readonly string _bookedEventTypeName =
        EventTypeExtensions.GetEventTypeName<RoomBooked>();

    /// <summary>
    ///     Write rows the way the pre-migration application really wrote them — through a store with
    ///     no upcasters — then hand the same schema to a store that has them. Writing them any other
    ///     way would be testing the upcaster against JSON the upcaster's own configuration produced.
    /// </summary>
    private async Task<Guid> ALegacyBookingAsync()
    {
        ConfigureStore(opts => opts.Events.AddEventType<RoomBooked>());
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new RoomBooked(Guid.NewGuid(), "Hilda"), new RoomCleaned(Guid.NewGuid()));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        return streamId;
    }

    private async Task ConfigureUpcastingStoreAsync()
    {
        ConfigureStore(opts =>
            opts.Events.Upcast<RoomBooked, RoomReserved>(
                old => new RoomReserved(old.RoomId, old.Guest, "legacy")));

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();
    }

    [Fact]
    public async Task the_raw_event_linq_query_upcasts()
    {
        var streamId = await ALegacyBookingAsync();
        await ConfigureUpcastingStoreAsync();

        await using var session = theStore.QuerySession();

        var events = await session.Events.QueryAllRawEvents()
            .Where(e => e.StreamId == streamId)
            .ToListAsync(TestContext.Current.CancellationToken);

        var reserved = events.Select(x => x.Data).OfType<RoomReserved>().ShouldHaveSingleItem();
        reserved.Guest.ShouldBe("Hilda");
        reserved.Source.ShouldBe("legacy");
    }

    [Fact]
    public async Task the_batched_stream_fetch_upcasts()
    {
        var streamId = await ALegacyBookingAsync();
        await ConfigureUpcastingStoreAsync();

        await using var session = theStore.QuerySession();

        var batch = session.CreateBatchQuery();
        var fetch = batch.Events.FetchStream(streamId);
        await batch.Execute(TestContext.Current.CancellationToken);

        var events = await fetch;
        events.Select(x => x.Data).OfType<RoomReserved>().ShouldHaveSingleItem()
            .Guest.ShouldBe("Hilda");
    }

    /// <summary>
    ///     The stored event type name keeps recording what was actually written, even though the
    ///     payload and <see cref="IEvent.EventType" /> are now the new type.
    /// </summary>
    /// <remarks>
    ///     Worth pinning because it is the half a store is tempted to "tidy up": the row's alias is a
    ///     fact about the database, and rewriting it on read would make an upcast indistinguishable
    ///     from a native append of the new type — which is precisely what an operator staring at a
    ///     mid-migration store needs to be able to tell apart.
    /// </remarks>
    [Fact]
    public async Task the_stored_event_type_name_still_reports_the_source_name()
    {
        var streamId = await ALegacyBookingAsync();
        await ConfigureUpcastingStoreAsync();

        await using var session = theStore.QuerySession();
        var events = await session.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        var upcast = events.Single(x => x.Data is RoomReserved);
        upcast.EventTypeName.ShouldBe(_bookedEventTypeName);
        upcast.EventType.ShouldBe(typeof(RoomReserved));
    }

    /// <summary>
    ///     Re-registering a stored event type name replaces the earlier transformation rather than
    ///     stacking on it — the last-wins rule the shared registry documents.
    /// </summary>
    [Fact]
    public async Task registering_the_same_source_name_twice_is_last_wins()
    {
        var streamId = await ALegacyBookingAsync();

        ConfigureStore(opts =>
        {
            opts.Events.Upcast<RoomBooked, RoomReserved>(old => new RoomReserved(old.RoomId, old.Guest, "first"));
            opts.Events.Upcast<RoomBooked, RoomReserved>(old => new RoomReserved(old.RoomId, old.Guest, "second"));
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync();

        await using var session = theStore.QuerySession();
        var events = await session.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        events.Select(x => x.Data).OfType<RoomReserved>().ShouldHaveSingleItem()
            .Source.ShouldBe("second");
    }

    /// <summary>
    ///     A store with no upcasters registered reads exactly as it always did.
    /// </summary>
    /// <remarks>
    ///     The negative that keeps the seam honest: every hydration path now carries an upcast branch,
    ///     and this is what says the branch is not taken — and not paid for — by the overwhelming
    ///     majority of stores.
    /// </remarks>
    [Fact]
    public async Task a_store_with_no_upcasters_reads_the_original_type()
    {
        var streamId = await ALegacyBookingAsync();

        await using var session = theStore.QuerySession();
        var events = await session.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);

        events.Select(x => x.Data).OfType<RoomBooked>().ShouldHaveSingleItem()
            .Guest.ShouldBe("Hilda");
        theStore.Options.EventGraph.Upcasters.HasAny.ShouldBeFalse();
    }

    /// <summary>
    ///     The upcast TARGET type is registered with the store at construction, before anything has
    ///     ever appended one.
    /// </summary>
    [Fact]
    public void the_target_event_type_is_pre_registered()
    {
        ConfigureStore(opts =>
            opts.Events.Upcast<RoomBooked, RoomReserved>(
                old => new RoomReserved(old.RoomId, old.Guest, "legacy")));

        theStore.Options.EventGraph.EventMappingFor(typeof(RoomReserved)).ShouldNotBeNull();
    }
}
