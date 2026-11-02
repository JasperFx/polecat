using JasperFx;
using JasperFx.Events;
using Polecat.Events;
using Polecat.Events.Archiving;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>A body with an <c>IsArchived</c> of its own, which says nothing about the row.</summary>
public record CaseFiled(string Reference, bool IsArchived);

/// <summary>
///     Archived events are left out of every event read unless the caller opts in, on every store.
///     Polecat filtered them but left no way in: its <c>is_archived = 0</c> sat beside a predicate naming
///     <c>IsArchived</c>, so <c>x.IsArchived</c> matched nothing, and there was no <c>MaybeArchived()</c>.
/// </summary>
/// <remarks>
///     Every read here is asked about an archived stream with a live stream beside it, so a read that
///     returned nothing at all, or everything, would still fail. The event table is shared with the rest
///     of the collection, so every predicate is scoped to those two streams.
/// </remarks>
[Collection("integration")]
public class archived_events_are_opt_in_tests : IntegrationContext
{
    private readonly Guid _archived = Guid.NewGuid();
    private readonly Guid _live = Guid.NewGuid();

    public archived_events_are_opt_in_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private IReadOnlyEventStore Reader => ((IEventStore)theStore).OpenReadOnlyEventStore();

    /// <summary>Two archived events in one stream, and one live event in the other.</summary>
    private async Task ArchiveOneStreamBesideALiveOne()
    {
        theSession.Events.StartStream(_archived, new QuestStarted("Archived"), new MembersJoined(1, "Town", ["A"]));
        theSession.Events.StartStream(_live, new QuestStarted("Live"));
        await theSession.SaveChangesAsync(Token);

        await using var session = theStore.LightweightSession();
        session.Events.ArchiveStream(_archived);
        await session.SaveChangesAsync(Token);
    }

    [Fact]
    public async Task naming_is_archived_finds_only_the_archived_events()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents()
            .Where(x => x.IsArchived && (x.StreamId == _archived || x.StreamId == _live))
            .ToListAsync(Token);

        events.Count.ShouldBe(2);
        events.ShouldAllBe(x => x.IsArchived && x.StreamId == _archived);
    }

    [Fact]
    public async Task negating_is_archived_finds_only_the_live_events()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents()
            .Where(x => !x.IsArchived && (x.StreamId == _archived || x.StreamId == _live))
            .ToListAsync(Token);

        events.Single().StreamId.ShouldBe(_live);
    }

    [Fact]
    public async Task comparing_is_archived_opts_in_as_well()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var inScope = query.Events.QueryAllRawEvents().Where(x => x.StreamId == _archived || x.StreamId == _live);

        (await inScope.Where(x => x.IsArchived == true).ToListAsync(Token)).Count.ShouldBe(2);
        (await inScope.Where(x => x.IsArchived != false).ToListAsync(Token)).Count.ShouldBe(2);
        (await inScope.Where(x => x.IsArchived == false).ToListAsync(Token)).Count.ShouldBe(1);
        (await inScope.Where(x => x.IsArchived != true).ToListAsync(Token)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task maybe_archived_finds_live_and_archived_events_together()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents()
            .Where(x => x.MaybeArchived() && (x.StreamId == _archived || x.StreamId == _live))
            .ToListAsync(Token);

        events.Count.ShouldBe(3);
        events.Where(x => x.StreamId == _archived).ShouldAllBe(x => x.IsArchived);
        events.Single(x => x.StreamId == _live).IsArchived.ShouldBeFalse();
    }

    [Fact]
    public async Task the_documented_opt_ins_read_what_the_docs_say()
    {
        await ArchiveOneStreamBesideALiveOne();
        var streamId = _archived;
        await using var session = theStore.QuerySession();

        #region sample_polecat_reading_archived_events
        // Live and archived events together. MaybeArchived() is in Polecat.Events.Archiving.
        var everything = await session.Events.QueryAllRawEvents()
            .Where(x => x.MaybeArchived() && x.StreamId == streamId)
            .ToListAsync();

        // Naming IsArchived replaces the implicit filter, so this finds only archived events...
        var archived = await session.Events.QueryAllRawEvents()
            .Where(x => x.IsArchived && x.StreamId == streamId)
            .ToListAsync();

        // ...and this finds only live ones.
        var live = await session.Events.QueryAllRawEvents()
            .Where(x => !x.IsArchived && x.StreamId == streamId)
            .ToListAsync();
        #endregion

        everything.Count.ShouldBe(2);
        archived.Count.ShouldBe(2);
        live.ShouldBeEmpty();
    }

    [Fact]
    public async Task an_opt_in_on_one_side_of_an_or_counts()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents()
            .Where(x => (x.IsArchived || x.Version == 1) && (x.StreamId == _archived || x.StreamId == _live))
            .ToListAsync(Token);

        events.Count.ShouldBe(3);

        // The negated form opts in too, so the first event of the archived stream comes along with it
        var firsts = await query.Events.QueryAllRawEvents()
            .Where(x => (!x.IsArchived || x.Version == 1) && (x.StreamId == _archived || x.StreamId == _live))
            .ToListAsync(Token);

        firsts.Count.ShouldBe(2);
        firsts.Single(x => x.StreamId == _archived).Version.ShouldBe(1);
    }

    [Fact]
    public async Task an_opt_in_counts_from_any_where_in_the_chain()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents()
            .Where(x => x.StreamId == _archived || x.StreamId == _live)
            .Where(x => x.Version > 0)
            .Where(x => x.IsArchived)
            .ToListAsync(Token);

        events.Count.ShouldBe(2);
    }

    [Fact]
    public async Task an_opt_in_counts_as_the_inline_predicate_of_a_terminal()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var stream = query.Events.QueryAllRawEvents().Where(x => x.StreamId == _archived);

        (await stream.CountAsync(x => x.IsArchived, Token)).ShouldBe(2);
        (await stream.CountAsync(x => x.MaybeArchived(), Token)).ShouldBe(2);
        (await stream.AnyAsync(x => x.IsArchived, Token)).ShouldBeTrue();
        (await stream.FirstOrDefaultAsync(x => x.IsArchived, Token)).ShouldNotBeNull();
    }

    [Fact]
    public async Task the_store_agnostic_query_takes_the_same_opt_ins()
    {
        await ArchiveOneStreamBesideALiveOne();

        var events = Reader.QueryRawEvents();

        (await events.Where(x => x.IsArchived && x.StreamId == _archived).ToListAsync(Token)).Count.ShouldBe(2);
        (await events.Where(x => x.MaybeArchived() && x.StreamId == _archived).ToListAsync(Token)).Count.ShouldBe(2);
    }

    [Fact]
    public async Task aggregating_an_opt_in_query_folds_the_archived_events()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();

        var party = await query.Events.QueryAllRawEvents()
            .Where(x => x.MaybeArchived() && x.StreamId == _archived)
            .AggregateToAsync<QuestParty>(token: Token);

        party.ShouldNotBeNull().Members.ShouldBe(["A"]);
    }

    [Fact]
    public async Task ordering_by_is_archived_is_not_an_opt_in()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var events = await query.Events.QueryAllRawEvents()
            .Where(x => x.StreamId == _archived || x.StreamId == _live)
            .OrderBy(x => x.IsArchived)
            .ToListAsync(Token);

        events.Single().StreamId.ShouldBe(_live);
    }

    [Fact]
    public async Task selecting_is_archived_is_not_an_opt_in()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var flags = await query.Events.QueryAllRawEvents()
            .Where(x => x.StreamId == _archived || x.StreamId == _live)
            .Select(x => x.IsArchived)
            .ToListAsync(Token);

        flags.ShouldBe([false]);
    }

    [Fact]
    public async Task the_flag_of_an_event_captured_from_outside_the_query_is_not_an_opt_in()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var captured = await query.Events.QueryAllRawEvents()
            .FirstAsync(x => x.IsArchived && x.StreamId == _archived, Token);

        (await query.Events.QueryAllRawEvents()
            .Where(x => captured.IsArchived && x.StreamId == _archived)
            .ToListAsync(Token)).ShouldBeEmpty();

        (await query.Events.QueryAllRawEvents()
            .Where(x => captured.MaybeArchived() && x.StreamId == _archived)
            .ToListAsync(Token)).ShouldBeEmpty();
    }

    [Fact]
    public async Task an_event_in_a_nested_lambda_is_not_the_event_being_filtered()
    {
        await ArchiveOneStreamBesideALiveOne();

        await using var query = theStore.QuerySession();
        var captured = await query.Events.QueryAllRawEvents()
            .Where(x => x.IsArchived && x.StreamId == _archived)
            .ToListAsync(Token);

        // These lambdas run over the list in memory, and the row's own filter is still in force
        (await query.Events.QueryAllRawEvents()
            .Where(x => captured.Where(e => e.IsArchived).Select(e => e.Id).Contains(x.Id))
            .ToListAsync(Token)).ShouldBeEmpty();

        (await query.Events.QueryAllRawEvents()
            .Where(x => captured.Where(e => e.MaybeArchived()).Select(e => e.Id).Contains(x.Id))
            .ToListAsync(Token)).ShouldBeEmpty();
    }

    [Fact]
    public async Task an_is_archived_member_on_an_event_body_says_nothing_about_the_row()
    {
        var reference = Guid.NewGuid().ToString();
        theSession.Events.StartStream(_archived, new CaseFiled(reference, true));
        theSession.Events.StartStream(_live, new CaseFiled(reference, true));
        await theSession.SaveChangesAsync(Token);

        await using var session = theStore.LightweightSession();
        session.Events.ArchiveStream(_archived);
        await session.SaveChangesAsync(Token);

        await using var query = theStore.QuerySession();

        // Only the live stream's body answers: the row filter is still on, and false matches neither.
        (await Reader.QueryEventData<CaseFiled>()
            .Where(x => x.IsArchived && x.Reference == reference)
            .ToListAsync(Token)).Count.ShouldBe(1);

        (await query.Events.QueryRawEventDataOnly<CaseFiled>()
            .Where(x => x.IsArchived && x.Reference == reference)
            .ToListAsync(Token)).Count.ShouldBe(1);

        (await Reader.QueryEventData<CaseFiled>()
            .Where(x => !x.IsArchived && x.Reference == reference)
            .ToListAsync(Token)).ShouldBeEmpty();
    }

    [Fact]
    public async Task stream_states_answer_for_archived_streams_with_no_opt_in_at_all()
    {
        await ArchiveOneStreamBesideALiveOne();

        var streams = Reader.QueryStreamStates().Where(x => x.Id == _archived || x.Id == _live);

        (await streams.ToListAsync(Token)).Count.ShouldBe(2);
        (await streams.Where(x => x.IsArchived).ToListAsync(Token)).Single().Id.ShouldBe(_archived);
        (await streams.Where(x => !x.IsArchived).ToListAsync(Token)).Single().Id.ShouldBe(_live);
    }
}

/// <summary>Dropping the archive filter must leave the tenant filter where it was.</summary>
public class archived_events_opt_in_under_conjoined_tenancy_tests : IAsyncLifetime
{
    private const string Schema = "archived_opt_in_conjoined";

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync() => await TestSchema.DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    public async Task an_opt_in_stays_inside_the_tenant()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });

        var streamId = Guid.NewGuid();

        foreach (var tenantId in new[] { "Red", "Blue" })
        {
            await using var session = store.LightweightSession(new SessionOptions { TenantId = tenantId });
            session.Events.StartStream(streamId, new QuestStarted(tenantId));
            await session.SaveChangesAsync(Token);

            session.Events.ArchiveStream(streamId);
            await session.SaveChangesAsync(Token);
        }

        await using var query = store.QuerySession(new SessionOptions { TenantId = "Red" });

        (await query.Events.QueryAllRawEvents().Where(x => x.IsArchived).ToListAsync(Token))
            .Single().TenantId.ShouldBe("Red");

        (await query.Events.QueryAllRawEvents().Where(x => x.MaybeArchived()).ToListAsync(Token))
            .Single().TenantId.ShouldBe("Red");
    }
}
