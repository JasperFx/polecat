using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Exceptions;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

public record CityFounded(string City);

public record CityVisited(string City);

public class CityLedger
{
    public string Id { get; set; } = string.Empty;
    public int Visits { get; set; }

    public static CityLedger Create(IEvent<CityFounded> e) => new() { Id = e.StreamKey! };
    public void Apply(CityVisited _) => Visits++;
}

public class CityVisits
{
    public string Id { get; set; } = string.Empty;
    public int Count { get; set; }
}

public class CityTrail
{
    public string Id { get; set; } = string.Empty;
    public int Visits { get; set; }
}

/// <summary>
///     Raises onto a stream named by the event, which nothing has looked at. Its own document is keyed by
///     the stream it's reading, so the raised key is the only one in this batch that can be lost.
/// </summary>
public partial class CityTrailProjection: SingleStreamProjection<CityTrail, string>
{
    public void Apply(CityTrail view, CityVisited _) => view.Visits++;

    public override ValueTask RaiseSideEffects(IDocumentSession session, IEventSlice<CityTrail> slice)
    {
        foreach (var visit in slice.Events().Select(x => x.Data).OfType<CityVisited>())
        {
            slice.AppendEvent(visit.City, new CityFounded(visit.City));
        }

        return ValueTask.CompletedTask;
    }
}

public partial class CityVisitsProjection: MultiStreamProjection<CityVisits, string>
{
    public CityVisitsProjection()
    {
        Identity<CityVisited>(x => x.City);
    }

    public void Apply(CityVisits view, CityVisited _) => view.Count++;
}

/// <summary>
///     #919's stream half. <c>pc_streams.id</c> is the same <c>varchar(250)</c> as a document key, so a second
///     <c>StartStream</c> of an id the code page can't hold threw <c>ExistingStreamIdCollisionException</c> for
///     a stream that doesn't exist, and an <c>Append</c> landed on the stream it collapsed onto.
/// </summary>
public class string_stream_key_outside_code_page_tests: OneOffConfigurationsContext
{
    private const string Tokyo = "東京";
    private const string Osaka = "大阪";
    private const string LostInTheColumn = "??";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public override async ValueTask InitializeAsync()
    {
        ConfigureStore(opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        await base.InitializeAsync();
    }

    private async Task seedAsync(string key)
    {
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<CityLedger>(key, new CityFounded("seed"), new CityVisited("seed"));
        await session.SaveChangesAsync(Ct);
    }

    private async Task<long> versionOfAsync(string key)
    {
        await using var query = theStore.QuerySession();
        return (await query.Events.FetchStreamAsync(key, token: Ct)).Count;
    }

    // ---- writes ----

    [Fact]
    public async Task start_stream_is_refused_rather_than_colliding_with_a_stream_that_does_not_exist()
    {
        await using var first = theStore.LightweightSession();
        first.Events.StartStream<CityLedger>(Tokyo, new CityFounded("tokyo"));
        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => first.SaveChangesAsync(Ct));

        await using var second = theStore.LightweightSession();
        second.Events.StartStream<CityLedger>(Osaka, new CityFounded("osaka"));
        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => second.SaveChangesAsync(Ct));

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamStateAsync(LostInTheColumn, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task the_refusal_names_the_key_the_aggregate_and_the_way_out()
    {
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<CityLedger>(Tokyo, new CityFounded("tokyo"));

        var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        refused.Id.ShouldBe(Tokyo);
        refused.Type.ShouldBe(typeof(CityLedger));
        refused.CodePage.ShouldBe(1252);
        refused.Message.ShouldContain(Tokyo);
        refused.Message.ShouldContain(nameof(CityLedger));
        refused.Message.ShouldContain("StreamIdentity.AsGuid");
    }

    [Fact]
    public async Task a_stream_started_without_an_aggregate_type_is_refused_too()
    {
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(Tokyo, new CityFounded("tokyo"));

        var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        refused.Type.ShouldBeNull();
    }

    [Fact]
    public async Task append_is_refused_and_leaves_the_stream_it_would_have_landed_on()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        session.Events.Append(Tokyo, new CityVisited("tokyo"));
        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        (await versionOfAsync(LostInTheColumn)).ShouldBe(2);
    }

    [Fact]
    public async Task nothing_else_in_the_save_is_written()
    {
        await using var session = theStore.LightweightSession();
        session.Events.StartStream<CityLedger>("plain", new CityFounded("plain"));
        session.Store(new StringDoc { Id = "plain" });
        session.Events.StartStream<CityLedger>(Tokyo, new CityFounded("tokyo"));

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        (await versionOfAsync("plain")).ShouldBe(0);
        await using var query = theStore.QuerySession();
        (await query.LoadAsync<StringDoc>("plain", Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task a_key_within_the_code_page_works_as_it_always_did()
    {
        await seedAsync("café");

        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append("café", new CityVisited("café"));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync("café", token: Ct)).Count.ShouldBe(3);
        (await query.Events.FetchStreamStateAsync("café", Ct))!.Version.ShouldBe(3);
        (await query.Events.AggregateStreamAsync<CityLedger>("café", token: Ct))!.Visits.ShouldBe(2);
    }

    [Fact]
    public async Task fetch_for_writing_then_appending_is_refused_at_the_save()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<CityLedger>(Tokyo, Ct);
        stream.AppendOne(new CityVisited("tokyo"));

        await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));
        (await versionOfAsync(LostInTheColumn)).ShouldBe(2);
    }

    [Fact]
    public async Task a_stream_only_read_for_writing_does_not_fail_the_save()
    {
        await using var session = theStore.LightweightSession();
        await session.Events.FetchForWriting<CityLedger>(Tokyo, Ct);
        session.Store(new StringDoc { Id = "plain" });

        await session.SaveChangesAsync(Ct);
    }

    // ---- reads ----

    [Fact]
    public async Task fetch_stream_finds_nothing_rather_than_the_stream_it_collapses_onto()
    {
        await seedAsync(LostInTheColumn);

        await using var query = theStore.QuerySession();

        (await query.Events.FetchStreamAsync(Tokyo, token: Ct)).ShouldBeEmpty();
        (await query.Events.FetchStreamStateAsync(Tokyo, Ct)).ShouldBeNull();
        (await query.Events.AggregateStreamAsync<CityLedger>(Tokyo, token: Ct)).ShouldBeNull();
        (await query.Events.AggregateStreamToLastKnownAsync<CityLedger>(Tokyo, token: Ct)).ShouldBeNull();
        (await query.Events.FetchLatest<CityLedger>(Tokyo, Ct)).ShouldBeNull();

        (await query.Events.FetchStreamAsync(LostInTheColumn, token: Ct)).Count.ShouldBe(2);
    }

    [Fact]
    public async Task fetch_for_writing_sees_a_new_stream()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<CityLedger>(Tokyo, Ct);

        stream.Aggregate.ShouldBeNull();
        stream.CurrentVersion.ShouldBe(0);
    }

    [Fact]
    public async Task fetch_for_exclusive_writing_sees_a_new_stream_and_locks_nothing_of_anyone_elses()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForExclusiveWriting<CityLedger>(Tokyo, Ct);

        stream.Aggregate.ShouldBeNull();
        stream.CurrentVersion.ShouldBe(0);

        await using var other = theStore.LightweightSession();
        other.Events.Append(LostInTheColumn, new CityVisited("elsewhere"));
        await other.SaveChangesAsync(Ct);
    }

    [Fact]
    public async Task fetch_many_for_writing_sees_a_new_stream()
    {
        await seedAsync(LostInTheColumn);
        await seedAsync("plain");

        await using var session = theStore.LightweightSession();
        var streams = await session.Events.FetchManyForWriting<CityLedger>([Tokyo, "plain"], Ct);

        streams[0].Aggregate.ShouldBeNull();
        streams[0].CurrentVersion.ShouldBe(0);
        streams[1].Aggregate!.Visits.ShouldBe(1);
    }

    [Fact]
    public async Task a_batched_fetch_finds_nothing()
    {
        await seedAsync(LostInTheColumn);

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var state = batch.Events.FetchStreamState(Tokyo);
        var events = batch.Events.FetchStream(Tokyo);
        await batch.Execute(Ct);

        (await state).ShouldBeNull();
        (await events).ShouldBeEmpty();
    }

    [Fact]
    public async Task a_batched_fetch_still_finds_a_key_within_the_code_page()
    {
        await seedAsync("café");

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var state = batch.Events.FetchStreamState("café");
        var events = batch.Events.FetchStream("café");
        await batch.Execute(Ct);

        (await state)!.Version.ShouldBe(2);
        (await events).Count.ShouldBe(2);
    }

    [Fact]
    public async Task a_linq_comparison_on_the_key_finds_nothing()
    {
        await seedAsync(LostInTheColumn);

        await using var query = theStore.QuerySession();

        (await query.Events.QueryAllRawEvents().Where(x => x.StreamKey == Tokyo).ToListAsync(Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task an_optimistic_append_sees_no_stream()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        await Should.ThrowAsync<JasperFx.Events.NonExistentStreamException>(() =>
            session.Events.AppendOptimistic(Tokyo, Ct, new CityVisited("tokyo")));
    }

    [Fact]
    public async Task an_exclusive_append_sees_no_stream()
    {
        await seedAsync(LostInTheColumn);

        await using var session = theStore.LightweightSession();
        await Should.ThrowAsync<JasperFx.Events.NonExistentStreamException>(() =>
            session.Events.AppendExclusive(Tokyo, Ct, new CityVisited("tokyo")));
    }

    [Fact]
    public async Task the_event_store_explorer_finds_nothing()
    {
        await seedAsync(LostInTheColumn);

        var explorer = (IEventStore)theStore;

        (await explorer.GetStreamMetadataAsync(LostInTheColumn, Ct)).ShouldNotBeNull();
        (await explorer.GetStreamMetadataAsync(Tokyo, Ct)).ShouldBeNull();

        var events = 0;
        await foreach (var _ in explorer.ReadStreamAsync(Tokyo, Ct))
        {
            events++;
        }

        events.ShouldBe(0);
    }

    // ---- what a stream already holds ----

    [Fact]
    public async Task archiving_a_key_that_cannot_be_there_archives_nothing()
    {
        await seedAsync(LostInTheColumn);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.ArchiveStream(Tokyo);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamStateAsync(LostInTheColumn, Ct))!.IsArchived.ShouldBeFalse();
    }

    [Fact]
    public async Task tombstoning_a_key_that_cannot_be_there_removes_nothing()
    {
        await seedAsync(LostInTheColumn);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.TombstoneStream(Tokyo);
            await session.SaveChangesAsync(Ct);
        }

        (await versionOfAsync(LostInTheColumn)).ShouldBe(2);
    }

    // ---- projections ----

    [Fact]
    public async Task an_inline_projection_cannot_write_an_id_the_column_would_lose()
    {
        ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<CityVisitsProjection>(ProjectionLifecycle.Inline);
        });

        await using var session = theStore.LightweightSession();
        session.Events.StartStream("plain", new CityVisited(Tokyo));

        var refused = await Should.ThrowAsync<StringIdOutsideCodePageException>(() => session.SaveChangesAsync(Ct));

        refused.Id.ShouldBe(Tokyo);
        refused.Type.ShouldBe(typeof(CityVisits));

        // The events went in the same transaction
        (await versionOfAsync("plain")).ShouldBe(0);
    }

    [Fact]
    public async Task the_daemon_cannot_write_an_id_the_column_would_lose()
    {
        ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<CityVisitsProjection>(ProjectionLifecycle.Async);
        });

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream("plain", new CityVisited(Tokyo), new CityVisited(Osaka));
            await session.SaveChangesAsync(Ct);
        }

        using var daemon = await theStore.BuildProjectionDaemonAsync();

        var refused = await Should.ThrowAsync<Exception>(() =>
            daemon.RebuildProjectionAsync<CityVisitsProjection>(TimeSpan.FromSeconds(30), Ct));

        var cause = refused;
        while (cause is not StringIdOutsideCodePageException && cause.InnerException != null)
        {
            cause = cause.InnerException;
        }

        cause.ShouldBeOfType<StringIdOutsideCodePageException>();

        await using var query = theStore.QuerySession();
        (await query.Query<CityVisits>().ToListAsync(Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task the_daemon_cannot_raise_an_event_onto_a_key_the_column_would_lose()
    {
        ConfigureStore(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add(new CityTrailProjection(), ProjectionLifecycle.Async);
        });

        await seedAsync(LostInTheColumn);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream("plain", new CityVisited(Tokyo));
            await session.SaveChangesAsync(Ct);
        }

        using var daemon = await theStore.BuildProjectionDaemonAsync();

        var failure = await Should.ThrowAsync<Exception>(() => daemon.CatchUpAsync(TimeSpan.FromSeconds(30), Ct));

        var cause = failure;
        while (cause is not StringIdOutsideCodePageException && cause.InnerException != null)
        {
            cause = cause.InnerException;
        }

        cause.ShouldBeOfType<StringIdOutsideCodePageException>();

        // Without the refusal the raised event joins the stream its key collapses onto
        (await versionOfAsync(LostInTheColumn)).ShouldBe(2);
    }
}
