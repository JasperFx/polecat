using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Daemon;
using Polecat.Exceptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

[Collection("integration")]
public class event_loader_tests : IntegrationContext
{
    public event_loader_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        // Clean slate for each test
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DELETE FROM [dbo].[pc_events];
            DELETE FROM [dbo].[pc_streams];
            DELETE FROM [dbo].[pc_event_progression];
            """;
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task load_returns_correct_page()
    {
        await InsertEventsAsync(5);
        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        page.Count.ShouldBe(5);
        page.Floor.ShouldBe(0);
    }

    [Fact]
    public async Task load_respects_batch_size()
    {
        await InsertEventsAsync(10);
        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 3);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        page.Count.ShouldBe(3);
    }

    [Fact]
    public async Task load_respects_floor_and_ceiling()
    {
        await InsertEventsAsync(10);

        // Get the actual seq_ids so we can set floor/ceiling precisely
        var allSeqIds = await GetAllSeqIdsAsync();
        allSeqIds.Count.ShouldBe(10);

        // Load only events from seq_id 3 to 7 (exclusive floor, inclusive ceiling)
        var loader = CreateLoader();
        var request = CreateRequest(allSeqIds[2], allSeqIds[6], batchSize: 100);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        // Should get events at positions 3, 4, 5, 6 (4 events from seq 4 to 7 inclusive)
        page.Count.ShouldBe(4);

        // All returned events should have seq_id > floor and <= ceiling
        foreach (var e in page)
        {
            e.Sequence.ShouldBeGreaterThan(allSeqIds[2]);
            e.Sequence.ShouldBeLessThanOrEqualTo(allSeqIds[6]);
        }
    }

    [Fact]
    public async Task load_calculates_ceiling()
    {
        await InsertEventsAsync(5);
        var highWater = await GetHighestSeqIdAsync();

        var loader = CreateLoader();
        var request = CreateRequest(0, highWater, batchSize: 100);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        // When all events are loaded and count < batchSize, ceiling == highWater
        page.Ceiling.ShouldBe(highWater);
    }

    [Fact]
    public async Task load_empty_range_returns_empty_page()
    {
        // No events inserted
        var loader = CreateLoader();
        var request = CreateRequest(0, 100, batchSize: 100);
        var page = await loader.LoadAsync(request, CancellationToken.None);

        page.Count.ShouldBe(0);
    }

    // #368 / jasperfx#565: the daemon classifies a paused shard purely from what the store's exception
    // declares through IEventFailureContext — there is deliberately no fallback type-name sniffing. These
    // two pin the throw sites that used to raise a bare InvalidOperationException, which classified as
    // ShardFailureCategory.Other with no event details at all.
    [Fact]
    public async Task unresolvable_event_type_throws_a_classified_failure_naming_the_sequence()
    {
        await InsertEventsAsync(1);
        var seqId = (await GetAllSeqIdsAsync()).Single();
        await CorruptDotNetTypeAsync(seqId, "Nope.NotARealEventType, Nope");

        var loader = CreateLoader();
        var request = CreateRequest(0, seqId, batchSize: 100);

        var ex = await Should.ThrowAsync<UnknownEventTypeException>(
            async () => await loader.LoadAsync(request, CancellationToken.None));

        ShardFailure.For(ex, DateTimeOffset.UtcNow).Category.ShouldBe(ShardFailureCategory.UnknownEventType);
        ex.Sequence.ShouldBe(seqId);
    }

    [Fact]
    public async Task corrupted_event_body_throws_a_classified_failure_naming_the_event_type()
    {
        await InsertEventsAsync(1);
        var seqId = (await GetAllSeqIdsAsync()).Single();
        var alias = theStore.Database.Events.EventMappingFor(typeof(QuestStarted)).EventTypeName;
        await CorruptEventBodyAsync(seqId);

        var loader = CreateLoader();
        var request = CreateRequest(0, seqId, batchSize: 100);

        var ex = await Should.ThrowAsync<EventDeserializationFailureException>(
            async () => await loader.LoadAsync(request, CancellationToken.None));

        // The acceptance case from the issue: EventSerialization, with the failing sequence AND the
        // store's type alias — the alias a consumer can act on, not the assembly-qualified dotnet_type.
        var failure = ShardFailure.For(ex, DateTimeOffset.UtcNow);
        failure.Category.ShouldBe(ShardFailureCategory.EventSerialization);
        failure.Event.ShouldNotBeNull();
        failure.Event.Sequence.ShouldBe(seqId);
        failure.Event.EventTypeName.ShouldBe(alias);
    }

    // ---- #550: the event-type allow-list is applied in SQL, not after hydration ----

    /// <summary>
    ///     The discriminating fact for pushing the filter into SQL. The run of non-matching events is
    ///     longer than the batch size, so <c>TOP(@batchSize)</c> used to fill the page entirely with rows
    ///     the client-side filter then discarded: the page came back empty, failed
    ///     <c>CalculateCeiling</c>'s "did not fill the batch" test, and claimed the high-water mark as its
    ///     ceiling after looking at only the first three rows of the range. The <c>QuestStarted</c> above
    ///     them was stepped over and never delivered — a silently skipped event, not merely a slow one.
    ///     With the filter in SQL the query scans the whole range for matches, finds it, and the ceiling
    ///     it claims is honest.
    /// </summary>
    [Fact]
    public async Task a_matching_event_beyond_a_run_of_filtered_events_is_still_delivered()
    {
        await AppendAsync(Enumerable.Range(0, 10)
            .Select(object (i) => new MembersJoined(i + 1, $"Town {i}", [$"Member {i}"]))
            .ToArray());
        await AppendAsync(new QuestStarted("Beyond the run"));

        var highWater = await GetHighestSeqIdAsync();

        var page = await CreateFilteredLoader()
            .LoadAsync(CreateRequest(0, highWater, batchSize: 3), TestContext.Current.CancellationToken);

        page.Count.ShouldBe(1);
        page.Single().Data.ShouldBeOfType<QuestStarted>();

        // Everything up to the high-water mark was scanned for matches, so the whole non-matching run is
        // behind the floor after a single page and the shard cannot stall on it.
        page.Ceiling.ShouldBe(highWater);
    }

    /// <summary>
    ///     The other half of the ceiling contract under a SQL-side filter: a page that fills its batch
    ///     with matching events claims only as far as its last row, and the following request picks up
    ///     from there and finishes the range.
    /// </summary>
    [Fact]
    public async Task a_full_page_of_matching_events_reports_the_last_matched_sequence()
    {
        await AppendAsync(
            new QuestStarted("one"), new MembersJoined(1, "A", ["a"]),
            new QuestStarted("two"), new MembersJoined(2, "B", ["b"]),
            new QuestStarted("three"), new MembersJoined(3, "C", ["c"]));

        var seqIds = await GetAllSeqIdsAsync();
        var highWater = seqIds.Last();

        var page = await CreateFilteredLoader()
            .LoadAsync(CreateRequest(0, highWater, batchSize: 2), TestContext.Current.CancellationToken);

        page.Select(x => x.Sequence).ShouldBe([seqIds[0], seqIds[2]]);
        page.Ceiling.ShouldBe(seqIds[2]);

        var next = await CreateFilteredLoader()
            .LoadAsync(CreateRequest(page.Ceiling, highWater, batchSize: 2),
                TestContext.Current.CancellationToken);

        next.Select(x => x.Sequence).ShouldBe([seqIds[4]]);
        next.Ceiling.ShouldBe(highWater);
    }

    /// <summary>
    ///     A regression guard on the shape of the pushed-down predicate. A row whose <c>dotnet_type</c>
    ///     does not resolve fails a shard that wants it (the unfiltered control below), and must stay
    ///     excluded for a shard that does not — the SQL filter has to reject an unrecognised
    ///     <c>dotnet_type</c>, not wave it through. This already held before the push-down, because the
    ///     client-side check also ran ahead of type resolution, so it does <em>not</em> discriminate the
    ///     change; it pins behaviour the new <c>IN</c> clause could quietly break.
    /// </summary>
    [Fact]
    public async Task a_filtered_load_still_excludes_an_unresolvable_event_of_another_type()
    {
        await AppendAsync(new QuestStarted("Keep me"), new MembersJoined(1, "Town", ["Discard me"]));

        var seqIds = await GetAllSeqIdsAsync();
        var highWater = seqIds.Last();
        await CorruptDotNetTypeAsync(highWater, "Nope.NotARealEventType, Nope");

        await Should.ThrowAsync<UnknownEventTypeException>(async () => await CreateLoader()
            .LoadAsync(CreateRequest(0, highWater, batchSize: 100), TestContext.Current.CancellationToken));

        var page = await CreateFilteredLoader()
            .LoadAsync(CreateRequest(0, highWater, batchSize: 100), TestContext.Current.CancellationToken);

        page.Count.ShouldBe(1);
        page.Single().Data.ShouldBeOfType<QuestStarted>();
        page.Ceiling.ShouldBe(highWater);
    }

    /// <summary>
    ///     The <c>dotnet_type is null</c> disjunct in the pushed-down filter, which exists to preserve
    ///     behaviour exactly. The client-side check only ever excluded a row that <em>had</em> a
    ///     dotnet_type, so a null one reached hydration and failed there. Drop the disjunct and a
    ///     filtered shard would instead step over such a row in silence — a different, worse answer than
    ///     the loud one this store has always given.
    /// </summary>
    [Fact]
    public async Task a_null_dotnet_type_still_reaches_hydration_under_a_filtered_load()
    {
        await AppendAsync(new QuestStarted("Keep me"), new MembersJoined(1, "Town", ["No type at all"]));

        var seqIds = await GetAllSeqIdsAsync();
        var highWater = seqIds.Last();
        await CorruptDotNetTypeAsync(highWater, null);

        await Should.ThrowAsync<UnknownEventTypeException>(async () => await CreateFilteredLoader()
            .LoadAsync(CreateRequest(0, highWater, batchSize: 100), TestContext.Current.CancellationToken));
    }

    private PolecatEventLoader CreateFilteredLoader()
    {
        var filtering = new EventFilterable();
        filtering.IncludeType<QuestStarted>();

        return new PolecatEventLoader(theStore.Database.Events, theStore.Options,
            theStore.Options.ConnectionString, filtering);
    }

    private async Task AppendAsync(params object[] events)
    {
        theSession.Events.StartStream(Guid.NewGuid(), events);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private async Task CorruptDotNetTypeAsync(long seqId, string? dotNetType)
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE [dbo].[pc_events] SET dotnet_type = @type WHERE seq_id = @seq;";
        cmd.Parameters.AddWithValue("@type", (object?)dotNetType ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@seq", seqId);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task CorruptEventBodyAsync(long seqId)
    {
        // Well-formed JSON (so the native `json` column accepts it) that cannot bind to the event type at
        // all, so the row reads back fine but STJ throws on materialization — the shape of the failure the
        // issue is about. A JSON array rather than a bad property value: property NAMES are subject to the
        // serializer's naming policy, so an unmatched name would silently bind to the default instead.
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE [dbo].[pc_events] SET data = @data WHERE seq_id = @seq;";
        cmd.Parameters.AddWithValue("@data", "[1, 2, 3]");
        cmd.Parameters.AddWithValue("@seq", seqId);
        (await cmd.ExecuteNonQueryAsync()).ShouldBe(1);
    }

    private PolecatEventLoader CreateLoader()
    {
        return new PolecatEventLoader(theStore.Database.Events, theStore.Options, theStore.Options.ConnectionString);
    }

    private static EventRequest CreateRequest(long floor, long highWater, int batchSize)
    {
        return new EventRequest
        {
            Floor = floor,
            HighWater = highWater,
            BatchSize = batchSize,
            Name = new ShardName("TestLoader"),
            ErrorOptions = new ErrorHandlingOptions(),
            Runtime = null!,
            Metrics = null!
        };
    }

    private async Task InsertEventsAsync(int count)
    {
        for (var i = 0; i < count; i++)
        {
            var streamId = Guid.NewGuid();
            theSession.Events.StartStream(streamId, new QuestStarted($"Quest {i + 1}"));
            await theSession.SaveChangesAsync();
        }
    }

    private async Task<long> GetHighestSeqIdAsync()
    {
        return await theStore.Database.FetchHighestEventSequenceNumber(CancellationToken.None);
    }

    private async Task<List<long>> GetAllSeqIdsAsync()
    {
        var seqIds = new List<long>();
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT seq_id FROM [dbo].[pc_events] ORDER BY seq_id;";
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            seqIds.Add(reader.GetInt64(0));
        }

        return seqIds;
    }
}
