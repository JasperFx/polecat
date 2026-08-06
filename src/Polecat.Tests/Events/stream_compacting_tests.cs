using JasperFx.Events;
using JasperFx.Events.Protected;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Events;

[Collection("integration")]
public class stream_compacting_tests : IntegrationContext
{
    public stream_compacting_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task compact_stream_at_latest()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_latest");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Compact Quest"),
            new MembersJoined(1, "Town", ["Alice", "Bob"]),
            new MonsterSlain("Goblin", 10),
            new MembersJoined(2, "Forest", ["Charlie"]));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        await session2.Events.CompactStreamAsync<QuestParty>(streamId);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // After compaction, only one event should remain (the Compacted<T>)
        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);

        var compacted = events[0].Data.ShouldBeOfType<Compacted<QuestParty>>();
        compacted.Snapshot.ShouldNotBeNull();
        compacted.Snapshot.Name.ShouldBe("Compact Quest");
        compacted.Snapshot.Members.ShouldContain("Alice");
        compacted.Snapshot.Members.ShouldContain("Bob");
        compacted.Snapshot.Members.ShouldContain("Charlie");
        compacted.Snapshot.MonstersSlain.ShouldContain("Goblin");
    }

    [Fact]
    public async Task compact_stream_preserves_stream_version()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_version");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Version Quest"),
            new MembersJoined(1, "Town", ["Hero"]),
            new MonsterSlain("Dragon", 100));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        await session2.Events.CompactStreamAsync<QuestParty>(streamId);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Can still append events after compaction
        await using var session3 = theStore.LightweightSession();
        session3.Events.Append(streamId, new MembersJoined(3, "Castle", ["Knight"]));
        await session3.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(2); // Compacted + new event
    }

    [Fact]
    public async Task compact_stream_is_idempotent()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_idempotent");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Idempotent Quest"),
            new MembersJoined(1, "Town", ["Hero"]));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // First compaction
        await using var session2 = theStore.LightweightSession();
        await session2.Events.CompactStreamAsync<QuestParty>(streamId);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Second compaction should be a no-op (already compacted)
        await using var session3 = theStore.LightweightSession();
        await session3.Events.CompactStreamAsync<QuestParty>(streamId);
        await session3.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
        events.Count.ShouldBe(1);
        events[0].Data.ShouldBeOfType<Compacted<QuestParty>>();
    }

    [Fact]
    public async Task compact_empty_stream_is_noop()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_empty");

        var streamId = Guid.NewGuid();

        // No events for this stream, should not throw
        await using var session = theStore.LightweightSession();
        await session.Events.CompactStreamAsync<QuestParty>(streamId);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     Regression for #423, and the first coverage this hook has ever had here. The compactor
    ///     gated the archiving callback on IEventsArchiver&lt;IDocumentOperations&gt; alone.
    ///     IEventsArchiver&lt;T&gt; is INVARIANT, so an archiver closed over IDocumentSession -- the
    ///     operations type Polecat closes IEventStore&lt;,&gt; over, and therefore the one every
    ///     JasperFx-generic caller supplies -- never matched. The callback was skipped silently and
    ///     compaction went on to permanently delete the very events the archiver existed to copy
    ///     into cold storage first.
    /// </summary>
    [Fact]
    public async Task the_archiver_runs_before_the_events_are_deleted()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_archiver");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archived Quest"),
            new MembersJoined(1, "Town", ["Alice"]),
            new MonsterSlain("Goblin", 10));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var archiver = new RecordingArchiver();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.CompactStreamAsync<QuestParty>(streamId, x => x.Archiver = archiver);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        archiver.Calls.ShouldBe(1);
        archiver.Events.Count.ShouldBe(3);
        archiver.Events.Select(x => x.Version).ShouldBe(new long[] { 1, 2, 3 });
    }

    /// <summary>
    ///     The historical closure keeps working, so the #423 fix is not a breaking change for
    ///     anyone who hand-wrote an archiver against IDocumentOperations.
    /// </summary>
    [Fact]
    public async Task an_archiver_closed_over_the_operations_type_still_runs()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "compact_archiver_ops");

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId,
            new QuestStarted("Archived Quest"),
            new MembersJoined(1, "Town", ["Alice"]));
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var archiver = new RecordingOperationsArchiver();

        await using var session2 = theStore.LightweightSession();
        await session2.Events.CompactStreamAsync<QuestParty>(streamId, x => x.Archiver = archiver);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        archiver.Calls.ShouldBe(1);
        archiver.Events.Count.ShouldBe(2);
    }

    private sealed class RecordingArchiver: IEventsArchiver<IDocumentSession>
    {
        public int Calls { get; private set; }
        public IReadOnlyList<IEvent> Events { get; private set; } = [];

        public Task MaybeArchiveAsync<T>(IDocumentSession operations, StreamCompactingRequest<T> request,
            IReadOnlyList<IEvent> events, CancellationToken cancellation) where T : class
        {
            Calls++;
            Events = events;
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingOperationsArchiver: IEventsArchiver<IDocumentOperations>
    {
        public int Calls { get; private set; }
        public IReadOnlyList<IEvent> Events { get; private set; } = [];

        public Task MaybeArchiveAsync<T>(IDocumentOperations operations, StreamCompactingRequest<T> request,
            IReadOnlyList<IEvent> events, CancellationToken cancellation) where T : class
        {
            Calls++;
            Events = events;
            return Task.CompletedTask;
        }
    }
}
