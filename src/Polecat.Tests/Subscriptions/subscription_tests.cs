using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Subscriptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Subscriptions;

[Collection("integration")]
public class subscription_tests : IntegrationContext
{
    public subscription_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStoreWithSubscription<T>()
        where T : ISubscription, new()
    {
        await StoreOptions(opts =>
        {
            opts.Projections.Subscribe<T>();
        });
        return theStore;
    }

    [Fact]
    public async Task subscription_receives_events_via_daemon()
    {
        var store = await CreateStoreWithSubscription<RecordingSubscription>();
        RecordingSubscription.Reset();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Subscription Quest"),
            new MembersJoined(1, "Town", ["Hero"]));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await store.WaitForProjectionAsync();

        RecordingSubscription.ProcessedEvents.ShouldNotBeEmpty();
        RecordingSubscription.ProcessedEvents.Count.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task subscription_tracks_progress()
    {
        var store = await CreateStoreWithSubscription<RecordingSubscription>();
        RecordingSubscription.Reset();

        var streamId = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(streamId,
            new QuestStarted("Progress Quest"),
            new MembersJoined(1, "Village", ["Scout"]));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await store.WaitForProjectionAsync();

        // Verify the subscription actually received events (primary concern)
        RecordingSubscription.ProcessedEvents.ShouldNotBeEmpty();

        // Verify progress entries exist — the HighWaterMark at minimum
        var allProgress = await store.Database.AllProjectionProgress(CancellationToken.None);
        allProgress.ShouldNotBeEmpty();
    }

    [Fact]
    public async Task subscription_processes_multiple_pages()
    {
        var store = await CreateStoreWithSubscription<RecordingSubscription>();
        RecordingSubscription.Reset();

        // Insert events in two separate batches
        for (var i = 0; i < 2; i++)
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(),
                new QuestStarted($"Quest {i}"),
                new MembersJoined(1, $"Location {i}", [$"Hero {i}"]));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await store.WaitForProjectionAsync();

        // Should have received events from both batches (at least 4 events total)
        RecordingSubscription.ProcessedEvents.Count.ShouldBeGreaterThanOrEqualTo(4);
    }

    [Fact]
    public async Task subscription_registration_creates_shard()
    {
        var store = await CreateStoreWithSubscription<RecordingSubscription>();

        var shards = store.Options.Projections.AllShards();
        shards.Count.ShouldBeGreaterThan(0);
        shards.Any(s => s.Name.Identity.Contains("RecordingSubscription")).ShouldBeTrue();
    }

    [Fact]
    public async Task subscription_wrapper_works_for_raw_interface()
    {
        // Test that a non-SubscriptionBase ISubscription also works
        await StoreOptions(opts =>
        {
            opts.Projections.Subscribe(new RawSubscription());
        });

        RawSubscription.Reset();

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, new QuestStarted("Raw Quest"));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await theStore.WaitForProjectionAsync();

        RawSubscription.ProcessedCount.ShouldBeGreaterThan(0);
    }

    /// <summary>
    ///     #550 at the daemon level. The loader pushes a subscription's event-type allow-list into SQL,
    ///     so rows of other types never leave SQL Server. Two facts this pins that the loader tests
    ///     cannot: events of non-allowed types do not reach the subscription at all, and the shard's
    ///     recorded progress still advances to the head across a run of filtered-out events longer than
    ///     its batch size. Get the ceiling accounting wrong under a server-side filter and this either
    ///     stalls short of the head or — as the client-side filter did — steps over the one event the
    ///     subscription actually wanted.
    /// </summary>
    [Fact]
    public async Task a_filtered_subscription_sees_only_its_types_and_still_reaches_the_head()
    {
        await StoreOptions(opts =>
        {
            // Its own schema, so the store is guaranteed empty: this test asserts absolute counts and
            // an absolute progression, and the default `dbo` is shared with — and seeded by — every
            // other class in the "integration" collection.
            opts.DatabaseSchemaName = "filtered_subscription";

            // A batch smaller than the run of filtered-out events below, so a page can be filled
            // entirely with rows the subscription does not want.
            opts.Projections.Subscribe<FilteredRecordingSubscription>(x => x.Options.BatchSize = 3);
        });

        FilteredRecordingSubscription.Reset();

        await using (var session = theStore.LightweightSession())
        {
            // The run the subscription filters out, appended BEFORE the one event it wants — so the
            // first page is nothing but discards and the loader has to scan past them to find it.
            for (var i = 0; i < 10; i++)
            {
                session.Events.StartStream(Guid.NewGuid(), new MembersJoined(i + 1, $"Town {i}", [$"Member {i}"]));
            }

            session.Events.StartStream(Guid.NewGuid(), new QuestStarted("Beyond the run"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await theStore.WaitForProjectionAsync();

        var seen = FilteredRecordingSubscription.ProcessedEvents;
        seen.Count.ShouldBe(1);
        seen.Single().ShouldBeOfType<QuestStarted>();

        var shard = theStore.Options.Projections.AllShards()
            .Single(x => x.Name.Identity.Contains(nameof(FilteredRecordingSubscription)));

        var progress = await theStore.Database.ProjectionProgressFor(shard.Name, CancellationToken.None);
        progress.ShouldBe(await theStore.Database.FetchHighestEventSequenceNumber(CancellationToken.None));
    }
}

/// <summary>
///     Test subscription that records all events it receives.
/// </summary>
public class RecordingSubscription : SubscriptionBase
{
    private static readonly List<object> _events = new();
    private static readonly object _lock = new();

    public static IReadOnlyList<object> ProcessedEvents
    {
        get
        {
            lock (_lock) return _events.ToList();
        }
    }

    public static void Reset()
    {
        lock (_lock) _events.Clear();
    }

    public override Task<IChangeListener> ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            foreach (var @event in page.Events)
            {
                _events.Add(@event.Data);
            }
        }

        return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
    }
}

/// <summary>
///     #550: a subscription that names the one event type it wants, so the daemon's loader has an
///     allow-list to push into SQL.
/// </summary>
public class FilteredRecordingSubscription : SubscriptionBase
{
    private static readonly List<object> _events = new();
    private static readonly object _lock = new();

    public FilteredRecordingSubscription()
    {
        IncludeType<QuestStarted>();
    }

    public static IReadOnlyList<object> ProcessedEvents
    {
        get
        {
            lock (_lock) return _events.ToList();
        }
    }

    public static void Reset()
    {
        lock (_lock) _events.Clear();
    }

    public override Task<IChangeListener> ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            foreach (var @event in page.Events)
            {
                _events.Add(@event.Data);
            }
        }

        return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
    }
}

/// <summary>
///     A raw ISubscription implementation (not extending SubscriptionBase) for testing the wrapper.
/// </summary>
public class RawSubscription : ISubscription
{
    private static int _count;

    public static int ProcessedCount => _count;

    public static void Reset() => _count = 0;

    public Task<IChangeListener> ProcessEventsAsync(
        EventRange page,
        ISubscriptionController controller,
        IDocumentOperations operations,
        CancellationToken cancellationToken)
    {
        Interlocked.Add(ref _count, page.Events.Count);
        return Task.FromResult<IChangeListener>(NullChangeListener.Instance);
    }
}
