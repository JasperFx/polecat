using JasperFx;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Projections;
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
        await session.SaveChangesAsync();

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

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
        await session.SaveChangesAsync();

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);

        // Allow a moment for final progress writes to commit
        await Task.Delay(250);
        await daemon.StopAllAsync();

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
            await session.SaveChangesAsync();
        }

        using var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

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
        await session.SaveChangesAsync();

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
        await daemon.StopAllAsync();

        RawSubscription.ProcessedCount.ShouldBeGreaterThan(0);
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
