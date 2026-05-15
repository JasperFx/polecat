using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Storage;

namespace Polecat.Tests.Daemon;

/// <summary>
///     Coverage for the Polecat-owned augmenter that populates
///     <see cref="ShardState.SkippedEventsCount"/> on Skipped publications.
///     The framework's <c>HighWaterAgent</c> owns the publish path via
///     <see cref="ShardStateTracker.MarkSkippingAsync"/>, but doesn't set the
///     count — Polecat fills it in via an observer subscribed on the tracker
///     in <see cref="PolecatDatabase"/>'s constructor.
/// </summary>
public class skipped_events_count_augmenter_tests
{
    [Fact]
    public async Task augments_skipped_publications_with_most_recent_count()
    {
        await using var store = BuildStore();
        var tracker = store.Database.Tracker;

        var observed = await ObserveNextAsync(tracker,
            t => t.MarkSkippingAsync(lastKnownGoodHighWaterMark: 100, newHighWaterMark: 250));

        observed.Action.ShouldBe(ShardAction.Skipped);
        observed.PreviousGoodMark.ShouldBe(100);
        observed.Sequence.ShouldBe(250);
        observed.SkippedEventsCount.ShouldBe(150);
    }

    [Fact]
    public async Task leaves_non_skipped_publications_alone()
    {
        await using var store = BuildStore();
        var tracker = store.Database.Tracker;

        var observed = await ObserveNextAsync(tracker, t => t.MarkHighWaterAsync(42));

        observed.Action.ShouldNotBe(ShardAction.Skipped);
        observed.SkippedEventsCount.ShouldBeNull();
    }

    [Fact]
    public async Task preserves_explicitly_set_skipped_events_count()
    {
        // If a future code path populates SkippedEventsCount explicitly
        // (e.g. cumulative semantics from a richer detector), the augmenter
        // must not overwrite it.
        await using var store = BuildStore();
        var tracker = store.Database.Tracker;

        var observed = await ObserveNextAsync(tracker, t => t.PublishAsync(
            new ShardState(ShardState.HighWaterMark, 250)
            {
                Action = ShardAction.Skipped,
                PreviousGoodMark = 100,
                SkippedEventsCount = 999
            }));

        observed.SkippedEventsCount.ShouldBe(999);
    }

    private static async Task<ShardState> ObserveNextAsync(
        ShardStateTracker tracker, Func<ShardStateTracker, ValueTask> publish)
    {
        var tcs = new TaskCompletionSource<ShardState>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = tracker.Subscribe(new CapturingObserver(tcs));
        await publish(tracker);
        // The tracker dispatches synchronously through a Block; await briefly
        // to give the publish task time to propagate to the subscriber.
        var winner = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        winner.ShouldBe(tcs.Task);
        return await tcs.Task;
    }

    private static DocumentStore BuildStore()
    {
        return new DocumentStore(new StoreOptions
        {
            ConnectionString =
                "Server=localhost;Database=skip_augmenter;Integrated Security=true;TrustServerCertificate=true",
            AutoCreateSchemaObjects = JasperFx.AutoCreate.None
        });
    }

    private sealed class CapturingObserver : IObserver<ShardState>
    {
        private readonly TaskCompletionSource<ShardState> _tcs;
        public CapturingObserver(TaskCompletionSource<ShardState> tcs) => _tcs = tcs;
        public void OnCompleted() { }
        public void OnError(Exception error) => _tcs.TrySetException(error);
        public void OnNext(ShardState value) => _tcs.TrySetResult(value);
    }
}
