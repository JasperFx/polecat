using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Core;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

public record RestartableIncrement(int Index);

public class RestartableTally
{
    public Guid Id { get; set; }
    public int Count { get; set; }
}

/// <summary>
///     Throws while folding the event with index <see cref="FailOnIndex" />, for as long as <see cref="Failing" /> is
///     on, and counts every successful application per event index so a replay over already-built documents shows up.
///     The state is static and private to this one test class, which has a single fact.
/// </summary>
public partial class RestartableTallyProjection : SingleStreamProjection<RestartableTally, Guid>
{
    public static volatile bool Failing;
    public static int FailOnIndex;
    public static readonly ConcurrentDictionary<int, int> Applications = new();

    public RestartableTallyProjection()
    {
        // Small pages, so several of them commit before the failing one
        Options.BatchSize = 10;
    }

    public void Apply(RestartableIncrement @event, RestartableTally tally)
    {
        if (Failing && @event.Index == FailOnIndex)
        {
            throw new DivideByZeroException($"Boom on event {@event.Index}");
        }

        tally.Count++;
        Applications.AddOrUpdate(@event.Index, 1, (_, count) => count + 1);
    }
}

/// <summary>
///     #768 (JasperFx/jasperfx#1015, wolverine#4569): a projection shard started in Rebuild mode with
///     <see cref="IProjectionDaemon.StartAgentInRebuildModeAsync" /> that fails part of the way through can be stopped
///     and started again, and the second run finishes the rebuild from the progress the first run committed, instead
///     of replaying from the beginning over the documents the first run already built.
///     <para>
///     Uses extended progression tracking and nothing else: no schema change beyond the columns that feature already
///     adds to <c>pc_event_progression</c>. Before jasperfx#1015, every position strategy answered Rebuild with a floor
///     of 0 and a rewind of the progression row, so the second start deleted the row and folded every event again on
///     top of the existing documents.
///     </para>
/// </summary>
public class rebuild_mode_start_resumes_after_a_failure : OneOffConfigurationsContext
{
    private const int NumberOfEvents = 100;
    private static readonly ShardName TheShard = new("RestartableTally");

    public rebuild_mode_start_resumes_after_a_failure()
    {
        RestartableTallyProjection.Applications.Clear();
        RestartableTallyProjection.Failing = false;
        RestartableTallyProjection.FailOnIndex = 60;
    }

    public override async ValueTask DisposeAsync()
    {
        RestartableTallyProjection.Failing = false;
        await base.DisposeAsync();
    }

    // One event per stream: a document that was folded twice has a Count of 2
    private async Task appendAsync(int from, int count)
    {
        await using var session = theStore.LightweightSession();
        for (var index = from; index < from + count; index++)
        {
            session.Events.StartStream<RestartableTally>(Guid.NewGuid(), new RestartableIncrement(index));
        }

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private async Task<(long LastSeqId, string? AgentStatus, string? FailureCategory)> readProgressionAsync()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT last_seq_id, agent_status, failure_category FROM {theDatabase.Events.ProgressionTableName} WHERE name = @name;";
        cmd.Parameters.AddWithValue("@name", TheShard.Identity);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return (0, null, null);

        string? text(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
        return (reader.GetInt64(0), text(1), text(2));
    }

    private Task<long> highestSequenceAsync() =>
        theDatabase.FetchHighestEventSequenceNumber(TestContext.Current.CancellationToken);

    private async Task waitUntilAsync(Func<Task<bool>> condition, string description)
    {
        var deadline = DateTimeOffset.UtcNow.Add(30.Seconds());
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(100);
        }

        throw new TimeoutException($"Timed out waiting until {description}; progression row is {await readProgressionAsync()}");
    }

    [Fact]
    public async Task a_failed_rebuild_mode_run_can_be_restarted_and_finished_from_its_committed_progress()
    {
        ConfigureStore(opts =>
        {
            opts.Events.EnableExtendedProgressionTracking = true;
            opts.Projections.Add(new RestartableTallyProjection(), ProjectionLifecycle.Async);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
        await appendAsync(1, NumberOfEvents);

        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();

        // First run: fails on event 60
        RestartableTallyProjection.Failing = true;
        var paused = new ShardActionWatcher(daemon.Tracker, TheShard.Identity, ShardAction.Paused, 30.Seconds());

        var first = await daemon.StartAgentInRebuildModeAsync(TheShard, TestContext.Current.CancellationToken);
        first.Mode.ShouldBe(ShardExecutionMode.Rebuild);

        await paused.Task;

        // The progress the first run committed, and the failure, both recorded through extended progression
        // tracking. The extended columns are written by the progression writer on its own flush, a moment after
        // the pause, so poll for them
        await waitUntilAsync(async () => (await readProgressionAsync()).FailureCategory != null,
            "extended progression tracking records the failure");
        var afterFailure = await readProgressionAsync();
        afterFailure.LastSeqId.ShouldBeGreaterThan(0);
        afterFailure.LastSeqId.ShouldBeLessThan(await highestSequenceAsync());
        afterFailure.FailureCategory.ShouldNotBeNull();

        // Pages before the failing one are already folded into documents
        RestartableTallyProjection.Applications.Keys.Where(i => i < 60).ShouldNotBeEmpty();

        // Stop, clear the fault, and start the rebuild-mode run again
        await daemon.StopAgentAsync(TheShard);
        RestartableTallyProjection.Failing = false;

        var second = await daemon.StartAgentInRebuildModeAsync(TheShard, TestContext.Current.CancellationToken);
        second.Mode.ShouldBe(ShardExecutionMode.Rebuild);

        var highest = await highestSequenceAsync();
        await waitUntilAsync(async () => (await readProgressionAsync()).LastSeqId >= highest,
            $"the shard reaches the high-water mark {highest}");

        // Finished: every event folded into its document exactly once. A replay from the floor over the documents
        // the first run built would leave every committed stream with a Count of 2
        await using (var session = theStore.QuerySession())
        {
            var tallies = await session.Query<RestartableTally>().ToListAsync(TestContext.Current.CancellationToken);
            tallies.Count.ShouldBe(NumberOfEvents);
            var histogram = tallies.GroupBy(x => x.Count).OrderBy(x => x.Key)
                .Select(x => $"{x.Count()} document(s) with Count == {x.Key}");
            tallies.Count(x => x.Count != 1).ShouldBe(0, string.Join(", ", histogram));
        }

        // The restart's Started publication clears the failure columns, on the progression writer's own flush
        // just like the write that set them, so this polls too (a single read here fails about one run in three)
        await waitUntilAsync(async () => (await readProgressionAsync()).FailureCategory == null,
            "the restarted shard clears the recorded failure");

        // No event at or below the first run's committed mark was folded again by the second run
        await using (var session = theStore.QuerySession())
        {
            var committed = await session.Events.QueryAllRawEvents()
                .Where(x => x.Sequence <= afterFailure.LastSeqId)
                .ToListAsync(TestContext.Current.CancellationToken);

            committed.ShouldNotBeEmpty();
            foreach (var e in committed)
            {
                var index = ((RestartableIncrement)e.Data).Index;
                RestartableTallyProjection.Applications[index].ShouldBe(1, $"event {index} at sequence {e.Sequence}");
            }
        }

        // And the restarted run keeps following the store past where the rebuild started
        await appendAsync(NumberOfEvents + 1, 5);
        var newHighest = await highestSequenceAsync();
        await waitUntilAsync(async () => (await readProgressionAsync()).LastSeqId >= newHighest,
            $"the shard follows the store to {newHighest}");

        daemon.StatusFor(TheShard.Identity).ShouldBe(AgentStatus.Running);
    }

    private sealed class ShardActionWatcher : IObserver<ShardState>
    {
        private readonly IDisposable _unsubscribe;
        private readonly string _shardName;
        private readonly ShardAction _expected;
        private readonly TaskCompletionSource<ShardState> _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationTokenSource _timeout;

        public ShardActionWatcher(ShardStateTracker tracker, string shardName, ShardAction expected, TimeSpan timeout)
        {
            _shardName = shardName;
            _expected = expected;

            _timeout = new CancellationTokenSource(timeout);
            _timeout.Token.Register(() => _completion.TrySetException(
                new TimeoutException($"Shard {_shardName} did not receive the action {_expected} in the time allowed")));

            _unsubscribe = tracker.Subscribe(this);
        }

        public Task<ShardState> Task => _completion.Task;

        public void OnCompleted()
        {
        }

        public void OnError(Exception error) => _completion.TrySetException(error);

        public void OnNext(ShardState value)
        {
            if (value.ShardName == _shardName && value.Action == _expected)
            {
                _completion.TrySetResult(value);
                _unsubscribe.Dispose();
                _timeout.Dispose();
            }
        }
    }
}
