using System.Diagnostics;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Polecat.Subscriptions;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.Daemon;

/// <summary>
///     polecat#744 / jasperfx#953. The HotCold repro, reduced to the one claim that matters: a node whose
///     subscription is mid-batch stops within <c>StopAndDrainTimeout</c> instead of working its backlog
///     to completion.
/// </summary>
/// <remarks>
///     <para>
///         The field report was a HotCold node that had LOST a subscription's lock and kept processing for
///         34 s while another node already ran the same shard, with graceful shutdown taking 27 s against
///         a 30 s termination grace period. Both numbers are the same bug: before JasperFx 2.81.0
///         <c>SubscriptionExecutionBase.StopAndDrainAsync</c> awaited its execution block with no token at
///         all, so the drain was unbounded no matter what the caller asked for.
///     </para>
///     <para>
///         ⚠️ This drives the stop path directly rather than racing two hosts for an advisory lock. Losing
///         a lock is only how the HotCold node is ASKED to stop — the defect was entirely in what the
///         subscription execution did once asked, which is the same code path on a lock loss, a
///         reassignment and a SIGTERM. A two-node lock race would add a second SQL Server, a timing
///         window and nothing to the claim.
///     </para>
///     <para>
///         The timing assertion is deliberately loose. It separates "bounded by roughly the timeout" from
///         "ran the batch to completion", which are 2 s and 60 s apart here; it is not a latency budget.
///     </para>
/// </remarks>
public class subscription_drain_is_bounded_tests : OneOffConfigurationsContext
{
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(2);

    // Far longer than the drain bound, so "stopped on time" and "finished the batch" cannot be confused.
    private static readonly TimeSpan BatchDuration = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task a_stop_does_not_wait_for_a_batch_slower_than_the_drain_timeout()
    {
        StallingSubscription.Reset();

        ConfigureStore(opts =>
        {
            opts.DatabaseSchemaName = "drain_bounded";
            opts.Projections.StopAndDrainTimeout = DrainTimeout;
            opts.Projections.Subscribe<StallingSubscription>();
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await using (var session = theStore.LightweightSession())
        {
            for (var i = 0; i < 25; i++)
            {
                session.Events.StartStream(Guid.NewGuid(),
                    new QuestStarted($"Quest {i}"),
                    new MembersJoined(1, "Bree", ["Barliman"]));
            }

            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        using (daemon)
        {
            await daemon.StartAllAsync();

            // The premise: the subscription is INSIDE ProcessEventsAsync when the stop arrives. Without this
            // the test would pass on a daemon that had not started working yet, proving nothing.
            var entered = await Task.WhenAny(StallingSubscription.Entered,
                Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken));
            entered.ShouldBe((Task)StallingSubscription.Entered);

            var stopwatch = Stopwatch.StartNew();
            await daemon.StopAllAsync();
            stopwatch.Stop();

            stopwatch.Elapsed.ShouldBeLessThan(BatchDuration - TimeSpan.FromSeconds(20));
        }

        // The batch was abandoned rather than completed, so nothing marked it done and whoever runs the
        // shard next re-delivers it. That redelivery is the documented trade-off of the bound.
        StallingSubscription.Completed.ShouldBeFalse();
    }

    public class StallingSubscription : SubscriptionBase
    {
        private static TaskCompletionSource _entered =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public static Task Entered => _entered.Task;

        public static bool Completed { get; private set; }

        public static void Reset()
        {
            _entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Completed = false;
        }

        public override async Task<IChangeListener> ProcessEventsAsync(
            EventRange page,
            ISubscriptionController controller,
            IDocumentOperations operations,
            CancellationToken cancellationToken)
        {
            _entered.TrySetResult();

            // Stands in for a slow external call. Honours the token, which is the contract the drain
            // relies on — a subscription that ignores it is the HotCold double-runner in #744.
            await Task.Delay(BatchDuration, cancellationToken);

            Completed = true;
            return NullChangeListener.Instance;
        }
    }
}
