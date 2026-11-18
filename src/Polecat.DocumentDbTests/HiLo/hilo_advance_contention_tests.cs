using System.Collections.Concurrent;
using Polecat.Internal;
using Polecat.Schema.Identity.Sequences;
using Polecat.Tests.Harness;
using Weasel.Core.Sequences;

namespace Polecat.Tests.HiLo;

/// <summary>
///     gh-1004 (marten#5938): advancing to the next hi used to read <c>hi_value</c> and then
///     compare-and-set it, returning -1 to every caller whose read another caller's commit had overtaken.
///     Under contention most attempts lost: 16 sequences x 25 advances at
///     <c>MaxAdvanceToNextHiAttempts = 1</c> failed 347 of 400 times. The advance is now one atomic MERGE.
/// </summary>
/// <remarks>
///     Each <see cref="HiloSequence" /> instance stands for a separate process: they share only the
///     <c>pc_hilo</c> row. One attempt is the sharpest form of the claim, because an atomic increment never
///     loses one; at the default 30 attempts the same defect is a rare flake rather than a reliable failure.
/// </remarks>
public class hilo_advance_contention_tests : OneOffConfigurationsContext
{
    private const int Callers = 16;
    private const int AdvancesPerCaller = 25;

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    private static string NewEntity() => "HiloContention_" + Guid.NewGuid().ToString("N")[..8];

    private HiloSequence NewSequence(string entity, int maxLo = 1000) =>
        new(new ConnectionFactory(ConnectionSource.ConnectionString), theStore.Options, entity,
            new HiloSettings { MaxAdvanceToNextHiAttempts = 1, MaxLo = maxLo });

    [Fact]
    public async Task every_concurrent_async_advance_secures_a_hi_on_its_first_attempt()
    {
        var ct = TestContext.Current.CancellationToken;
        var entity = NewEntity();

        // pc_hilo and this entity's row exist before the race, so only the increment is contended
        var warm = NewSequence(entity);
        await warm.AdvanceToNextHi(ct);

        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = new ConcurrentBag<Exception>();
        var his = new ConcurrentBag<long> { warm.CurrentHi };

        var tasks = Enumerable.Range(0, Callers).Select(_ => Task.Run(async () =>
        {
            var sequence = NewSequence(entity);
            await start.Task;

            for (var i = 0; i < AdvancesPerCaller; i++)
            {
                try
                {
                    await sequence.AdvanceToNextHi(ct);
                    his.Add(sequence.CurrentHi);
                }
                catch (Exception e)
                {
                    failures.Add(e);
                }
            }
        }, ct)).ToArray();

        start.SetResult();
        await Task.WhenAll(tasks).WaitAsync(Bound, ct);

        failures.Count.ShouldBe(0,
            $"{failures.Count} of {Callers * AdvancesPerCaller} advances failed; first: {failures.FirstOrDefault()}");

        // Every advance got a block of its own, and together they are exactly the next 400 blocks
        his.OrderBy(x => x).ShouldBe(Enumerable.Range(0, Callers * AdvancesPerCaller + 1).Select(x => (long)x));
    }

    [Fact]
    public async Task every_concurrent_sync_advance_secures_a_hi_on_its_first_attempt()
    {
        var ct = TestContext.Current.CancellationToken;
        var entity = NewEntity();

        var warm = NewSequence(entity);
        await warm.AdvanceToNextHi(ct);

        var start = new ManualResetEventSlim();
        var failures = new ConcurrentBag<Exception>();
        var ids = new ConcurrentBag<long>();

        // MaxLo = 1, so every NextLong exhausts its block and the next one runs the synchronous advance.
        // Dedicated threads, so sixteen blocked callers never lean on the thread pool.
        var tasks = Enumerable.Range(0, Callers).Select(_ => Task.Factory.StartNew(() =>
        {
            var sequence = NewSequence(entity, maxLo: 1);
            start.Wait(ct);

            for (var i = 0; i < AdvancesPerCaller; i++)
            {
                try
                {
                    ids.Add(sequence.NextLong());
                }
                catch (Exception e)
                {
                    failures.Add(e);
                }
            }
        }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default)).ToArray();

        start.Set();
        await Task.WhenAll(tasks).WaitAsync(Bound, ct);

        failures.Count.ShouldBe(0,
            $"{failures.Count} of {Callers * AdvancesPerCaller} advances failed; first: {failures.FirstOrDefault()}");

        // With one id per block the id is hi + 1, so blocks 1..400 become ids 2..401
        ids.OrderBy(x => x).ShouldBe(Enumerable.Range(2, Callers * AdvancesPerCaller).Select(x => (long)x));
    }

    [Fact]
    public async Task concurrent_first_use_of_an_entity_hands_out_block_zero_exactly_once()
    {
        var ct = TestContext.Current.CancellationToken;

        // pc_hilo exists, but the entity's row does not: every caller races to create it
        await NewSequence(NewEntity()).AdvanceToNextHi(ct);
        var entity = NewEntity();

        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failures = new ConcurrentBag<Exception>();
        var his = new ConcurrentBag<long>();

        var tasks = Enumerable.Range(0, Callers).Select(_ => Task.Run(async () =>
        {
            var sequence = NewSequence(entity);
            await start.Task;

            try
            {
                await sequence.AdvanceToNextHi(ct);
                his.Add(sequence.CurrentHi);
            }
            catch (Exception e)
            {
                failures.Add(e);
            }
        }, ct)).ToArray();

        start.SetResult();
        await Task.WhenAll(tasks).WaitAsync(Bound, ct);

        failures.Count.ShouldBe(0,
            $"{failures.Count} of {Callers} first advances failed; first: {failures.FirstOrDefault()}");
        his.OrderBy(x => x).ShouldBe(Enumerable.Range(0, Callers).Select(x => (long)x));
    }
}
