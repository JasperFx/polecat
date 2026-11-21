using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;

namespace Polecat.Tests.HiLo;

public class StarvationIntDoc
{
    public int Id { get; set; }
}

public class StarvationLongDoc
{
    public long Id { get; set; }
}

/// <summary>
///     #1088 (marten#5969): the first synchronous <c>Store()</c> calls on a Hi-Lo document type, on a schema where
///     <c>pc_hilo</c> does not exist yet, must not need the thread pool. Application code calls <c>Store()</c> from
///     pool threads, so a first burst of them can hold every pool thread, and the <c>pc_hilo</c> ensure used to block
///     on asynchronous work that needed a free pool thread to finish. A 10 ms heartbeat came back up to two seconds late.
/// </summary>
/// <remarks>
///     This test used to time a pool heartbeat during a burst of writers against a 500 ms limit. A cold
///     <c>pc_hilo</c> ensure holds every waiting writer for as long as the DDL takes, so that measured the database's
///     latency as much as the pool, and it failed at 580 ms on a loaded CI runner against SQL Server 2022 with
///     nothing wrong. Now it saturates the pool and checks the stores finish while a sentinel is still queued behind
///     the blockers, which doesn't depend on how fast the database is. It saturates the pool on purpose, so it runs
///     alone, in the <c>thread-pool</c> collection.
/// </remarks>
[Collection("thread-pool")]
public class hilo_thread_pool_starvation_tests : OneOffConfigurationsContext
{
    // A pool with every thread blocked injects threads slowly (a couple a second), and each one it injects picks up
    // the next blocker. This many more than it has keeps the sentinel queued far longer than the stores take.
    private const int ExtraBlockers = 256;

    // Only a regression comes anywhere near this: the stores then wait on pool work queued behind every blocker
    private static readonly TimeSpan StoreTimeout = TimeSpan.FromSeconds(30);

    private readonly ITestOutputHelper _output;

    public hilo_thread_pool_starvation_tests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task the_first_synchronous_stores_complete_without_the_thread_pool()
    {
        ConfigureStore(opts =>
        {
            // #1090: with MARS on, as the test connection string has it. SqlClient's managed network layer (Linux
            // and macOS) completes a synchronous read on a MARS connection through the thread pool, so Polecat opens
            // the connections for its own synchronous pc_hilo work without MARS.
            new SqlConnectionStringBuilder(opts.ConnectionString).MultipleActiveResultSets.ShouldBeTrue();

            opts.Schema.For<StarvationIntDoc>();
            opts.Schema.For<StarvationLongDoc>();
        });

        // Resolve the document storage up front, so only the sequences are cold
        theStore.Options.Providers.GetProvider<StarvationIntDoc>();
        theStore.Options.Providers.GetProvider<StarvationLongDoc>();

        // Several writers, so all but one of them wait on the ensure the first one runs, as a burst of cold
        // callers does
        const int writerCount = 8;
        const int docsPerType = 5;
        var ints = new ConcurrentBag<int>();
        var longs = new ConcurrentBag<long>();
        var failures = new ConcurrentBag<Exception>();
        var sessions = new ConcurrentBag<IDocumentSession>();

        var writers = Enumerable.Range(0, writerCount).Select(_ => new Thread(() =>
        {
            try
            {
                // IDocumentSession is only IAsyncDisposable, so the sessions are disposed once the pool is let go
                var session = theStore.LightweightSession();
                sessions.Add(session);
                for (var i = 0; i < docsPerType; i++)
                {
                    var doc = new StarvationIntDoc();
                    session.Store(doc);
                    ints.Add(doc.Id);

                    var other = new StarvationLongDoc();
                    session.Store(other);
                    longs.Add(other.Id);
                }
            }
            catch (Exception e)
            {
                failures.Add(e);
            }
        }) { IsBackground = true }).ToArray();

        using var release = new ManualResetEventSlim(false);
        var sentinel = new StrongBox<bool>();
        var blockers = ThreadPool.ThreadCount + ExtraBlockers;
        bool finished;
        bool sentinelHadRun;
        TimeSpan elapsed;

        try
        {
            // Every pool thread, and every thread the pool injects for a long while, picks up a blocker, and the
            // sentinel goes in behind them all. Pool work the stores queue goes in behind the sentinel, so stores
            // that finish while it is still queued needed nothing from the pool.
            for (var i = 0; i < blockers; i++)
            {
                ThreadPool.UnsafeQueueUserWorkItem(static r => r.Wait(), release, preferLocal: false);
            }

            ThreadPool.UnsafeQueueUserWorkItem(static s => Volatile.Write(ref s.Value, true), sentinel,
                preferLocal: false);

            var stopwatch = Stopwatch.StartNew();
            foreach (var writer in writers) writer.Start();

            // Blocks the test's own thread too, on purpose: awaiting here would need the pool
            finished = writers.All(writer => writer.Join(remaining(stopwatch)));
            elapsed = stopwatch.Elapsed;
            sentinelHadRun = Volatile.Read(ref sentinel.Value);
        }
        finally
        {
            release.Set();
        }

        foreach (var writer in writers) writer.Join();
        foreach (var session in sessions) await session.DisposeAsync();

        _output.WriteLine(
            $"{writerCount} writers, {blockers} blockers on {Environment.ProcessorCount} processors: the stores took {elapsed.TotalMilliseconds:N0} ms, " +
            $"finished: {finished}, sentinel had run: {sentinelHadRun}");

        failures.ShouldBeEmpty(string.Join(Environment.NewLine, failures.Select(x => x.ToString())));

        finished.ShouldBeTrue(
            $"the first synchronous Store() calls were still blocked after {StoreTimeout.TotalSeconds:N0} s with the thread pool saturated");
        sentinelHadRun.ShouldBeFalse(
            $"the pool got through all {blockers} blockers while the stores ran ({elapsed.TotalMilliseconds:N0} ms), so this run can't show whether they needed it");

        ints.Count.ShouldBe(writerCount * docsPerType);
        ints.ShouldAllBe(id => id > 0);
        ints.Distinct().Count().ShouldBe(ints.Count);
        longs.Count.ShouldBe(writerCount * docsPerType);
        longs.ShouldAllBe(id => id > 0);
        longs.Distinct().Count().ShouldBe(longs.Count);
    }

    private static TimeSpan remaining(Stopwatch stopwatch)
    {
        var left = StoreTimeout - stopwatch.Elapsed;
        return left > TimeSpan.Zero ? left : TimeSpan.Zero;
    }
}
