using System.Collections.Concurrent;
using System.Diagnostics;
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
///     #1088 (marten#5969): the first burst of synchronous <c>Store()</c> calls on a Hi-Lo document type, on a
///     schema where <c>pc_hilo</c> does not exist yet, must not starve the thread pool. Each writer blocks a pool
///     thread in <c>Store()</c>, as application code does, and a 10 ms heartbeat on the pool has to keep running.
/// </summary>
public class hilo_thread_pool_starvation_tests : OneOffConfigurationsContext
{
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromMilliseconds(10);

    private readonly ITestOutputHelper _output;

    public hilo_thread_pool_starvation_tests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task the_first_burst_of_synchronous_stores_does_not_starve_the_thread_pool()
    {
        ConfigureStore(opts =>
        {
            // Without MARS. SqlClient's managed network layer (Linux and macOS) completes a synchronous read on a
            // MARS connection through the thread pool, so with MARS on, any synchronous I/O in a burst like this one
            // waits for the pool to inject threads, whatever Polecat does. That is SqlClient's, not this test's.
            opts.ConnectionString = new SqlConnectionStringBuilder(ConnectionSource.ConnectionString)
            {
                MultipleActiveResultSets = false
            }.ConnectionString;

            opts.Schema.For<StarvationIntDoc>();
            opts.Schema.For<StarvationLongDoc>();
        });

        // Resolve the document storage up front, so only the sequences are cold
        theStore.Options.Providers.GetProvider<StarvationIntDoc>();
        theStore.Options.Providers.GetProvider<StarvationLongDoc>();

        // More writers than the pool has threads, so it cannot hand every one of them a thread without injecting
        // more
        var writers = Math.Max(ThreadPool.ThreadCount, Environment.ProcessorCount) + 16;
        const int docsPerWriter = 5;

        using var stop = new CancellationTokenSource();
        var clock = Stopwatch.StartNew();
        var poolTicks = new ConcurrentQueue<TimeSpan>();
        var dedicatedTicks = new ConcurrentQueue<TimeSpan>();
        var heartbeat = Task.Run(() => poolHeartbeatAsync(clock, poolTicks, stop.Token));
        var dedicated = new Thread(() => dedicatedHeartbeat(clock, dedicatedTicks, stop.Token)) { IsBackground = true };
        dedicated.Start();

        // Let the heartbeats settle into their loops before the writers arrive
        await Task.Delay(100);

        var ints = new ConcurrentBag<int>();
        var longs = new ConcurrentBag<long>();
        var failures = new ConcurrentBag<Exception>();
        var sessions = new ConcurrentBag<IDocumentSession>();

        var stopwatch = Stopwatch.StartNew();
        var tasks = Enumerable.Range(0, writers).Select(_ => Task.Run(() =>
        {
            try
            {
                // IDocumentSession is only IAsyncDisposable, so the sessions are disposed after the burst
                var session = theStore.LightweightSession();
                sessions.Add(session);
                for (var i = 0; i < docsPerWriter; i++)
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
        })).ToArray();

        await Task.WhenAll(tasks);
        var burst = stopwatch.Elapsed;

        foreach (var session in sessions) await session.DisposeAsync();

        stop.Cancel();
        await heartbeat;
        dedicated.Join();

        // The stall is counted on a dedicated (non-pool) thread's clock: a busy machine delays that thread as much
        // as the pool heartbeat, and pool starvation does not delay it at all
        var pool = poolTicks.ToArray();
        var reference = dedicatedTicks.ToArray();
        var worstWallClock = TimeSpan.Zero;
        var worstTicks = 0;
        for (var i = 1; i < pool.Length; i++)
        {
            var gap = pool[i] - pool[i - 1];
            if (gap > worstWallClock) worstWallClock = gap;

            var from = pool[i - 1];
            var to = pool[i];
            var ticks = reference.Count(x => x > from && x < to);
            if (ticks > worstTicks) worstTicks = ticks;
        }

        var stall = HeartbeatInterval * worstTicks;

        _output.WriteLine(
            $"{writers} writers on {Environment.ProcessorCount} processors: burst took {burst.TotalMilliseconds:N0} ms, " +
            $"worst pool heartbeat gap {worstWallClock.TotalMilliseconds:N0} ms wall clock, {stall.TotalMilliseconds:N0} ms on a dedicated thread's clock");

        failures.ShouldBeEmpty(string.Join(Environment.NewLine, failures.Select(x => x.ToString())));

        ints.Count.ShouldBe(writers * docsPerWriter);
        ints.ShouldAllBe(id => id > 0);
        ints.Distinct().Count().ShouldBe(ints.Count);
        longs.ShouldAllBe(id => id > 0);
        longs.Distinct().Count().ShouldBe(longs.Count);

        stall.ShouldBeLessThan(TimeSpan.FromMilliseconds(500),
            $"the thread pool was starved for {stall.TotalMilliseconds:N0} ms while {writers} writers stored their first documents");
    }

    private static async Task poolHeartbeatAsync(Stopwatch clock, ConcurrentQueue<TimeSpan> ticks,
        CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            ticks.Enqueue(clock.Elapsed);
            await Task.Delay(HeartbeatInterval, CancellationToken.None);
        }

        ticks.Enqueue(clock.Elapsed);
    }

    private static void dedicatedHeartbeat(Stopwatch clock, ConcurrentQueue<TimeSpan> ticks, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            ticks.Enqueue(clock.Elapsed);
            Thread.Sleep(HeartbeatInterval);
        }
    }
}
