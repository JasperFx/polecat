using System.Diagnostics;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat;
using Polecat.Events;

namespace DcbLoadTest;

/// <summary>
/// 2. FetchForWritingByTags latency against a seeded store, and the full uncontended decision
/// (fetch + append + save) that adds the boundary check to it.
/// </summary>
public static class ReadScenario
{
    private const int EventsPerStream = 10;
    private const int Regions = 1000;

    public static async Task Run(Settings settings)
    {
        var streams = settings.Int("seed-streams", 100_000);
        var iterations = settings.Int("iterations", 1000);

        Report.Line($"## 2. FetchForWritingByTags latency, {streams * EventsPerStream:N0} seeded events");
        Report.Line();
        Report.Header(settings);
        Report.Line($"{streams:N0} customers x {EventsPerStream} events (OrderPlaced/OrderShipped alternating), " +
                    $"each event also tagged with 1 of {Regions} regions (~{streams * EventsPerStream / Regions:N0} events per region). " +
                    $"Seeded under TagValueAndEventType so both kinds of version row exist; {iterations} sequential iterations per row, random customer each time.");
        Report.Line();

        var rows = new List<string[]>();

        var schema = $"dcb_read_{streams}";
        var customers = await Seed(settings, schema, streams);

        foreach (var scope in settings.Scopes)
        {
            using var store = Stores.Build(settings, schema, scope);
            var random = new Random(42);

            CustomerId AnyCustomer() => customers[random.Next(customers.Length)];

            var queries = new (string Name, Func<EventTagQuery> Query)[]
            {
                ("1 tag (~10 events)", () => new EventTagQuery().Or(AnyCustomer())),
                ("1 tag + AndEventsOfType (~5 events)", () => new EventTagQuery().Or(AnyCustomer()).AndEventsOfType<OrderPlaced>()),
                ("1 tag, no events yet", () => new EventTagQuery().Or(new CustomerId(Guid.NewGuid()))),
                ($"customer OR region (~{10 + streams * EventsPerStream / Regions:N0} events)",
                    () => new EventTagQuery().Or(AnyCustomer()).Or(new RegionId($"region-{random.Next(Regions)}"))),
            };

            foreach (var (name, query) in queries)
            {
                var fetch = new Latencies();
                var decision = new Latencies();
                long eventsRead = 0;

                // Warm-up
                for (var i = 0; i < 25; i++) await Fetch(store, query(), new Latencies());

                for (var i = 0; i < iterations; i++) eventsRead += await Fetch(store, query(), fetch);
                for (var i = 0; i < iterations; i++) await Decide(store, query(), decision);

                rows.Add([
                    scope.ToString(), name, (eventsRead / (double)iterations).ToString("N1"),
                    Report.Ms(fetch.Percentile(50)), Report.Ms(fetch.Percentile(99)),
                    Report.Ms(decision.Percentile(50)), Report.Ms(decision.Percentile(99))
                ]);
                Report.Progress($"{scope} {name}: fetch p50 {fetch.Percentile(50):N2}ms, decision p50 {decision.Percentile(50):N2}ms");
            }
        }

        Report.Table(
            ["Scope", "Query", "events read", "fetch p50 ms", "fetch p99 ms", "fetch+append+save p50 ms", "p99 ms"],
            rows);
    }

    private static async Task<int> Fetch(IDocumentStore store, EventTagQuery query, Latencies latency)
    {
        var started = Stopwatch.GetTimestamp();
        await using var session = store.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<OrderAggregate>(query);
        latency.Add(Stopwatch.GetElapsedTime(started));
        return boundary.Events.Count;
    }

    private static async Task Decide(IDocumentStore store, EventTagQuery query, Latencies latency)
    {
        var started = Stopwatch.GetTimestamp();
        await using var session = store.LightweightSession();
        var boundary = await session.Events.FetchForWritingByTags<OrderAggregate>(query);
        var customer = (CustomerId)query.Conditions[0].TagValue;
        var shipped = session.Events.BuildEvent(new OrderShipped("TRK-decision"));
        shipped.WithTag(customer);
        boundary.AppendOne(shipped);
        await session.SaveChangesAsync();
        latency.Add(Stopwatch.GetElapsedTime(started));
    }

    /// <summary>
    /// Seed once per size; reused when the schema already holds the events (pass --reseed to rebuild).
    /// </summary>
    /// <remarks>
    /// Polecat has no event bulk import (no BulkInsertEventsAsync), so this batches
    /// <c>--seed-streams-per-save</c> streams per session across <c>--seed-writers</c> writers. That is
    /// the ordinary tagged-append path, so it writes the pc_event_tag_* rows and the version rows too.
    /// </remarks>
    private static async Task<CustomerId[]> Seed(Settings settings, string schema, int streams)
    {
        // Customer ids are derived from the stream index so a reused schema still has known customers.
        var customers = Enumerable.Range(0, streams).Select(CustomerFor).ToArray();
        var expected = (long)streams * EventsPerStream;

        long existing = 0;
        try
        {
            existing = await Sql.Count(settings, schema, "pc_events");
        }
        catch
        {
            // schema does not exist yet
        }

        if (existing >= expected && settings.Get("reseed", "false") != "true")
        {
            Report.Progress($"{schema}: reusing {existing:N0} seeded events");
            return customers;
        }

        using var store = await Stores.Fresh(settings, schema, DcbConflictScope.TagValueAndEventType);
        var writers = settings.Int("seed-writers", 8);
        var streamsPerSave = settings.Int("seed-streams-per-save", 50);
        var sw = Stopwatch.StartNew();
        var next = -streamsPerSave;
        long done = 0;

        var seeding = Task.WhenAll(Enumerable.Range(0, writers).Select(_ => Task.Run(async () =>
        {
            while (true)
            {
                var first = Interlocked.Add(ref next, streamsPerSave);
                if (first >= streams) return;

                var last = Math.Min(first + streamsPerSave, streams);
                for (var attempt = 1; ; attempt++)
                {
                    try
                    {
                        await using var session = store.LightweightSession();
                        for (var s = first; s < last; s++)
                        {
                            var streamId = Guid.NewGuid();
                            for (var e = 0; e < EventsPerStream; e++)
                            {
                                object data = e % 2 == 0
                                    ? new OrderPlaced($"ORD-{s}-{e}", 10m + e)
                                    : new OrderShipped($"TRK-{s}-{e}");
                                var wrapped = session.Events.BuildEvent(data);
                                wrapped.WithTag(customers[s], new RegionId($"region-{(s * EventsPerStream + e) % Regions}"));
                                session.Events.Append(streamId, wrapped);
                            }
                        }

                        await session.SaveChangesAsync();
                        break;
                    }
                    catch (Exception ex) when (attempt < 5 && Errors.IsDeadlock(ex))
                    {
                        // Seeding is not a measurement; a deadlock here is reported and the batch redone.
                        Report.Progress($"{schema}: seed batch at stream {first} deadlocked (attempt {attempt}); retrying");
                    }
                }

                Interlocked.Add(ref done, last - first);
            }
        })));

        while (await Task.WhenAny(seeding, Task.Delay(TimeSpan.FromSeconds(15))) != seeding)
        {
            var count = Interlocked.Read(ref done);
            Report.Progress($"{schema}: seeded {count:N0}/{streams:N0} streams in {sw.Elapsed.TotalSeconds:N0}s");
        }

        await seeding;
        Report.Progress($"{schema}: seeded {expected:N0} events in {sw.Elapsed.TotalSeconds:N0}s; updating statistics");
        await Sql.Execute(settings, "exec sp_updatestats");
        return customers;
    }

    private static CustomerId CustomerFor(int index)
    {
        Span<byte> bytes = stackalloc byte[16];
        BitConverter.TryWriteBytes(bytes, index);
        bytes[15] = 0x5e;
        return new CustomerId(new Guid(bytes));
    }
}
