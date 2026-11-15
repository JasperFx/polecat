using System.Diagnostics;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat;

namespace DcbLoadTest;

/// <summary>
/// 4. pc_dcb_tag_version growth and page health over a long run of decisions. A decision reads one
/// customer's boundary (customer AND OrderPlaced) and appends an OrderShipped tagged with that customer,
/// across a large tag set chosen uniformly at random, so writes land on every leaf page rather than one.
/// </summary>
/// <remarks>
/// SQL Server has no MVCC heap and no HOT updates: a version bump is an in-place update of a fixed-width
/// bigint, so the PostgreSQL HOT ratio and dead-tuple count have no direct equivalent. What can grow here
/// is new rows (the per-event-type keys TagValueAndEventType adds as each customer first ships), and those
/// inserts land mid-index on a random-GUID key, so they split pages. The measures are therefore page splits
/// (leaf_allocation_count), leaf page count, fullness and fragmentation, and ghost records (version ghosts
/// appear only under snapshot isolation / RCSI / ADR).
///
/// Polecat has no DcbTagVersionFillFactor. <c>--fillfactors none,70</c> approximates Marten's by
/// rebuilding the version table's clustered index WITH (FILLFACTOR = n) after the pre-creation, which is
/// the only point SQL Server applies a fill factor. That rebuild is harness-only DDL.
/// </remarks>
public static class GrowthScenario
{
    public static async Task Run(Settings settings)
    {
        var tagValues = settings.Int("tag-values", 200_000);
        var seconds = settings.Int("seconds", 300);
        var writers = settings.Int("writers", 16);
        var sampleEvery = settings.Int("sample-seconds", 30);
        var sampleMode = settings.Get("sample-mode", "DETAILED");
        var fillFactors = settings.Get("fillfactors", "none,70").Split(',')
            .Select(x => x == "none" ? (int?)null : int.Parse(x)).ToArray();

        Report.Line($"## 4. pc_dcb_tag_version growth over {seconds}s of decisions");
        Report.Line();
        Report.Header(settings);
        Report.Line($"{tagValues:N0} customers pre-created (one OrderPlaced each), then {writers} writers for {seconds}s, " +
                    "each decision reading customer AND OrderPlaced and appending OrderShipped for a uniformly random customer. " +
                    $"Sizes and page shape are pc_dcb_tag_version's (sys.dm_db_partition_stats, sys.dm_db_index_physical_stats {sampleMode} at the clustered leaf); " +
                    "page splits, row inserts and row updates are sys.dm_db_index_operational_stats deltas from the start of the run.");
        Report.Line();

        var summary = new List<string[]>();
        foreach (var scope in settings.Scopes)
        foreach (var fillFactor in fillFactors)
        {
            var label = $"{scope}, fillfactor {(fillFactor?.ToString() ?? "default (0/100)")}";
            var schema = $"dcb_grow_{Stores.Short(scope)}_{fillFactor?.ToString() ?? "def"}";
            var store = await Stores.Fresh(settings, schema, scope);
            var precreate = Stopwatch.StartNew();
            var customers = await Precreate(store, tagValues);
            Report.Progress($"{label}: pre-created {tagValues:N0} customers in {precreate.Elapsed.TotalSeconds:N0}s");

            if (fillFactor.HasValue)
            {
                await Sql.Execute(settings,
                    $"alter index all on {Sql.Qualified(schema, Sql.VersionTable)} rebuild with (fillfactor = {fillFactor.Value})");
            }

            if (settings.Get("update-stats", "false") == "true")
            {
                await Sql.Execute(settings, "exec sp_updatestats");
            }

            var startOps = await Sql.Operational(settings, schema);
            var startPhysical = await Sql.Physical(settings, schema, mode: sampleMode);
            Report.Progress($"{label}: {startPhysical.Rows:N0} version rows, reserved {Report.Mb(startPhysical.ReservedBytes)} MB");

            var samples = new List<string[]> { Sample(0, double.NaN, startPhysical, OpStats.Zero) };

            long saves = 0;
            long conflicts = 0;
            long deadlocks = 0;
            long otherErrors = 0;
            string? firstOther = null;
            var latency = new Latencies();
            var stop = Stopwatch.StartNew();
            var deadline = TimeSpan.FromSeconds(seconds);

            var work = Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
            {
                var random = new Random(w * 104729 + 17);
                while (stop.Elapsed < deadline)
                {
                    var customer = customers[random.Next(customers.Length)];
                    var started = Stopwatch.GetTimestamp();
                    try
                    {
                        await using var session = store.LightweightSession();
                        var boundary = await session.Events.FetchForWritingByTags<OrderAggregate>(
                            new EventTagQuery().Or(customer).AndEventsOfType<OrderPlaced>());
                        var shipped = session.Events.BuildEvent(new OrderShipped("TRK"));
                        shipped.WithTag(customer);
                        boundary.AppendOne(shipped);
                        await session.SaveChangesAsync();
                        Interlocked.Increment(ref saves);
                        latency.Add(Stopwatch.GetElapsedTime(started));
                    }
                    catch (Exception e) when (Errors.Is<DcbConcurrencyException>(e))
                    {
                        Interlocked.Increment(ref conflicts);
                    }
                    catch (Exception e) when (Errors.IsDeadlock(e))
                    {
                        Interlocked.Increment(ref deadlocks);
                    }
                    catch (Exception e)
                    {
                        Interlocked.Increment(ref otherErrors);
                        firstOther ??= Errors.Describe(e) + ": " + e.Message;
                    }
                }
            })).ToArray();

            var all = Task.WhenAll(work);
            var lastSaves = 0L;
            var lastTime = TimeSpan.Zero;
            while (!all.IsCompleted)
            {
                await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(sampleEvery)));
                if (all.IsCompleted) break;
                var now = stop.Elapsed;
                var current = Interlocked.Read(ref saves);
                var ops = (await Sql.Operational(settings, schema)).Minus(startOps);
                var physical = await Sql.Physical(settings, schema, mode: sampleMode);
                samples.Add(Sample(now.TotalSeconds, (current - lastSaves) / (now - lastTime).TotalSeconds, physical, ops));
                lastSaves = current;
                lastTime = now;
            }

            await all;
            var elapsed = stop.Elapsed;
            var endOps = (await Sql.Operational(settings, schema)).Minus(startOps);
            var end = await Sql.Physical(settings, schema, mode: sampleMode);
            samples.Add(Sample(elapsed.TotalSeconds, double.NaN, end, endOps));

            Report.Line($"### {label}");
            Report.Line();
            if (otherErrors > 0) Report.Line($"**{otherErrors} other errors**, first: {firstOther}");
            Report.Table(
                ["t (s)", "saves/s in interval", "version rows", "reserved MB", "leaf MB", "non-leaf MB", "leaf pages",
                    "page fullness %", "fragmentation %", "ghosts / version ghosts", "row inserts", "row updates", "page splits"],
                samples);

            summary.Add([
                scope.ToString(), fillFactor?.ToString() ?? "default",
                Report.N0(saves), (saves / elapsed.TotalSeconds).ToString("N0"),
                Report.Ms(latency.Percentile(50)), Report.Ms(latency.Percentile(99)),
                conflicts.ToString(), deadlocks.ToString(),
                $"{Report.N0(startPhysical.Rows)} -> {Report.N0(end.Rows)}",
                (endOps.RowWrites / (double)Math.Max(saves, 1)).ToString("N2"),
                Report.N0(endOps.LeafAllocations),
                $"{Report.Mb(startPhysical.ReservedBytes)} -> {Report.Mb(end.ReservedBytes)}",
                $"{Report.Mb(startPhysical.DataBytes)} -> {Report.Mb(end.DataBytes)}",
                $"{startPhysical.PageSpaceUsedPct:N1} -> {end.PageSpaceUsedPct:N1}",
                $"{startPhysical.FragmentationPct:N1} -> {end.FragmentationPct:N1}",
                $"{Report.N0(end.GhostRecords)} / {Report.N0(end.VersionGhostRecords)}",
                endOps.LockPromotions.ToString()
            ]);
            store.Dispose();
        }

        Report.Line("### Summary");
        Report.Line();
        Report.Table(
            ["Scope", "fillfactor", "saves", "saves/s", "p50 ms", "p99 ms", "conflicts", "client 1205", "version rows",
                "row writes/save", "page splits", "reserved MB", "leaf MB", "page fullness %", "fragmentation %",
                "ghosts / version ghosts at end", "lock escalations"],
            summary);
    }

    private static string[] Sample(double seconds, double savesPerSec, PhysicalStats physical, OpStats ops) =>
    [
        seconds.ToString("N0"), double.IsNaN(savesPerSec) || savesPerSec == 0 ? "-" : savesPerSec.ToString("N0"),
        Report.N0(physical.Rows), Report.Mb(physical.ReservedBytes), Report.Mb(physical.DataBytes), Report.Mb(physical.IndexBytes),
        Report.N0(physical.LeafPages), physical.PageSpaceUsedPct.ToString("N1"), physical.FragmentationPct.ToString("N1"),
        $"{Report.N0(physical.GhostRecords)} / {Report.N0(physical.VersionGhostRecords)}",
        Report.N0(ops.LeafInserts), Report.N0(ops.LeafUpdates), Report.N0(ops.LeafAllocations)
    ];

    private static async Task<CustomerId[]> Precreate(IDocumentStore store, int count)
    {
        var customers = Enumerable.Range(0, count).Select(_ => new CustomerId(Guid.NewGuid())).ToArray();
        const int perSave = 500;
        var next = -perSave;
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            while (true)
            {
                var first = Interlocked.Add(ref next, perSave);
                if (first >= count) return;
                await using var session = store.LightweightSession();
                for (var i = first; i < Math.Min(first + perSave, count); i++)
                {
                    var placed = session.Events.BuildEvent(new OrderPlaced($"ORD-{i}", 10m));
                    placed.WithTag(customers[i]);
                    session.Events.StartStream(Guid.NewGuid(), placed);
                }

                await session.SaveChangesAsync();
            }
        })));
        return customers;
    }
}
