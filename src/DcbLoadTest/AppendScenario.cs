using System.Diagnostics;
using JasperFx.Events;
using Polecat;
using Polecat.Events;

namespace DcbLoadTest;

/// <summary>
/// 1. Plain tagged-append throughput: no boundary read, so the only DCB cost is the producer-side bump
/// of pc_dcb_tag_version for every distinct tag (and, under the per-event-type scopes, every distinct
/// (tag, event type)) the save carries.
/// </summary>
public static class AppendScenario
{
    private sealed record Variant(string Name, string Description, int Saves, int Writers, int EventsPerSave,
        bool Tagged, int Regions);

    private sealed class Sample
    {
        public readonly List<double> EventsPerSec = new();
        public readonly List<double> SavesPerSec = new();
        public readonly Latencies Latency = new();
        public readonly List<double> VersionWritesPerSave = new();
        public readonly List<double> LockWaitsPerSave = new();
        public long LockPromotions;
    }

    public static async Task Run(Settings settings)
    {
        var repeats = settings.Int("repeats", 3);
        var writers = settings.Int("writers", 16);
        var variants = new[]
        {
            new Variant("single", "1 event, 1 tag (new customer) per save; sequential",
                settings.Int("single-saves", 2000), 1, 1, true, 0),
            new Variant("batch", "10 events of 2 types, tags customer (new) + region (1 of 1000); sequential",
                settings.Int("batch-saves", 400), 1, 10, true, 1000),
            new Variant("untagged", "as batch, no tags (baseline)",
                settings.Int("batch-saves", 400), 1, 10, false, 0),
            new Variant("concurrent-hot", $"as batch, {writers} writers, region is 1 of 10 (hot version rows)",
                settings.Int("concurrent-saves", 100), writers, 10, true, 10),
        };

        var scopes = settings.Scopes;
        var samples = new Dictionary<(string, DcbConflictScope), Sample>();
        var versionRows = new Dictionary<DcbConflictScope, (long Rows, long Events, long Bytes)>();

        Report.Line("## 1. Tagged-append throughput");
        Report.Line();
        Report.Header(settings);

        for (var r = 0; r < repeats; r++)
        {
            foreach (var scope in scopes)
            {
                var schema = $"dcb_app_{Stores.Short(scope)}";
                var store = await Stores.Fresh(settings, schema, scope);
                Report.Progress($"repeat {r + 1}/{repeats}: {scope}");

                foreach (var variant in variants)
                {
                    var key = (variant.Name, scope);
                    if (!samples.TryGetValue(key, out var sample)) samples[key] = sample = new Sample();

                    // Warm-up: plans, and as many pooled connections as the variant has writers, so the
                    // measured p99 is not SQL Server logins. Not measured.
                    await RunVariant(store, variant with { Saves = Math.Min(20, variant.Saves) }, new Latencies());

                    // sys.dm_db_index_operational_stats is live, so unlike pg_stat there is nothing to settle.
                    var before = await Sql.Operational(settings, schema);

                    var latency = new Latencies();
                    var elapsed = await RunVariant(store, variant, latency);
                    var totalSaves = variant.Saves * variant.Writers;

                    var delta = (await Sql.Operational(settings, schema)).Minus(before);

                    sample.EventsPerSec.Add(totalSaves * variant.EventsPerSave / elapsed.TotalSeconds);
                    sample.SavesPerSec.Add(totalSaves / elapsed.TotalSeconds);
                    sample.Latency.AddRange(latency);
                    sample.VersionWritesPerSave.Add((double)delta.RowWrites / totalSaves);
                    sample.LockWaitsPerSave.Add((double)delta.RowLockWaits / totalSaves);
                    sample.LockPromotions += delta.LockPromotions;

                    Report.Progress(
                        $"  {variant.Name}: {totalSaves / elapsed.TotalSeconds:N0} saves/s, " +
                        $"{(double)delta.RowWrites / totalSaves:N1} version writes/save, " +
                        $"{delta.RowLockWaits} row lock waits ({delta.RowLockWaitMs} ms)");
                }

                var physical = await Sql.Physical(settings, schema, mode: "LIMITED");
                versionRows[scope] = (
                    physical.Rows,
                    await Sql.Count(settings, schema, "pc_events"),
                    physical.ReservedBytes);
                store.Dispose();
            }
        }

        Report.Line("### pc_dcb_tag_version after one repeat of every variant");
        Report.Line();
        Report.Table(["Scope", "events", "version rows", "reserved MB"],
            scopes.Select(scope =>
            {
                var v = versionRows[scope];
                return new[] { scope.ToString(), Report.N0(v.Events), Report.N0(v.Rows), Report.Mb(v.Bytes) };
            }));

        foreach (var variant in variants)
        {
            Report.Line($"### {variant.Name}: {variant.Description}");
            Report.Line();
            Report.Line($"{variant.Saves * variant.Writers} saves x {repeats} repeats; throughput is the median repeat (min-max), latency is per save, pooled across repeats. " +
                        "Version-row writes are the leaf_insert_count + leaf_update_count delta on pc_dcb_tag_version's clustered index.");
            Report.Line();

            double? baseline = null;
            Report.Table(
                ["Scope", "events/s", "saves/s (min-max)", "p50 ms", "p99 ms", "version-row writes/save",
                    "row lock waits/save", "lock escalations", "vs TagValue"],
                scopes.Select(scope =>
                {
                    var s = samples[(variant.Name, scope)];
                    var median = Median(s.SavesPerSec);
                    if (scope == DcbConflictScope.TagValue) baseline = median;
                    var versus = scope != DcbConflictScope.TagValue && baseline is { } b
                        ? $"{(median / b - 1) * 100:+0;-0}%"
                        : "";
                    return new[]
                    {
                        scope.ToString(), Report.N0(Median(s.EventsPerSec)),
                        $"{median:N0} ({s.SavesPerSec.Min():N0}-{s.SavesPerSec.Max():N0})",
                        Report.Ms(s.Latency.Percentile(50)), Report.Ms(s.Latency.Percentile(99)),
                        Median(s.VersionWritesPerSave).ToString("N1"), Median(s.LockWaitsPerSave).ToString("N2"),
                        s.LockPromotions.ToString(), versus
                    };
                }));
        }
    }

    private static async Task<TimeSpan> RunVariant(IDocumentStore store, Variant variant, Latencies latency)
    {
        var sw = Stopwatch.StartNew();
        var writers = Enumerable.Range(0, variant.Writers).Select(w => Task.Run(async () =>
        {
            var random = new Random(w * 7919 + Environment.TickCount);
            for (var s = 0; s < variant.Saves; s++)
            {
                var started = Stopwatch.GetTimestamp();
                await using var session = store.LightweightSession();
                var streamId = Guid.NewGuid();
                var customer = new CustomerId(Guid.NewGuid());
                var region = variant.Regions > 0 ? new RegionId($"region-{random.Next(variant.Regions)}") : null;

                for (var e = 0; e < variant.EventsPerSave; e++)
                {
                    object data = e % 2 == 0
                        ? new OrderPlaced($"ORD-{s}-{e}", 10m + e)
                        : new OrderShipped($"TRK-{s}-{e}");

                    if (!variant.Tagged)
                    {
                        session.Events.Append(streamId, data);
                        continue;
                    }

                    var wrapped = session.Events.BuildEvent(data);
                    if (region is null) wrapped.WithTag(customer);
                    else wrapped.WithTag(customer, region);
                    session.Events.Append(streamId, wrapped);
                }

                await session.SaveChangesAsync();
                latency.Add(Stopwatch.GetElapsedTime(started));
            }
        })).ToArray();

        await Task.WhenAll(writers);
        return sw.Elapsed;
    }

    internal static double Median(IReadOnlyList<double> values)
    {
        if (values.Count == 0) return double.NaN;
        var sorted = values.OrderBy(x => x).ToArray();
        return sorted.Length % 2 == 1
            ? sorted[sorted.Length / 2]
            : (sorted[sorted.Length / 2 - 1] + sorted[sorted.Length / 2]) / 2;
    }
}
