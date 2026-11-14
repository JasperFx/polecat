using System.Diagnostics;
using JasperFx.Events;
using Polecat.Events;

namespace DcbLoadTest;

/// <summary>
/// The harness's original benchmark: seed a store with tagged events, then time sequential untagged and
/// tagged appends into it. Kept for continuity; it now runs in the harness's own catalog and schema.
/// </summary>
public static class LegacyScenario
{
    public static async Task Run(Settings settings)
    {
        var seedStreams = settings.Int("seed-streams", 1000);
        var benchStreams = settings.Int("bench-streams", 200);
        const int eventsPerStream = 10;

        Report.Line("## Legacy: append into a seeded store");
        Report.Line();
        Report.Header(settings);

        using var store = await Stores.Fresh(settings, "dcb_legacy", DcbConflictScope.TagValueAndEventType);

        var seed = Stopwatch.StartNew();
        for (var s = 0; s < seedStreams; s++)
        {
            await AppendStream(store, s, eventsPerStream, tagged: true, regions: 10, prefix: "seed");
        }

        Report.Progress($"seeded {seedStreams * eventsPerStream:N0} tagged events in {seed.Elapsed.TotalSeconds:N1}s");

        var rows = new List<string[]>();
        foreach (var tagged in new[] { false, true })
        {
            var sw = Stopwatch.StartNew();
            for (var s = 0; s < benchStreams; s++)
            {
                await AppendStream(store, s, eventsPerStream, tagged, regions: 10, prefix: "bench");
            }

            var events = benchStreams * eventsPerStream;
            rows.Add([tagged ? "With Tags" : "No Tags", Report.N0(events), sw.Elapsed.TotalMilliseconds.ToString("N1"),
                (events / sw.Elapsed.TotalSeconds).ToString("N1")]);
        }

        Report.Table(["Scenario", "Events", "Total (ms)", "Events/sec"], rows);
    }

    private static async Task AppendStream(Polecat.IDocumentStore store, int s, int events, bool tagged, int regions,
        string prefix)
    {
        await using var session = store.LightweightSession();
        var streamId = Guid.NewGuid();
        var customer = new CustomerId(Guid.NewGuid());
        var region = new RegionId($"region-{s % regions}");

        for (var e = 0; e < events; e++)
        {
            object data = e % 2 == 0
                ? new OrderPlaced($"ORD-{prefix}-{s}-{e}", 99.99m + e)
                : new OrderShipped($"TRACK-{prefix}-{s}-{e}");

            if (!tagged)
            {
                session.Events.Append(streamId, data);
                continue;
            }

            var wrapped = session.Events.BuildEvent(data);
            wrapped.WithTag(customer, region);
            session.Events.Append(streamId, wrapped);
        }

        await session.SaveChangesAsync();
    }
}
