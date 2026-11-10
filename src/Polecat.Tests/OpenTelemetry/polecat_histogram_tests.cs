using System.Diagnostics.Metrics;
using JasperFx;
using JasperFx.Events;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.OpenTelemetry;

/// <summary>
///     #803: the save, append-batch and query histograms publish on the one "Polecat" meter, and
///     DocumentStore's IEventStore seam hands out that same meter and the same ActivitySource.
/// </summary>
public class polecat_histogram_tests
{
    [Fact]
    public async Task save_append_and_query_histograms_record_on_the_shared_meter()
    {
        await using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "otel_histograms";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });

        var seen = new Dictionary<string, List<double>>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Polecat" && instrument.Name.StartsWith("polecat.", StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<double>((i, m, _, _) => { lock (seen) { if (!seen.TryGetValue(i.Name, out var l)) seen[i.Name] = l = []; l.Add(m); } });
        listener.SetMeasurementEventCallback<long>((i, m, _, _) => { lock (seen) { if (!seen.TryGetValue(i.Name, out var l)) seen[i.Name] = l = []; l.Add(m); } });
        listener.Start();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted("a"), new QuestStarted("b"), new QuestStarted("c"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        lock (seen)
        {
            seen["polecat.save_changes.duration"].ShouldNotBeEmpty();
            seen["polecat.event.append.batch_size"].ShouldContain(3);
            seen["polecat.query.duration"].ShouldNotBeEmpty();
        }

        var eventStore = (IEventStore)store;
        eventStore.Meter.Name.ShouldBe("Polecat");
        eventStore.ActivitySource.Name.ShouldBe("Polecat");
    }
}
