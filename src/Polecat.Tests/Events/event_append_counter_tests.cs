using System.Diagnostics.Metrics;
using JasperFx;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.Events;

/// <summary>
///     #238: Polecat emits a <c>polecat.event.append</c> counter (unit <c>events</c>, tags
///     <c>event_type</c> + <c>tenant_id</c>) incremented once per appended event on commit. On by
///     default since Polecat 6 (#1032).
/// </summary>
/// <remarks>
///     The meter is process-wide and on by default, so every store in the test run feeds it. Only this
///     class appends the event types below, which is how its measurements are told apart.
/// </remarks>
public class event_append_counter_tests : IAsyncLifetime
{
    public record CounterStarted(string Name);

    public record CounterJoined(int Day, string Location);

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore(string schema, Action<StoreOptions>? configure = null)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            configure?.Invoke(opts);
        });
    }

    private static readonly string[] OurTypes = ["counter_started", "counter_joined"];

    private static (MeterListener listener, List<(long Value, Dictionary<string, object?> Tags)> measurements) Listen()
    {
        var measurements = new List<(long, Dictionary<string, object?>)>();
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Polecat" && instrument.Name == "polecat.event.append")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            var dict = new Dictionary<string, object?>();
            foreach (var tag in tags) dict[tag.Key] = tag.Value;
            if (dict.TryGetValue("event_type", out var type) && OurTypes.Contains(type as string))
            {
                lock (measurements) measurements.Add((measurement, dict));
            }
        });
        listener.Start();
        return (listener, measurements);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task turned_off_emits_no_measurements(bool restoreV5Defaults)
    {
        await using var store = CreateStore("otel_counter_off", opts =>
        {
            if (restoreV5Defaults) opts.RestoreV5Defaults();
            else opts.OpenTelemetry.EventCountersEnabled = false;
        });
        store.Options.OpenTelemetry.EventCountersEnabled.ShouldBeFalse();

        var (listener, measurements) = Listen();
        using (listener)
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new CounterStarted("No counter"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        measurements.ShouldBeEmpty();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task counts_each_appended_event_with_type_and_tenant_tags(bool callTrackEventCounters)
    {
        // On by default; TrackEventCounters() still works and changes nothing
        await using var store = CreateStore("otel_counter_on", opts =>
        {
            if (callTrackEventCounters) opts.OpenTelemetry.TrackEventCounters();
        });
        store.Options.OpenTelemetry.EventCountersEnabled.ShouldBeTrue();

        var (listener, measurements) = Listen();
        using (listener)
        {
            await using var session = store.LightweightSession();
            // 1 CounterStarted + 2 CounterJoined = 3 events
            session.Events.StartStream(Guid.NewGuid(),
                new CounterStarted("Counter Quest"),
                new CounterJoined(1, "Town"),
                new CounterJoined(2, "City"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);

            listener.RecordObservableInstruments();
        }

        // 3 increments of 1 each
        measurements.Count.ShouldBe(3);
        measurements.Sum(m => m.Value).ShouldBe(3);

        // every measurement carries the default tenant + a known event type
        measurements.ShouldAllBe(m => (string)m.Tags["tenant_id"]! == StorageConstants.DefaultTenantId);

        var byType = measurements
            .GroupBy(m => (string)m.Tags["event_type"]!)
            .ToDictionary(g => g.Key, g => g.Sum(x => x.Value));

        byType["counter_started"].ShouldBe(1);
        byType["counter_joined"].ShouldBe(2);
    }
}
