using System.Diagnostics.Metrics;
using JasperFx;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.Events;

/// <summary>
///     #238: when <c>OpenTelemetry.TrackEventCounters()</c> is enabled, Polecat emits a
///     <c>polecat.event.append</c> counter (unit <c>events</c>, tags <c>event_type</c> +
///     <c>tenant_id</c>) incremented once per appended event on commit. Off by default.
/// </summary>
public class event_append_counter_tests : IAsyncLifetime
{
    public Task InitializeAsync() => Task.CompletedTask;
    public Task DisposeAsync() => Task.CompletedTask;

    private static DocumentStore CreateStore(string schema, bool trackCounters)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            if (trackCounters)
            {
                opts.OpenTelemetry.TrackEventCounters();
            }
        });
    }

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
            lock (measurements) measurements.Add((measurement, dict));
        });
        listener.Start();
        return (listener, measurements);
    }

    [Fact]
    public async Task disabled_by_default_emits_no_measurements()
    {
        await using var store = CreateStore("otel_counter_off", trackCounters: false);
        store.Options.OpenTelemetry.EventCountersEnabled.ShouldBeFalse();

        var (listener, measurements) = Listen();
        using (listener)
        {
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted("No counter"));
            await session.SaveChangesAsync();
        }

        measurements.ShouldBeEmpty();
    }

    [Fact]
    public async Task counts_each_appended_event_with_type_and_tenant_tags()
    {
        await using var store = CreateStore("otel_counter_on", trackCounters: true);
        store.Options.OpenTelemetry.EventCountersEnabled.ShouldBeTrue();

        var (listener, measurements) = Listen();
        using (listener)
        {
            await using var session = store.LightweightSession();
            // 1 QuestStarted + 2 MembersJoined = 3 events
            session.Events.StartStream(Guid.NewGuid(),
                new QuestStarted("Counter Quest"),
                new MembersJoined(1, "Town", ["A"]),
                new MembersJoined(2, "City", ["B"]));
            await session.SaveChangesAsync();

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

        byType["quest_started"].ShouldBe(1);
        byType["members_joined"].ShouldBe(2);
    }
}
