using System.Diagnostics;
using Polecat.Internal.OpenTelemetry;
using Polecat.Tests.Harness;

namespace Polecat.Tests.OpenTelemetry;

public class TracedDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class open_telemetry_tracing_tests : IntegrationContext
{
    public open_telemetry_tracing_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task save_changes_emits_activity_when_normal_tracking()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "otel_normal";
            opts.OpenTelemetry.TrackConnections = TrackLevel.Normal;
        });

        Activity? captured = null;

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Polecat",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = activity => captured = activity,
        };
        ActivitySource.AddActivityListener(listener);

        var doc = new TracedDoc { Id = Guid.NewGuid(), Name = "Traced" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        captured.ShouldNotBeNull();
        captured.OperationName.ShouldBe("polecat.save_changes");
        captured.Tags.ShouldContain(t => t.Key == "tenant.id");
    }

    [Fact]
    public async Task no_activity_when_tracking_disabled()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "otel_none";
            opts.OpenTelemetry.TrackConnections = TrackLevel.None;
        });

        Activity? captured = null;

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Polecat",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = activity => captured = activity,
        };
        ActivitySource.AddActivityListener(listener);

        var doc = new TracedDoc { Id = Guid.NewGuid(), Name = "NotTraced" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        captured.ShouldBeNull();
    }

    [Fact]
    public async Task verbose_tracking_includes_operation_events()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "otel_verbose";
            opts.OpenTelemetry.TrackConnections = TrackLevel.Verbose;
        });

        Activity? captured = null;

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Polecat",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => captured = activity,
        };
        ActivitySource.AddActivityListener(listener);

        var doc = new TracedDoc { Id = Guid.NewGuid(), Name = "Verbose" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        captured.ShouldNotBeNull();
        captured.Events.ShouldContain(e => e.Name.StartsWith("polecat."));
    }

    [Fact]
    public async Task activity_records_exception_on_failure()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "otel_error";
            opts.OpenTelemetry.TrackConnections = TrackLevel.Normal;
        });

        Activity? captured = null;

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Polecat",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => captured = activity,
        };
        ActivitySource.AddActivityListener(listener);

        // Store a doc, then try to Insert a duplicate to trigger an error
        var doc = new TracedDoc { Id = Guid.NewGuid(), Name = "Original" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Insert(new TracedDoc { Id = doc.Id, Name = "Duplicate" });

        captured = null;
        await Should.ThrowAsync<Exception>(async () => await session2.SaveChangesAsync());

        captured.ShouldNotBeNull();
        captured.Status.ShouldBe(ActivityStatusCode.Error);
        captured.Events.ShouldContain(e => e.Name == "exception");
    }

    [Fact]
    public void activity_source_has_correct_name()
    {
        PolecatTracing.ActivitySource.Name.ShouldBe("Polecat");
    }

    [Fact]
    public void opentelemetry_options_defaults()
    {
        var options = new OpenTelemetryOptions();
        options.TrackConnections.ShouldBe(TrackLevel.None);
        options.Meter.Name.ShouldBe("Polecat");
    }
}
