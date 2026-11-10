using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Polecat.Internal.OpenTelemetry;

/// <summary>
///     The one <see cref="System.Diagnostics.ActivitySource" /> and the one <see cref="System.Diagnostics.Metrics.Meter" />
///     Polecat publishes on, both named "Polecat". <c>DocumentStore</c> (the JasperFx <c>IEventStore</c> seam), the
///     session tracing decorator and the opt-in event counter all share these rather than each creating their own.
/// </summary>
internal static class PolecatTracing
{
    private const string Name = "Polecat";

    internal static ActivitySource ActivitySource { get; } = new(
        Name,
        typeof(PolecatTracing).Assembly.GetName().Version!.ToString());

    internal static Meter Meter { get; } = new(
        Name,
        typeof(PolecatTracing).Assembly.GetName().Version!.ToString());

    /// <summary>Wall time of one <c>SaveChangesAsync</c> unit of work, in milliseconds.</summary>
    internal static Histogram<double> SaveChangesDuration { get; } = Meter.CreateHistogram<double>(
        "polecat.save_changes.duration", "ms", "Duration of a SaveChangesAsync unit of work");

    /// <summary>Events appended by one committed unit of work.</summary>
    internal static Histogram<long> EventAppendBatchSize { get; } = Meter.CreateHistogram<long>(
        "polecat.event.append.batch_size", "events", "Number of events appended by one committed save");

    /// <summary>
    ///     Time for a session command to come back from the server. For a reader that is time to the reader
    ///     opening (first result), not the time to drain it.
    /// </summary>
    internal static Histogram<double> QueryDuration { get; } = Meter.CreateHistogram<double>(
        "polecat.query.duration", "ms", "Duration of a command executed through a session");

    public static Activity? StartActivity(string spanName, Activity? parentActivity = null,
        ActivityTagsCollection? tags = null,
        ActivityKind activityKind = ActivityKind.Internal)
    {
        return ActivitySource.StartActivity(spanName, activityKind, parentActivity?.Context ?? default, tags);
    }
}
