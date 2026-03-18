using System.Diagnostics;

namespace Polecat.Internal.OpenTelemetry;

/// <summary>
///     Static helper for Polecat's OpenTelemetry ActivitySource.
/// </summary>
internal static class PolecatTracing
{
    internal static ActivitySource ActivitySource { get; } = new(
        "Polecat",
        typeof(PolecatTracing).Assembly.GetName().Version!.ToString());

    public static Activity? StartActivity(string spanName, Activity? parentActivity = null,
        ActivityTagsCollection? tags = null,
        ActivityKind activityKind = ActivityKind.Internal)
    {
        return ActivitySource.StartActivity(spanName, activityKind, parentActivity?.Context ?? default, tags);
    }
}
