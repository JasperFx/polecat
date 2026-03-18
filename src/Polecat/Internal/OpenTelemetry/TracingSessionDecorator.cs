using System.Diagnostics;

namespace Polecat.Internal.OpenTelemetry;

/// <summary>
///     Provides tracing helpers for session operations.
/// </summary>
internal static class TracingSessionDecorator
{
    /// <summary>
    ///     Starts a tracing activity for a session operation if tracing is enabled.
    /// </summary>
    public static Activity? StartSessionActivity(string operationName, string tenantId,
        OpenTelemetryOptions options)
    {
        if (options.TrackConnections == TrackLevel.None) return null;
        if (!PolecatTracing.ActivitySource.HasListeners()) return null;

        var tags = new ActivityTagsCollection
        {
            { "tenant.id", tenantId }
        };

        return PolecatTracing.StartActivity(operationName, Activity.Current, tags);
    }

    /// <summary>
    ///     Adds verbose operation events to an activity if verbose tracking is enabled.
    /// </summary>
    public static void AddOperationEvents(Activity? activity, IEnumerable<IStorageOperation> operations,
        OpenTelemetryOptions options)
    {
        if (activity == null) return;
        if (options.TrackConnections != TrackLevel.Verbose) return;

        foreach (var op in operations)
        {
            var typeName = op.DocumentType?.Name ?? "unknown";
            activity.AddEvent(new ActivityEvent($"polecat.{op.Role().ToString().ToLowerInvariant()}",
                tags: new ActivityTagsCollection { { "document.type", typeName } }));
        }
    }

    /// <summary>
    ///     Adds an exception to an activity.
    /// </summary>
    public static void RecordException(Activity? activity, Exception ex)
    {
        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
        activity?.AddEvent(new ActivityEvent("exception",
            tags: new ActivityTagsCollection
            {
                { "exception.type", ex.GetType().FullName! },
                { "exception.message", ex.Message }
            }));
    }
}
