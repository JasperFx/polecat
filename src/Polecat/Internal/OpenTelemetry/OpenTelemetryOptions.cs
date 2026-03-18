using System.Diagnostics.Metrics;

namespace Polecat.Internal.OpenTelemetry;

/// <summary>
///     Controls the level of OpenTelemetry tracing.
/// </summary>
public enum TrackLevel
{
    /// <summary>No tracing.</summary>
    None,

    /// <summary>Connection-level tracking (session lifecycle).</summary>
    Normal,

    /// <summary>Connection + individual write operations.</summary>
    Verbose
}

/// <summary>
///     OpenTelemetry configuration for Polecat.
/// </summary>
public sealed class OpenTelemetryOptions
{
    /// <summary>
    ///     Controls connection/session level tracing. Default is None.
    /// </summary>
    public TrackLevel TrackConnections { get; set; } = TrackLevel.None;

    /// <summary>
    ///     Meter for custom metrics. Name is "Polecat".
    /// </summary>
    public Meter Meter { get; } = new("Polecat");
}
