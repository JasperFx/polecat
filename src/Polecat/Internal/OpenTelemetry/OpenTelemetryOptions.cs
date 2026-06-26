using System.Diagnostics.Metrics;

namespace Polecat.Internal.OpenTelemetry;

/// <summary>
///     OpenTelemetry configuration for Polecat. A thin subclass of the lifted
///     <see cref="JasperFx.OpenTelemetry.OpenTelemetryOptions"/> (jasperfx#332)
///     that supplies the "Polecat" meter name via the base ctor. TrackConnections + Meter
///     come from the base; TrackLevel is type-forwarded to JasperFx.OpenTelemetry.TrackLevel
///     via a global using alias.
/// </summary>
public sealed class OpenTelemetryOptions : JasperFx.OpenTelemetry.OpenTelemetryOptions
{
    private Counter<long>? _eventAppendCounter;

    public OpenTelemetryOptions() : base("Polecat")
    {
    }

    /// <summary>
    ///     #238: when true, Polecat emits the <c>polecat.event.append</c> counter (unit
    ///     <c>events</c>, tags <c>event_type</c> + <c>tenant_id</c>), incremented once per appended
    ///     event on a successful commit. Off by default. Mirrors Marten's opt-in event-append
    ///     counter so OpenTelemetry dashboards line up across the two engines. Toggle via
    ///     <see cref="TrackEventCounters" />.
    /// </summary>
    public bool EventCountersEnabled { get; private set; }

    /// <summary>
    ///     Opt into the <c>polecat.event.append</c> counter. Mirrors Marten's
    ///     <c>OpenTelemetryOptions.TrackEventCounters()</c>.
    /// </summary>
    public void TrackEventCounters()
    {
        EventCountersEnabled = true;
    }

    /// <summary>
    ///     The lazily-created append counter, published on the Polecat
    ///     <see cref="JasperFx.OpenTelemetry.OpenTelemetryOptions.Meter" />. Created the first time
    ///     it is touched (i.e. when <see cref="EventCountersEnabled" /> is on and events commit).
    /// </summary>
    internal Counter<long> EventAppendCounter =>
        _eventAppendCounter ??= Meter.CreateCounter<long>(
            "polecat.event.append",
            "events",
            "Number of events appended to the event store");
}
