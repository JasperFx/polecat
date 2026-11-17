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
    ///     event on a successful commit. On by default since Polecat 6: without a listener on the
    ///     meter it costs nothing. Set it to false, or call <see cref="StoreOptions.RestoreV5Defaults" />,
    ///     to turn it off. Mirrors Marten's event-append counter so OpenTelemetry dashboards line up
    ///     across the two engines.
    /// </summary>
    public bool EventCountersEnabled { get; set; } = true;

    /// <summary>
    ///     Turn on the <c>polecat.event.append</c> counter, which is already on unless something turned
    ///     it off. Mirrors Marten's <c>OpenTelemetryOptions.TrackEventCounters()</c>.
    /// </summary>
    public void TrackEventCounters()
    {
        EventCountersEnabled = true;
    }

    /// <summary>
    ///     The lazily-created append counter, published on the shared
    ///     <see cref="PolecatTracing.Meter" />. Created the first time
    ///     it is touched (i.e. when <see cref="EventCountersEnabled" /> is on and events commit).
    /// </summary>
    internal Counter<long> EventAppendCounter =>
        _eventAppendCounter ??= PolecatTracing.Meter.CreateCounter<long>(
            "polecat.event.append",
            "events",
            "Number of events appended to the event store");
}
