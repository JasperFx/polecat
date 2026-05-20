namespace Polecat.Internal.OpenTelemetry;

/// <summary>
///     OpenTelemetry configuration for Polecat. A thin subclass of the lifted
///     <see cref="JasperFx.OpenTelemetry.OpenTelemetryOptions"/> (jasperfx#332)
///     that supplies the "Polecat" meter name via the base ctor. Polecat had
///     nothing beyond the base (TrackConnections + Meter), so this subclass exists
///     only to preserve the <c>Polecat.Internal.OpenTelemetry.OpenTelemetryOptions</c>
///     public name and its parameterless construction
///     (<c>StoreOptions.OpenTelemetry = new()</c>). TrackConnections + Meter come
///     from the base; TrackLevel is type-forwarded to JasperFx.OpenTelemetry.TrackLevel
///     via a global using alias.
/// </summary>
public sealed class OpenTelemetryOptions : JasperFx.OpenTelemetry.OpenTelemetryOptions
{
    public OpenTelemetryOptions() : base("Polecat")
    {
    }
}
