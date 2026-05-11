using JasperFx.Events.Projections;

namespace Polecat.Projections;

/// <summary>
/// Maps the canonical <see cref="SnapshotLifecycle"/> (defined in
/// <c>JasperFx.Events.Projections</c>) onto Polecat's projection-registration
/// <see cref="ProjectionLifecycle"/>. Kept product-local because the broader
/// <see cref="ProjectionLifecycle"/> is a projection-registration concern,
/// not a shared event-sourcing concept (per the dedup audit row
/// JasperFx/jasperfx#220 / pillar #214).
/// </summary>
internal static class SnapshotLifecycleExtensions
{
    public static ProjectionLifecycle ToProjectionLifecycle(this SnapshotLifecycle lifecycle) =>
        lifecycle switch
        {
            SnapshotLifecycle.Inline => ProjectionLifecycle.Inline,
            SnapshotLifecycle.Async => ProjectionLifecycle.Async,
            _ => throw new ArgumentOutOfRangeException(nameof(lifecycle), lifecycle, null)
        };
}
