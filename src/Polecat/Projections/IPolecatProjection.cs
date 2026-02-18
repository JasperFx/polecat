using JasperFx.Events.Projections;

namespace Polecat.Projections;

/// <summary>
///     Marker interface for Polecat projections.
///     Mirrors Marten's IProjection : IJasperFxProjection&lt;IDocumentOperations&gt;.
/// </summary>
public interface IPolecatProjection : IJasperFxProjection<IDocumentSession>
{
}
