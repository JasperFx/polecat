namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     Polecat's projection coordinator marker. The canonical contract — member-for-member
///     identical — lives in <see cref="JasperFx.Events.Daemon.IProjectionCoordinator"/>
///     (lifted from Marten in the Critter Stack 2026 dedupe pass, jasperfx#214); this empty
///     inheriting interface preserves source compatibility for the
///     <c>Polecat.Events.Daemon.Coordination</c> namespace.
/// </summary>
public interface IProjectionCoordinator : JasperFx.Events.Daemon.IProjectionCoordinator
{
}

/// <summary>
///     Typed projection coordinator marker for ancillary store registrations in DI.
///     Inherits the lifted generic variant; the marker constraint is the shared
///     <c>where T : class</c> (relaxed from Polecat's former <c>IDocumentStore</c>).
/// </summary>
public interface IProjectionCoordinator<T> : IProjectionCoordinator, JasperFx.Events.Daemon.IProjectionCoordinator<T>
    where T : class
{
}
