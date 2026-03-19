using JasperFx.Events.Daemon;
using Microsoft.Extensions.Hosting;

namespace Polecat.Events.Daemon.Coordination;

/// <summary>
///     Coordinates the lifecycle of asynchronous projection and subscription daemons.
///     Used by Wolverine's agent framework for distributed event processing.
/// </summary>
public interface IProjectionCoordinator : IHostedService
{
    /// <summary>
    ///     Get the projection daemon for the main database
    /// </summary>
    IProjectionDaemon DaemonForMainDatabase();

    /// <summary>
    ///     Get the projection daemon for a specific database by its identifier
    /// </summary>
    ValueTask<IProjectionDaemon> DaemonForDatabase(string databaseIdentifier);

    /// <summary>
    ///     Get all active projection daemons across all databases
    /// </summary>
    ValueTask<IReadOnlyList<IProjectionDaemon>> AllDaemonsAsync();

    /// <summary>
    ///     Stops the projection coordinator's automatic restart logic and stops all running agents
    ///     across all daemons. Does not release any held locks.
    /// </summary>
    Task PauseAsync();

    /// <summary>
    ///     Resumes the projection coordinator's automatic restart logic and starts all running agents
    ///     across all daemons. Intended to be used after <see cref="PauseAsync" />
    /// </summary>
    Task ResumeAsync();
}

/// <summary>
///     Typed projection coordinator for use with specific document store types
/// </summary>
public interface IProjectionCoordinator<T> : IProjectionCoordinator where T : IDocumentStore
{
}
