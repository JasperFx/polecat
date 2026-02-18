using JasperFx.Events.Daemon;
using JasperFx.Events.Daemon.HighWater;
using Microsoft.Extensions.Logging;
using Polecat.Projections;
using Polecat.Storage;

namespace Polecat.Events.Daemon;

/// <summary>
///     Polecat's async projection daemon. Thin wrapper over JasperFxAsyncDaemon
///     that wires Polecat's event store, database, and projection graph.
/// </summary>
public class PolecatProjectionDaemon
    : JasperFxAsyncDaemon<IDocumentSession, IQuerySession, IProjection>, IProjectionDaemon
{
    public PolecatProjectionDaemon(
        DocumentStore store,
        PolecatDatabase database,
        ILoggerFactory loggerFactory,
        IHighWaterDetector detector)
        : base(store, database, loggerFactory, detector, store.Options.Projections)
    {
    }

    public PolecatProjectionDaemon(
        DocumentStore store,
        PolecatDatabase database,
        ILogger logger,
        IHighWaterDetector detector)
        : base(store, database, logger, detector, store.Options.Projections)
    {
    }
}
