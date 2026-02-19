using JasperFx.Events.Projections;
using JasperFx.Events.Projections.Composite;
using Polecat.Events;

namespace Polecat.Projections;

/// <summary>
///     Polecat-specific composite projection that composes multiple projections
///     into ordered stages. Stages run sequentially, projections within a stage
///     run in parallel.
/// </summary>
public class PolecatCompositeProjection : CompositeProjection<IDocumentSession, IQuerySession>
{
    private readonly StoreOptions _options;

    internal PolecatCompositeProjection(string name, StoreOptions options) : base(name)
    {
        _options = options;
        Lifecycle = ProjectionLifecycle.Async;
    }

    /// <summary>
    ///     Add a snapshot (self-aggregating) projection to this composite.
    /// </summary>
    public void Snapshot<T>(int stageNumber = 1) where T : notnull, new()
    {
        var source = new SingleStreamProjection<T>();
        source.Lifecycle = ProjectionLifecycle.Async;
        source.AssembleAndAssertValidity();

        StageFor(stageNumber).Add((IProjectionSource<IDocumentSession, IQuerySession>)source);
    }

    /// <summary>
    ///     Add a projection source to be executed within this composite.
    /// </summary>
    public void Add(IProjectionSource<IDocumentSession, IQuerySession> projection, int stageNumber = 1)
    {
        if (projection is ProjectionBase b)
        {
            b.Lifecycle = ProjectionLifecycle.Async;
            b.AssembleAndAssertValidity();
        }

        StageFor(stageNumber).Add(projection);
    }

    /// <summary>
    ///     Add a custom IProjection implementation to be executed within this composite.
    ///     The projection will be wrapped for composite-safe execution.
    /// </summary>
    public void Add(IProjection projection, int stageNumber = 1)
    {
        var wrapper = new CompositeIProjectionSource(projection);
        StageFor(stageNumber).Add(wrapper);
    }

    /// <summary>
    ///     Add a projection source by type to be executed within this composite.
    /// </summary>
    public void Add<T>(int stageNumber = 1) where T : IProjectionSource<IDocumentSession, IQuerySession>, new()
    {
        Add(new T(), stageNumber);
    }
}
