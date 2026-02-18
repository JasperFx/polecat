using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Events;

namespace Polecat.Projections;

/// <summary>
///     Projection registration and configuration for Polecat.
///     Extends ProjectionGraph to integrate with the JasperFx async daemon framework.
/// </summary>
public class PolecatProjectionOptions
    : ProjectionGraph<IPolecatProjection, IDocumentSession, IQuerySession>
{
    private readonly EventGraph _events;
    private IInlineProjection<IDocumentSession>[]? _inlineProjections;

    internal PolecatProjectionOptions(EventGraph events) : base(events, "polecat")
    {
        _events = events;
    }

    protected override void onAddProjection(object projection)
    {
        if (projection is ProjectionBase pb)
        {
            foreach (var eventType in pb.IncludedEventTypes)
            {
                _events.AddEventType(eventType);
            }
        }
    }

    /// <summary>
    ///     Register a self-aggregating type for snapshot projection.
    ///     The aggregate type must have Apply/Create methods matching event types.
    /// </summary>
    public void Snapshot<T>(SnapshotLifecycle lifecycle)
        where T : notnull, new()
    {
        var projection = new SingleStreamProjection<T>();
        var mapped = lifecycle.Map();
        projection.Lifecycle = mapped;
        projection.AssembleAndAssertValidity();

        foreach (var eventType in projection.IncludedEventTypes)
        {
            _events.AddEventType(eventType);
        }

        All.Add((IProjectionSource<IDocumentSession, IQuerySession>)projection);
    }

    /// <summary>
    ///     Build the inline projection instances. Called once at DocumentStore construction.
    /// </summary>
    internal IInlineProjection<IDocumentSession>[] BuildInlineProjections()
    {
        if (_inlineProjections != null) return _inlineProjections;

        _inlineProjections = All
            .Where(x => x.Lifecycle == ProjectionLifecycle.Inline)
            .Select(x => x.BuildForInline())
            .ToArray();

        return _inlineProjections;
    }
}

/// <summary>
///     Lifecycle for snapshot projections, mirroring Marten's SnapshotLifecycle.
/// </summary>
public enum SnapshotLifecycle
{
    Inline,
    Async
}

public static class SnapshotLifecycleExtensions
{
    public static ProjectionLifecycle Map(this SnapshotLifecycle lifecycle) => lifecycle switch
    {
        SnapshotLifecycle.Inline => ProjectionLifecycle.Inline,
        SnapshotLifecycle.Async => ProjectionLifecycle.Async,
        _ => throw new ArgumentOutOfRangeException(nameof(lifecycle))
    };
}
