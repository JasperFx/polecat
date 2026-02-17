using System.Collections.Concurrent;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;

namespace Polecat.Projections;

/// <summary>
///     Projection registration and configuration for Polecat.
///     Collects projection registrations during configuration,
///     then builds the actual inline projections when the store is created.
/// </summary>
public class PolecatProjectionOptions
{
    private readonly List<ProjectionRegistration> _registrations = new();
    private IInlineProjection<IDocumentSession>[]? _inlineProjections;
    private readonly ConcurrentDictionary<Type, object> _aggregatorCache = new();
    private readonly List<IAggregatorSource<IQuerySession>> _aggregatorSources = new();

    /// <summary>
    ///     Register a projection type for inline or async execution.
    /// </summary>
    public void Add<TProjection>(ProjectionLifecycle lifecycle)
        where TProjection : ProjectionBase, IProjectionSource<IDocumentSession, IQuerySession>, new()
    {
        _registrations.Add(new ProjectionRegistration(typeof(TProjection), lifecycle, false));
    }

    /// <summary>
    ///     Register a self-aggregating type for inline snapshot projection.
    ///     The aggregate type must have Apply/Create methods matching event types.
    /// </summary>
    public void Snapshot<T>(SnapshotLifecycle lifecycle)
        where T : notnull, new()
    {
        _registrations.Add(new ProjectionRegistration(typeof(T), lifecycle.Map(), true));
    }

    internal bool HasRegistrations => _registrations.Count > 0;

    /// <summary>
    ///     Build the inline projection instances. Called once at DocumentStore construction.
    /// </summary>
    internal IInlineProjection<IDocumentSession>[] BuildInlineProjections(IEventRegistry eventRegistry)
    {
        if (_inlineProjections != null) return _inlineProjections;

        var inlineList = new List<IInlineProjection<IDocumentSession>>();

        foreach (var reg in _registrations)
        {
            IProjectionSource<IDocumentSession, IQuerySession> source;

            if (reg.IsSnapshot)
            {
                // Create a SingleStreamProjection<T> for the aggregate type
                var projectionType = typeof(SingleStreamProjection<>).MakeGenericType(reg.Type);
                var projection = (ProjectionBase)Activator.CreateInstance(projectionType)!;
                projection.Lifecycle = reg.Lifecycle;
                projection.AssembleAndAssertValidity();

                // Register event types with the registry
                foreach (var eventType in projection.IncludedEventTypes)
                {
                    eventRegistry.AddEventType(eventType);
                }

                source = (IProjectionSource<IDocumentSession, IQuerySession>)projection;

                // Also register as an aggregator source for live aggregation
                if (projection is IAggregatorSource<IQuerySession> aggSource)
                {
                    _aggregatorSources.Add(aggSource);
                }
            }
            else
            {
                // Instantiate the projection type directly
                var projection = (ProjectionBase)Activator.CreateInstance(reg.Type)!;
                projection.Lifecycle = reg.Lifecycle;
                projection.AssembleAndAssertValidity();

                foreach (var eventType in projection.IncludedEventTypes)
                {
                    eventRegistry.AddEventType(eventType);
                }

                source = (IProjectionSource<IDocumentSession, IQuerySession>)projection;

                if (projection is IAggregatorSource<IQuerySession> aggSource)
                {
                    _aggregatorSources.Add(aggSource);
                }
            }

            if (reg.Lifecycle == ProjectionLifecycle.Inline)
            {
                inlineList.Add(source.BuildForInline());
            }
        }

        _inlineProjections = inlineList.ToArray();
        return _inlineProjections;
    }

    /// <summary>
    ///     Get or build an aggregator for the given aggregate type.
    ///     Checks registered sources first, then builds on-the-fly from convention.
    /// </summary>
    internal IAggregator<T, IQuerySession> AggregatorFor<T>(IEventRegistry eventRegistry) where T : class, new()
    {
        if (_aggregatorCache.TryGetValue(typeof(T), out var cached))
            return (IAggregator<T, IQuerySession>)cached;

        // Check registered sources
        foreach (var source in _aggregatorSources)
        {
            if (source.AggregateType == typeof(T))
            {
                var agg = source.Build<T>();
                _aggregatorCache[typeof(T)] = agg;
                return agg;
            }
        }

        // Build on-the-fly from convention
        var projection = new SingleStreamProjection<T>();
        projection.Lifecycle = ProjectionLifecycle.Live;
        projection.AssembleAndAssertValidity();
        foreach (var et in projection.IncludedEventTypes) eventRegistry.AddEventType(et);

        var aggregator = ((IAggregatorSource<IQuerySession>)projection).Build<T>();
        _aggregatorCache[typeof(T)] = aggregator;
        return aggregator;
    }

    private record ProjectionRegistration(Type Type, ProjectionLifecycle Lifecycle, bool IsSnapshot);
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
