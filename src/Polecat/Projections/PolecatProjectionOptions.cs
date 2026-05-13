using System.Diagnostics.CodeAnalysis;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using JasperFx.Events.Projections.Composite;
using JasperFx.Events.Subscriptions;
using Polecat.Events;
using Polecat.Events.Projections;
using Polecat.Projections.Flattened;
using Polecat.Storage;
using Polecat.Subscriptions;

namespace Polecat.Projections;

/// <summary>
///     Projection registration and configuration for Polecat.
///     Extends ProjectionGraph to integrate with the JasperFx async daemon framework.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: registers projections via the JasperFx.Events ProjectionGraph (annotated RUC). Projection / aggregate types flow in from caller registration and are preserved per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: JasperFx.Events projection-registration helpers use Type.MakeGenericType — runtime code generation. AOT consumers rely on source-generated projection helpers.")]
public class PolecatProjectionOptions
    : ProjectionGraph<IProjection, IDocumentSession, IQuerySession>
{
    private readonly EventGraph _events;
    private StoreOptions? _storeOptions;
    private IInlineProjection<IDocumentSession>[]? _inlineProjections;

    internal PolecatProjectionOptions(EventGraph events) : base(events, "polecat")
    {
        _events = events;
    }

    internal void SetStoreOptions(StoreOptions options) => _storeOptions = options;

    /// <summary>
    ///     Opt into a performance optimization that directs Polecat to use a session-level
    ///     identity map for aggregates fetched via FetchForWriting() or FetchLatest().
    ///     See <see cref="EventGraph.UseIdentityMapForAggregates"/> for details.
    /// </summary>
    public bool UseIdentityMapForAggregates
    {
        get => _events.UseIdentityMapForAggregates;
        set => _events.UseIdentityMapForAggregates = value;
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

        if (projection is FlatTableProjection flatTable)
        {
            flatTable.Compile(_events);
        }
    }

    /// <summary>
    ///     Register a snapshot (self-aggregating) projection for the document type
    ///     <typeparamref name="T"/>. Behaves identically to Marten's
    ///     <c>Projections.Snapshot&lt;T&gt;()</c> — under the hood it builds and registers a
    ///     <see cref="SingleStreamProjection{TDoc, TId}"/> with the appropriate identity type
    ///     resolved from <typeparamref name="T"/>'s <c>Id</c> property.
    /// </summary>
    /// <typeparam name="T">
    ///     The aggregate document type. Must be self-aggregating — i.e. it has its own
    ///     <c>Apply</c>/<c>Create</c> methods. To register a custom <see cref="SingleStreamProjection{TDoc, TId}"/>
    ///     subclass, use <see cref="ProjectionGraph{TProjection,TOperations,TQuerySession}.Add{T}"/> instead.
    /// </typeparam>
    /// <param name="lifecycle">
    ///     The snapshot lifecycle: <see cref="SnapshotLifecycle.Inline"/> updates the snapshot in the
    ///     same transaction as the events; <see cref="SnapshotLifecycle.Async"/> updates via the async daemon.
    /// </param>
    public void Snapshot<T>(SnapshotLifecycle lifecycle) where T : notnull
    {
        AddSnapshotProjection<T>(lifecycle.ToProjectionLifecycle());
    }

    private void AddSnapshotProjection<T>(ProjectionLifecycle lifecycle) where T : notnull
    {
        if (typeof(T).CanBeCastTo<ProjectionBase>())
        {
            throw new InvalidOperationException(
                $"This registration mechanism can only be used for an aggregate type that is 'self-aggregating'. " +
                $"Please use the Projections.Add() API instead to register {typeof(T).FullNameInCode()}.");
        }

        // Resolve the Id type the same way DocumentMapping does
        var identityType = new DocumentMapping(typeof(T), _storeOptions!).IdType;
        var source = typeof(SingleStreamProjection<,>).CloseAndBuildAs<ProjectionBase>(typeof(T), identityType);
        source.Lifecycle = lifecycle;
        source.AssembleAndAssertValidity();

        Add((IProjectionSource<IDocumentSession, IQuerySession>)source, lifecycle);
    }

    /// <summary>
    ///     Register a composite projection that orchestrates multiple projections
    ///     in ordered stages. Stages run sequentially; projections within a stage
    ///     run in parallel. Composite projections always run asynchronously.
    /// </summary>
    public void CompositeProjectionFor(string name, Action<PolecatCompositeProjection> configure)
    {
        var composite = new PolecatCompositeProjection(name, _storeOptions!);
        configure(composite);
        composite.AssembleAndAssertValidity();

        // Register event types from all child projections
        foreach (var child in composite.AllProjections())
        {
            if (child is ProjectionBase pb)
            {
                foreach (var eventType in pb.IncludedEventTypes)
                {
                    _events.AddEventType(eventType);
                }
            }
        }

        All.Add((IProjectionSource<IDocumentSession, IQuerySession>)composite);
    }

    /// <summary>
    ///     Register a subscription for push-based event processing.
    /// </summary>
    public void Subscribe(Subscriptions.ISubscription subscription, Action<ISubscriptionOptions>? configure = null)
    {
        var source = subscription as ISubscriptionSource<IDocumentSession, IQuerySession>
            ?? new SubscriptionWrapper(subscription);

        if (source is ISubscriptionOptions options)
            configure?.Invoke(options);

        registerSubscription(source);
    }

    /// <summary>
    ///     Register a subscription by type. The subscription must have a parameterless constructor.
    /// </summary>
    public void Subscribe<T>(Action<ISubscriptionOptions>? configure = null)
        where T : Subscriptions.ISubscription, new()
    {
        Subscribe(new T(), configure);
    }

    /// <summary>
    /// Find a registered natural key definition for the given aggregate type, if any.
    /// </summary>
    public NaturalKeyDefinition? FindNaturalKeyDefinition(Type aggregateType)
    {
        if (TryFindAggregate(aggregateType, out var projection))
        {
            return projection.NaturalKeyDefinition;
        }

        return null;
    }

    /// <summary>
    ///     Build the inline projection instances. Called once at DocumentStore construction.
    /// </summary>
    internal IInlineProjection<IDocumentSession>[] BuildInlineProjections()
    {
        if (_inlineProjections != null) return _inlineProjections;

        // Ensure any FlatTableProjections are compiled
        foreach (var source in All)
        {
            if (source is FlatTableProjection flatTable)
            {
                flatTable.Compile(_events);
            }
        }

        var inlineList = All
            .Where(x => x.Lifecycle == ProjectionLifecycle.Inline)
            .Select(x => x.BuildForInline())
            .ToList();

        // Add natural key inline projections for any aggregate projections with natural keys
        foreach (var source in All)
        {
            if (source is IAggregateProjection { NaturalKeyDefinition: not null } aggProjection)
            {
                inlineList.Add(new NaturalKeyProjection(aggProjection.NaturalKeyDefinition, _events));
            }
        }

        _inlineProjections = inlineList.ToArray();

        return _inlineProjections;
    }
}

