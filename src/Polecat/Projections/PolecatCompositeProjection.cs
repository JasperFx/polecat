using System.Diagnostics.CodeAnalysis;
using JasperFx.Core.Reflection;
using JasperFx.Events.Projections;
using JasperFx.Events.Projections.Composite;
using Polecat.Events;
using Polecat.Storage;

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

    /// <summary>
    ///     Add a snapshot (self-aggregating) projection for the document type <typeparamref name="T"/>
    ///     to this composite. Behaves identically to Marten's <c>composite.Snapshot&lt;T&gt;()</c> —
    ///     under the hood it builds and registers a <see cref="SingleStreamProjection{TDoc, TId}"/>
    ///     with the appropriate identity type resolved from <typeparamref name="T"/>'s <c>Id</c> property.
    ///     Composite snapshots always run asynchronously.
    /// </summary>
    /// <typeparam name="T">
    ///     The aggregate document type. Must be self-aggregating (has its own <c>Apply</c>/<c>Create</c> methods).
    /// </typeparam>
    /// <param name="stageNumber">
    ///     Optionally move the execution of this snapshot projection to a later stage. The default is 1.
    /// </param>
    [RequiresDynamicCode("Closes SingleStreamProjection<,> over (T, T.Id) via Type.MakeGenericType. AOT consumers should register a concrete SingleStreamProjection<TDoc, TId> subclass via composite.Add() instead.")]
    [RequiresUnreferencedCode("Resolves T's Id property via DocumentMapping reflection. AOT consumers must preserve T's Id member through DynamicallyAccessedMembers or source generation.")]
    public void Snapshot<T>(int stageNumber = 1) where T : notnull
    {
        if (typeof(T).CanBeCastTo<ProjectionBase>())
        {
            throw new InvalidOperationException(
                $"This registration mechanism can only be used for an aggregate type that is 'self-aggregating'. " +
                $"Please use composite.Add() instead to register {typeof(T).FullNameInCode()}.");
        }

        var identityType = new DocumentMapping(typeof(T), _options).IdType;
        var source = typeof(SingleStreamProjection<,>).CloseAndBuildAs<ProjectionBase>(typeof(T), identityType);
        source.Lifecycle = ProjectionLifecycle.Async;
        source.AssembleAndAssertValidity();

        StageFor(stageNumber).Add((IProjectionSource<IDocumentSession, IQuerySession>)source);
    }
}
