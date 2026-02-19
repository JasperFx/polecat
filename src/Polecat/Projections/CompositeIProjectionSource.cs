using System.Diagnostics.CodeAnalysis;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Descriptors;
using JasperFx.Events.Projections;
using JasperFx.Events.Subscriptions;
using Microsoft.Extensions.Logging;

namespace Polecat.Projections;

/// <summary>
///     Wraps an IProjection for use inside a CompositeProjection, providing composite-safe
///     batch lifecycle management. Mirrors Marten's CompositeIProjectionSource.
/// </summary>
internal class CompositeIProjectionSource :
    ProjectionBase,
    IProjectionSource<IDocumentSession, IQuerySession>,
    ISubscriptionFactory<IDocumentSession, IQuerySession>
{
    private readonly IProjection _projection;

    public CompositeIProjectionSource(IProjection projection)
    {
        _projection = projection;
        Lifecycle = ProjectionLifecycle.Async;
        Name = projection.GetType().Name;
        Version = 1;
        if (_projection.GetType().TryGetAttribute<ProjectionVersionAttribute>(out var att))
        {
            Version = att.Version;
        }
    }

    public SubscriptionType Type => SubscriptionType.EventProjection;
    public ShardName[] ShardNames() => [new ShardName(Name, ShardName.All, Version)];
    public Type ImplementationType => _projection.GetType();
    public SubscriptionDescriptor Describe(IEventStore store) => new(this, store);

    public IReadOnlyList<AsyncShard<IDocumentSession, IQuerySession>> Shards()
    {
        return
        [
            new AsyncShard<IDocumentSession, IQuerySession>(Options, ShardRole.Projection,
                new ShardName(Name, "All", Version), this, this)
        ];
    }

    public bool TryBuildReplayExecutor(IEventStore<IDocumentSession, IQuerySession> store, IEventDatabase database,
        [NotNullWhen(true)] out IReplayExecutor? executor)
    {
        executor = default;
        return false;
    }

    IInlineProjection<IDocumentSession> IProjectionSource<IDocumentSession, IQuerySession>.BuildForInline()
    {
        throw new NotSupportedException("CompositeIProjectionSource does not support inline execution");
    }

    public ISubscriptionExecution BuildExecution(IEventStore<IDocumentSession, IQuerySession> store,
        IEventDatabase database, ILoggerFactory loggerFactory, ShardName shardName)
    {
        return new CompositeIProjectionExecution(_projection, shardName);
    }

    public ISubscriptionExecution BuildExecution(IEventStore<IDocumentSession, IQuerySession> store,
        IEventDatabase database, ILogger logger, ShardName shardName)
    {
        return new CompositeIProjectionExecution(_projection, shardName);
    }
}

/// <summary>
///     A lightweight ISubscriptionExecution for IProjection instances running inside a composite.
///     Does NOT dispose the shared batch — the composite manages batch lifecycle.
/// </summary>
internal class CompositeIProjectionExecution : ISubscriptionExecution
{
    private readonly IProjection _projection;

    public CompositeIProjectionExecution(IProjection projection, ShardName shardName)
    {
        _projection = projection;
        ShardName = shardName;
    }

    public ShardName ShardName { get; }
    public ShardExecutionMode Mode { get; set; }

    public async Task ProcessRangeAsync(EventRange range)
    {
        var batch = range.ActiveBatch as IProjectionBatch<IDocumentSession, IQuerySession>;
        if (batch == null) return;

        var groups = range.Events.GroupBy(x => x.TenantId).ToArray();
        foreach (var group in groups)
        {
            await using var session = batch.SessionForTenant(group.Key);
            await _projection.ApplyAsync(session, group.ToList(), CancellationToken.None).ConfigureAwait(false);
        }
    }

    public ValueTask EnqueueAsync(EventPage page, ISubscriptionAgent subscriptionAgent) => new();
    public Task StopAndDrainAsync(CancellationToken token) => Task.CompletedTask;
    public Task HardStopAsync() => Task.CompletedTask;

    public bool TryBuildReplayExecutor([NotNullWhen(true)] out IReplayExecutor? executor)
    {
        executor = default;
        return false;
    }

    public Task ProcessImmediatelyAsync(SubscriptionAgent subscriptionAgent, EventPage events,
        CancellationToken cancellation) => Task.CompletedTask;

    public bool TryGetAggregateCache<TId, TDoc>([NotNullWhen(true)] out IAggregateCaching<TId, TDoc>? caching)
    {
        caching = null;
        return false;
    }

    public ValueTask DisposeAsync() => new();
}
