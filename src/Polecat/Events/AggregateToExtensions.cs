using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Daemon;
using JasperFx.Events.Grouping;
using Polecat.Events.Linq;
using Polecat.Internal;
using Polecat.Linq;

namespace Polecat.Events;

/// <summary>
///     LINQ terminals that run the events matched by an event query (session.Events.QueryAllRawEvents())
///     through registered aggregations. Mirrors Marten's AggregateToExtensions (marten#4998 parity).
/// </summary>
public static class AggregateToExtensions
{
    private static readonly MethodInfo AggregateManyMethod = typeof(AggregateToExtensions)
        .GetMethod(nameof(aggregateManyAsync), BindingFlags.Static | BindingFlags.NonPublic)!;

    private static QuerySession SessionFor(IQueryable<IEvent> queryable)
    {
        if (queryable is PolecatLinqQueryable<IEvent> { PolecatProvider: EventLinqQueryProvider provider })
        {
            return provider.Session;
        }

        throw new ArgumentException(
            "AggregateTo*() can only be used on a Polecat event query, e.g. session.Events.QueryAllRawEvents().Where(...).",
            nameof(queryable));
    }

    /// <summary>
    ///     Run the events matched by this query through the multi-stream projection registered for
    ///     <typeparamref name="T"/> and return the aggregate it produces for each resulting identity. This is
    ///     the live-query twin of the projection step-through's multi-stream run: it drives the projection's
    ///     REAL slicer/grouper and per-slice build (the same core the step-through uses, minus the step
    ///     observer) against the live query session, so enrichment that reads reference data works for free.
    /// </summary>
    [RequiresUnreferencedCode("Reflects over the projection's JasperFxAggregationProjectionBase base to close the fold over its TId.")]
    [RequiresDynamicCode("Closes aggregateManyAsync over (T, TId) via MakeGenericMethod.")]
    public static async Task<IReadOnlyList<T>> AggregateToManyAsync<T>(this IQueryable<IEvent> queryable,
        CancellationToken token = default) where T : class
    {
        var session = SessionFor(queryable);
        var options = session.Options;

        // Validate the projection up front (even for an empty result set) so a call for an aggregate type
        // that has no registered projection is a clear programming error rather than a silent empty list.
        var projection = options.Projections.All
                             .OfType<IAggregateProjection>()
                             .FirstOrDefault(x => x.AggregateType == typeof(T))
                         ?? throw new ArgumentException(
                             $"No aggregate projection is registered that produces '{typeof(T).FullNameInCode()}'. AggregateToManyAsync() runs an event query through a registered (multi-stream) projection.",
                             nameof(queryable));

        var idType = findAggregateIdType(projection.GetType())
                     ?? throw new ArgumentException(
                         $"Projection '{projection.GetType().FullNameInCode()}' for '{typeof(T).FullNameInCode()}' is not a slicing aggregate projection that AggregateToManyAsync() can drive.",
                         nameof(queryable));

        var events = await queryable.ToListAsync(token).ConfigureAwait(false);
        if (events.Count == 0)
        {
            return Array.Empty<T>();
        }

        var closed = AggregateManyMethod.MakeGenericMethod(typeof(T), idType);
        var task = (Task<IReadOnlyList<T>>)closed.Invoke(null, [projection, events, session, token])!;
        return await task.ConfigureAwait(false);
    }

    // TId is not statically known at the AggregateToManyAsync<T> call site, so drive the strongly-typed
    // slice -> enrich -> build fold through a TId-closed helper invoked via reflection.
    private static async Task<IReadOnlyList<T>> aggregateManyAsync<T, TId>(
        JasperFxAggregationProjectionBase<T, TId, IDocumentSession, IQuerySession> projection,
        IReadOnlyList<IEvent> events,
        QuerySession session,
        CancellationToken token)
        where T : class where TId : notnull
    {
        // The SAME building blocks the step-through fold uses (BuildTimelinesAsync): the projection's own
        // slicer/grouper, then EnrichEventsAsync per group, then DetermineActionAsync (Create/Apply/
        // ShouldDelete dispatch) per slice — so the live-query result can't diverge from the step-through.
        var slicer = projection.BuildSlicer(session);
        var groups = await slicer.SliceAsync(events).ConfigureAwait(false);

        var identitySetter = new NulloIdentitySetter<T, TId>();
        var results = new List<T>();

        foreach (var groupObject in groups)
        {
            if (groupObject is not SliceGroup<T, TId> group)
            {
                continue;
            }

            await projection.EnrichEventsAsync(group, session, token).ConfigureAwait(false);

            foreach (var slice in group.Slices)
            {
                var (aggregate, action) = await projection
                    .DetermineActionAsync(session, default, slice.Id, identitySetter, slice.Events(), token)
                    .ConfigureAwait(false);

                if (action == ActionType.Delete || aggregate is null)
                {
                    continue;
                }

                // The fold uses a Nullo identity setter, so stamp the aggregate's own id from the slice.
                QueryEventStore.TrySetIdentity(aggregate, slice.Id);
                results.Add(aggregate);
            }
        }

        return results;
    }

    // Walk the projection's base chain for the closed JasperFxAggregationProjectionBase<TDoc, TId, ...>
    // and return its TId argument.
    private static Type? findAggregateIdType(Type projectionType)
    {
        var type = projectionType;
        while (type != null && type != typeof(object))
        {
            if (type.IsGenericType
                && type.GetGenericTypeDefinition() == typeof(JasperFxAggregationProjectionBase<,,,>))
            {
                return type.GetGenericArguments()[1];
            }

            type = type.BaseType;
        }

        return null;
    }
}
