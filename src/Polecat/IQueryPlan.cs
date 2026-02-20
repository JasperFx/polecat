using Polecat.Batching;
using Polecat.Internal.Batching;
using Polecat.Linq;

namespace Polecat;

/// <summary>
///     Polecat's concept of the "Specification" pattern for reusable queries.
///     Encapsulates a query that can be executed against an IQuerySession.
/// </summary>
/// <typeparam name="T">The result type of the query</typeparam>
public interface IQueryPlan<T>
{
    Task<T> Fetch(IQuerySession session, CancellationToken token);
}

/// <summary>
///     Polecat's concept of the "Specification" pattern for reusable queries
///     within batched queries. Encapsulates a query that can be executed as part
///     of an IBatchedQuery.
/// </summary>
/// <typeparam name="T">The result type of the query</typeparam>
public interface IBatchQueryPlan<T>
{
    Task<T> Fetch(IBatchedQuery query);
}

/// <summary>
///     Base class for query plans that return a list of items. Implements both
///     IQueryPlan and IBatchQueryPlan so it can be used with QueryByPlanAsync()
///     and batch.QueryByPlan().
/// </summary>
/// <typeparam name="T">The document type to query</typeparam>
public abstract class QueryListPlan<T> : IQueryPlan<IReadOnlyList<T>>, IBatchQueryPlan<IReadOnlyList<T>>
    where T : class
{
    /// <summary>
    ///     Define the query by returning an IQueryable from the session.
    /// </summary>
    public abstract IQueryable<T> Query(IQuerySession session);

    async Task<IReadOnlyList<T>> IQueryPlan<IReadOnlyList<T>>.Fetch(IQuerySession session, CancellationToken token)
    {
        return await Query(session).ToListAsync(token);
    }

    Task<IReadOnlyList<T>> IBatchQueryPlan<IReadOnlyList<T>>.Fetch(IBatchedQuery query)
    {
        if (query is BatchedQuery batch)
        {
            return batch.AddQueryableList(Query(query.Parent));
        }

        // Fallback for non-Polecat batch implementations
        return Query(query.Parent).ToListAsync();
    }
}
