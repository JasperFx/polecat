using System.Linq.Expressions;

namespace Polecat.Batching;

/// <summary>
///     LINQ-like query builder within a batch. Collects filters and returns
///     a Task that will be fulfilled when Execute() is called.
/// </summary>
public interface IBatchedQueryable<T> where T : class
{
    IBatchedQueryable<T> Where(Expression<Func<T, bool>> predicate);

    Task<IReadOnlyList<T>> ToList();
    Task<int> Count();
    Task<bool> Any();
    Task<T?> FirstOrDefault();
}
