using System.Linq.Expressions;
using System.Reflection;

namespace Polecat.Linq;

/// <summary>
///     LINQ extension to wait for async projections to catch up before executing a query.
/// </summary>
public static class NonStaleDataExtensions
{
    private static readonly MethodInfo QueryForNonStaleDataMethodInfo =
        typeof(NonStaleDataExtensions).GetMethod(nameof(QueryForNonStaleData), [typeof(IQueryable<>).MakeGenericType(Type.MakeGenericMethodParameter(0)), typeof(TimeSpan)])!;

    private static readonly MethodInfo QueryForNonStaleDataNoTimeoutMethodInfo =
        typeof(NonStaleDataExtensions).GetMethod(nameof(QueryForNonStaleData), [typeof(IQueryable<>).MakeGenericType(Type.MakeGenericMethodParameter(0))])!;

    /// <summary>
    ///     Wait for all async projections to catch up to the high water mark
    ///     before executing this query. Default timeout is 5 seconds.
    /// </summary>
    public static IQueryable<T> QueryForNonStaleData<T>(this IQueryable<T> queryable)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                QueryForNonStaleDataNoTimeoutMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression));
    }

    /// <summary>
    ///     Wait for all async projections to catch up to the high water mark
    ///     before executing this query, with a specified timeout.
    /// </summary>
    public static IQueryable<T> QueryForNonStaleData<T>(this IQueryable<T> queryable, TimeSpan timeout)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                QueryForNonStaleDataMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression,
                Expression.Constant(timeout)));
    }
}
