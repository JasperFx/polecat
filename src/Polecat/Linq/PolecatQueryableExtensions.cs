using System.Linq.Expressions;

namespace Polecat.Linq;

/// <summary>
///     Async LINQ extension methods for Polecat queryables.
/// </summary>
public static class PolecatQueryableExtensions
{
    /// <summary>
    ///     Asynchronously executes the query and returns results as a list.
    /// </summary>
    public static async Task<IReadOnlyList<T>> ToListAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        return await provider.ExecuteAsync<IReadOnlyList<T>>(queryable.Expression, token);
    }

    /// <summary>
    ///     Asynchronously returns the first element of a sequence.
    /// </summary>
    public static async Task<T> FirstAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.First));
        return (await provider.ExecuteAsync<T?>(expression, token))!;
    }

    /// <summary>
    ///     Asynchronously returns the first element of a sequence, or a default value.
    /// </summary>
    public static async Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.FirstOrDefault));
        return await provider.ExecuteAsync<T?>(expression, token);
    }

    /// <summary>
    ///     Asynchronously returns the first element of a sequence that satisfies a condition.
    /// </summary>
    public static async Task<T> FirstAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, bool>> predicate,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.First), predicate);
        return (await provider.ExecuteAsync<T?>(expression, token))!;
    }

    /// <summary>
    ///     Asynchronously returns the first element of a sequence that satisfies a condition, or default.
    /// </summary>
    public static async Task<T?> FirstOrDefaultAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, bool>> predicate,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.FirstOrDefault), predicate);
        return await provider.ExecuteAsync<T?>(expression, token);
    }

    /// <summary>
    ///     Asynchronously returns the only element of a sequence.
    /// </summary>
    public static async Task<T> SingleAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.Single));
        return (await provider.ExecuteAsync<T?>(expression, token))!;
    }

    /// <summary>
    ///     Asynchronously returns the only element of a sequence, or a default value.
    /// </summary>
    public static async Task<T?> SingleOrDefaultAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.SingleOrDefault));
        return await provider.ExecuteAsync<T?>(expression, token);
    }

    /// <summary>
    ///     Asynchronously returns the number of elements in a sequence.
    /// </summary>
    public static async Task<int> CountAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.Count));
        return await provider.ExecuteAsync<int>(expression, token);
    }

    /// <summary>
    ///     Asynchronously returns the number of elements that satisfy a condition.
    /// </summary>
    public static async Task<int> CountAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, bool>> predicate,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.Count), predicate);
        return await provider.ExecuteAsync<int>(expression, token);
    }

    /// <summary>
    ///     Asynchronously returns the long number of elements in a sequence.
    /// </summary>
    public static async Task<long> LongCountAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.LongCount));
        return await provider.ExecuteAsync<long>(expression, token);
    }

    /// <summary>
    ///     Asynchronously determines whether a sequence contains any elements.
    /// </summary>
    public static async Task<bool> AnyAsync<T>(
        this IQueryable<T> queryable, CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.Any));
        return await provider.ExecuteAsync<bool>(expression, token);
    }

    /// <summary>
    ///     Asynchronously determines whether any element satisfies a condition.
    /// </summary>
    public static async Task<bool> AnyAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, bool>> predicate,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpression(queryable, nameof(Queryable.Any), predicate);
        return await provider.ExecuteAsync<bool>(expression, token);
    }

    /// <summary>
    ///     Asynchronously computes the sum of a sequence.
    /// </summary>
    public static async Task<int> SumAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, int>> selector,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpressionWithSelector(queryable, "Sum", selector);
        return await provider.ExecuteAsync<int>(expression, token);
    }

    /// <summary>
    ///     Asynchronously computes the sum of a long sequence.
    /// </summary>
    public static async Task<long> SumAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, long>> selector,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpressionWithSelector(queryable, "Sum", selector);
        return await provider.ExecuteAsync<long>(expression, token);
    }

    /// <summary>
    ///     Asynchronously computes the sum of a decimal sequence.
    /// </summary>
    public static async Task<decimal> SumAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, decimal>> selector,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpressionWithSelector(queryable, "Sum", selector);
        return await provider.ExecuteAsync<decimal>(expression, token);
    }

    /// <summary>
    ///     Asynchronously computes the sum of a double sequence.
    /// </summary>
    public static async Task<double> SumAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, double>> selector,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpressionWithSelector(queryable, "Sum", selector);
        return await provider.ExecuteAsync<double>(expression, token);
    }

    /// <summary>
    ///     Asynchronously returns the minimum value.
    /// </summary>
    public static async Task<TResult> MinAsync<T, TResult>(
        this IQueryable<T> queryable, Expression<Func<T, TResult>> selector,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpressionWithSelector(queryable, "Min", selector);
        return await provider.ExecuteAsync<TResult>(expression, token);
    }

    /// <summary>
    ///     Asynchronously returns the maximum value.
    /// </summary>
    public static async Task<TResult> MaxAsync<T, TResult>(
        this IQueryable<T> queryable, Expression<Func<T, TResult>> selector,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpressionWithSelector(queryable, "Max", selector);
        return await provider.ExecuteAsync<TResult>(expression, token);
    }

    /// <summary>
    ///     Asynchronously computes the average.
    /// </summary>
    public static async Task<double> AverageAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, int>> selector,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpressionWithSelector(queryable, "Average", selector);
        return await provider.ExecuteAsync<double>(expression, token);
    }

    /// <summary>
    ///     Asynchronously computes the average for double.
    /// </summary>
    public static async Task<double> AverageAsync<T>(
        this IQueryable<T> queryable, Expression<Func<T, double>> selector,
        CancellationToken token = default)
    {
        var provider = GetPolecatProvider(queryable);
        var expression = BuildMethodCallExpressionWithSelector(queryable, "Average", selector);
        return await provider.ExecuteAsync<double>(expression, token);
    }

    private static PolecatLinqQueryProvider GetPolecatProvider<T>(IQueryable<T> queryable)
    {
        if (queryable.Provider is PolecatLinqQueryProvider provider)
        {
            return provider;
        }

        throw new InvalidOperationException(
            "This extension method can only be used with Polecat IQueryable instances. " +
            "Use session.Query<T>() to create a Polecat queryable.");
    }

    private static Expression BuildMethodCallExpression<T>(
        IQueryable<T> queryable, string methodName)
    {
        return Expression.Call(
            typeof(Queryable),
            methodName,
            [typeof(T)],
            queryable.Expression);
    }

    private static Expression BuildMethodCallExpression<T>(
        IQueryable<T> queryable, string methodName, Expression<Func<T, bool>> predicate)
    {
        return Expression.Call(
            typeof(Queryable),
            methodName,
            [typeof(T)],
            queryable.Expression,
            predicate);
    }

    private static Expression BuildMethodCallExpressionWithSelector<T, TResult>(
        IQueryable<T> queryable, string methodName, Expression<Func<T, TResult>> selector)
    {
        // Min/Max take two type args: TSource, TResult
        // Sum/Average take one type arg: TSource
        return methodName is "Min" or "Max"
            ? Expression.Call(typeof(Queryable), methodName,
                [typeof(T), typeof(TResult)], queryable.Expression, selector)
            : Expression.Call(typeof(Queryable), methodName,
                [typeof(T)], queryable.Expression, selector);
    }
}
