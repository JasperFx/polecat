using System.Linq.Expressions;
using System.Reflection;

namespace Polecat.Linq.Metadata;

/// <summary>
///     LINQ extension methods for filtering documents by metadata columns.
/// </summary>
public static class MetadataExtensions
{
    private static readonly MethodInfo ModifiedSinceMethodInfo =
        typeof(MetadataExtensions).GetMethod(nameof(ModifiedSince))!;

    private static readonly MethodInfo ModifiedBeforeMethodInfo =
        typeof(MetadataExtensions).GetMethod(nameof(ModifiedBefore))!;

    /// <summary>
    ///     Filter to documents modified on or after the given timestamp.
    /// </summary>
    public static IQueryable<T> ModifiedSince<T>(this IQueryable<T> queryable, DateTimeOffset timestamp)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                ModifiedSinceMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression,
                Expression.Constant(timestamp)));
    }

    /// <summary>
    ///     Filter to documents modified before the given timestamp.
    /// </summary>
    public static IQueryable<T> ModifiedBefore<T>(this IQueryable<T> queryable, DateTimeOffset timestamp)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                ModifiedBeforeMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression,
                Expression.Constant(timestamp)));
    }
}
