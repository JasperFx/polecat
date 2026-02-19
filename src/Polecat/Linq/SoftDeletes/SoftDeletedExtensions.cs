using System.Linq.Expressions;
using System.Reflection;

namespace Polecat.Linq.SoftDeletes;

/// <summary>
///     LINQ extension methods for querying soft-deleted documents.
/// </summary>
public static class SoftDeletedExtensions
{
    private static readonly MethodInfo MaybeDeletedMethodInfo =
        typeof(SoftDeletedExtensions).GetMethod(nameof(MaybeDeleted))!;

    private static readonly MethodInfo IsDeletedMethodInfo =
        typeof(SoftDeletedExtensions).GetMethod(nameof(IsDeleted))!;

    private static readonly MethodInfo DeletedSinceMethodInfo =
        typeof(SoftDeletedExtensions).GetMethod(nameof(DeletedSince))!;

    private static readonly MethodInfo DeletedBeforeMethodInfo =
        typeof(SoftDeletedExtensions).GetMethod(nameof(DeletedBefore))!;

    /// <summary>
    ///     Include both deleted and non-deleted documents in the query.
    ///     Removes the automatic is_deleted = 0 filter.
    /// </summary>
    public static IQueryable<T> MaybeDeleted<T>(this IQueryable<T> queryable)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                MaybeDeletedMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression));
    }

    /// <summary>
    ///     Query only soft-deleted documents (is_deleted = 1).
    /// </summary>
    public static IQueryable<T> IsDeleted<T>(this IQueryable<T> queryable)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                IsDeletedMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression));
    }

    /// <summary>
    ///     Query soft-deleted documents that were deleted on or after the given timestamp.
    /// </summary>
    public static IQueryable<T> DeletedSince<T>(this IQueryable<T> queryable, DateTimeOffset timestamp)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                DeletedSinceMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression,
                Expression.Constant(timestamp)));
    }

    /// <summary>
    ///     Query soft-deleted documents that were deleted before the given timestamp.
    /// </summary>
    public static IQueryable<T> DeletedBefore<T>(this IQueryable<T> queryable, DateTimeOffset timestamp)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                DeletedBeforeMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression,
                Expression.Constant(timestamp)));
    }
}
