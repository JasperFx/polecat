using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace Polecat.Linq.Metadata;

/// <summary>
///     LINQ extension methods for filtering documents by the created_at metadata column.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2060:DynamicallyAccessedMembers",
    Justification = "Class-level: CreatedAtExtensions' own marker methods (CreatedSince, CreatedBefore) are referenced by Expression.Call(...). Markers preserved by the class.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: LINQ expression construction (MakeGenericMethod on markers) requires runtime code generation.")]
public static class CreatedAtExtensions
{
    private static readonly MethodInfo CreatedSinceMethodInfo =
        typeof(CreatedAtExtensions).GetMethod(nameof(CreatedSince))!;

    private static readonly MethodInfo CreatedBeforeMethodInfo =
        typeof(CreatedAtExtensions).GetMethod(nameof(CreatedBefore))!;

    /// <summary>
    ///     Filter to documents created on or after the given timestamp.
    /// </summary>
    public static IQueryable<T> CreatedSince<T>(this IQueryable<T> queryable, DateTimeOffset timestamp)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                CreatedSinceMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression,
                Expression.Constant(timestamp)));
    }

    /// <summary>
    ///     Filter to documents created before the given timestamp.
    /// </summary>
    public static IQueryable<T> CreatedBefore<T>(this IQueryable<T> queryable, DateTimeOffset timestamp)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                CreatedBeforeMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression,
                Expression.Constant(timestamp)));
    }
}
