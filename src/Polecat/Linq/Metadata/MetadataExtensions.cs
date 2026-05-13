using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace Polecat.Linq.Metadata;

/// <summary>
///     LINQ extension methods for filtering documents by metadata columns.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2060:DynamicallyAccessedMembers",
    Justification = "Class-level: MetadataExtensions' own marker methods (ModifiedSince, ModifiedBefore) are referenced by Expression.Call(...). Markers preserved by the class.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: LINQ expression construction (MakeGenericMethod on markers) requires runtime code generation.")]
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
