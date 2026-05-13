using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace Polecat;

/// <summary>
///     Polecat-specific LINQ extension methods for use in Where() clauses.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2060:DynamicallyAccessedMembers",
    Justification = "Class-level: LinqExtensions' own marker methods (AnyTenant, TenantIsOneOf) are referenced by Expression.Call(...). Markers are preserved by the class itself.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: LINQ expression construction (MakeGenericMethod on markers) requires runtime code generation.")]
public static class LinqExtensions
{
    /// <summary>
    ///     Tests whether a property value matches any of the supplied values.
    ///     Translated to SQL IN clause.
    /// </summary>
    public static bool IsOneOf<T>(this T value, params T[] matches) => matches.Contains(value);

    /// <summary>
    ///     Tests whether a property value matches any of the supplied values.
    ///     Translated to SQL IN clause.
    /// </summary>
    public static bool IsOneOf<T>(this T value, IList<T> matches) => matches.Contains(value);

    /// <summary>
    ///     Synonym for IsOneOf. Tests whether a property value matches any of the supplied values.
    /// </summary>
    public static bool In<T>(this T value, params T[] matches) => matches.Contains(value);

    /// <summary>
    ///     Synonym for IsOneOf. Tests whether a property value matches any of the supplied values.
    /// </summary>
    public static bool In<T>(this T value, IList<T> matches) => matches.Contains(value);

    /// <summary>
    ///     Tests whether a collection property is empty.
    ///     Translated to OPENJSON count check.
    /// </summary>
    public static bool IsEmpty<T>(this IEnumerable<T> enumerable) => !enumerable.Any();

    private static readonly MethodInfo AnyTenantMethodInfo =
        typeof(LinqExtensions).GetMethod(nameof(AnyTenant))!;

    private static readonly MethodInfo TenantIsOneOfMethodInfo =
        typeof(LinqExtensions).GetMethod(nameof(TenantIsOneOf))!;

    /// <summary>
    ///     Query across all tenants, removing the tenant_id filter.
    /// </summary>
    public static IQueryable<T> AnyTenant<T>(this IQueryable<T> queryable)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                AnyTenantMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression));
    }

    /// <summary>
    ///     Filter to specific tenants by tenant_id.
    /// </summary>
    public static IQueryable<T> TenantIsOneOf<T>(this IQueryable<T> queryable, params string[] tenantIds)
    {
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                TenantIsOneOfMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression,
                Expression.Constant(tenantIds)));
    }
}
