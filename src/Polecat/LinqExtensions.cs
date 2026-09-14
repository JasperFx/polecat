using Polecat.Linq.SqlGeneration;
using Polecat.Linq;
using JasperFx.Events.Vectors;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Events;

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
    /// <summary>
    ///     Full-text search over a member declared with
    ///     <c>Schema.For&lt;T&gt;().FullTextIndex(...)</c>: every term in
    ///     <paramref name="searchTerm" /> has to appear in the member, in any order.
    /// </summary>
    /// <remarks>
    ///     Mirrors Marten's operator of the same name. The <c>regConfig</c> overloads Marten offers are
    ///     deliberately absent — a PostgreSQL text-search configuration has no counterpart against an
    ///     index Polecat tokenizes itself, and an overload that took one and ignored it would be worse
    ///     than its absence. Only callable inside a LINQ <c>Where</c>.
    /// </remarks>
    public static bool PlainTextSearch(this string member, string searchTerm)
        => throw new NotSupportedException(
            "PlainTextSearch() is a LINQ marker and only has meaning inside a Where() against a "
            + "Polecat document query.");

    /// <summary>
    ///     Full-text search for the terms of <paramref name="searchTerm" /> appearing in order and
    ///     adjacent to each other.
    /// </summary>
    /// <remarks>
    ///     Mirrors Marten's operator of the same name, and is what the token positions gh-611 stores
    ///     are for. Only callable inside a LINQ <c>Where</c>.
    /// </remarks>
    public static bool PhraseSearch(this string member, string searchTerm)
        => throw new NotSupportedException(
            "PhraseSearch() is a LINQ marker and only has meaning inside a Where() against a "
            + "Polecat document query.");

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

    /// <summary>
    ///     LINQ filter over an event query (e.g. <c>session.Events.QueryAllRawEvents()</c>) that matches only
    ///     events carrying the given DCB tag value. Composes into the same <c>Where()</c> as ordinary event
    ///     predicates (timestamp, event type, stream), so one query can express "these events, matching these
    ///     tags". <typeparamref name="TTag"/> must be a registered tag type (see <c>RegisterTagType&lt;TTag&gt;()</c>).
    ///     AND-ing several <c>HasTag</c> calls with normal predicates is supported; for OR-across-tags or the
    ///     richer event-type interplay, use the <c>EventTagQuery</c> builder with <c>QueryByTagsAsync</c> instead.
    ///     This is a marker method recognized by the LINQ provider and cannot be invoked directly.
    /// </summary>
    public static bool HasTag<TTag>(this IEvent e, TTag value) where TTag : notnull
    {
        throw new NotSupportedException(
            "IEvent.HasTag<TTag>() is a marker method for LINQ event queries and cannot be invoked directly. Use it inside session.Events.QueryAllRawEvents().Where(...).");
    }

    /// <summary>
    ///     The lossy sibling of <see cref="HasTag{TTag}" />: matches events carrying the given tag whose
    ///     value, <em>rendered as a string</em>, equals <paramref name="value" /> case-insensitively.
    ///     Backs <c>EventQuery.TagValues</c> (jasperfx#801 / polecat#575), whose name/value dictionary
    ///     form cannot carry a typed value.
    ///     This is a marker method recognized by the LINQ provider and cannot be invoked directly.
    /// </summary>
    /// <remarks>
    ///     Deliberately NOT expressed as <c>HasTag&lt;TTag&gt;</c> with the string parsed into
    ///     <typeparamref name="TTag" />. The two comparisons genuinely differ: typed equality on a Guid
    ///     tag distinguishes nothing between <c>"A1B2…"</c> and <c>"a1b2…"</c> only because Guid parsing
    ///     normalizes, while an <c>int</c> tag would reject <c>"007"</c> that string comparison accepts,
    ///     and a string tag is case-sensitive under typed equality and case-insensitive here. The lossy
    ///     form's contract is the string form, so it compares the string form.
    /// </remarks>
    public static bool HasTagValue<TTag>(this IEvent e, string value) where TTag : notnull
    {
        throw new NotSupportedException(
            "IEvent.HasTagValue<TTag>() is a marker method for LINQ event queries and cannot be invoked directly. Use it inside session.Events.QueryAllRawEvents().Where(...).");
    }

    /// <summary>
    ///     Order a LINQ query by distance from <paramref name="query" />, nearest first — so a vector
    ///     ordering can be COMPOSED with ordinary filters, paging and projections.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>This is what widening <c>Statement.OrderBys</c> to fragments bought.</b> An ordering
    ///         key used to be text appended verbatim, and a vector distance has the query vector inside
    ///         it, so it could not be expressed from an ordering position at all — which is why
    ///         <c>VectorSearchAsync</c> is written against raw SQL.
    ///     </para>
    ///     <para>
    ///         <c>VectorSearchAsync</c> stays, and the two are not redundant: it is the whole search in
    ///         one call and returns scores. This composes, so
    ///         <c>Query&lt;T&gt;().Where(x =&gt; x.Team == "red").OrderByVectorDistance(...).Take(5)</c>
    ///         is expressible, which raw SQL is not.
    ///     </para>
    ///     <para>
    ///         ⚠️ Refused by name, before any SQL runs, for a type with no declared vector index, a
    ///         member that is not the declared one, or a query vector of another length — the SAME
    ///         refusals <c>VectorSearchAsync</c> makes, because both call one resolver.
    ///     </para>
    /// </remarks>
    public static IQueryable<T> OrderByVectorDistance<T>(
        this IQueryable<T> queryable,
        Expression<Func<T, object?>> member,
        ReadOnlyMemory<float> query,
        DistanceFunction? distance = null) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(queryable);

        if (queryable.Provider is not PolecatLinqQueryProvider provider)
        {
            throw new NotSupportedException(
                "OrderByVectorDistance is only meaningful over a Polecat session's Query<T>().");
        }

        // Validated HERE rather than in the parser, because the refusals need the document's mapping
        // and this is the last place that can reach a session. A bad call therefore throws at the call
        // site rather than when the query is finally enumerated.
        var index = VectorSearchExtensions.ResolveIndex(provider.Session, member, query);
        var ordering = new OrderByClause(new VectorDistanceOrdering(index, query, distance), false, null);

        // The tree carries the BUILT ordering rather than the arguments that produced it, because the
        // parser cannot rebuild it: resolving the declaration needs the document's mapping and the
        // parser has only a member resolver. A marker of its own rather than reusing this method's
        // signature, so the expression's arity matches what the parser reads.
        return queryable.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                OrderedByVectorDistanceMethodInfo.MakeGenericMethod(typeof(T)),
                queryable.Expression,
                Expression.Constant(ordering)));
    }

    private static readonly MethodInfo OrderedByVectorDistanceMethodInfo =
        typeof(LinqExtensions).GetMethod(nameof(OrderedByVectorDistance),
            BindingFlags.Public | BindingFlags.Static)!;

    /// <summary>
    ///     Marker carrying a built vector ordering into the query tree. Public only because
    ///     <see cref="Expression.Call(Expression, MethodInfo, Expression[])" /> needs to find it;
    ///     calling it directly is meaningless.
    /// </summary>
    public static IQueryable<T> OrderedByVectorDistance<T>(this IQueryable<T> queryable, object ordering)
        where T : notnull
        => throw new NotSupportedException(
            "OrderedByVectorDistance is a marker for LINQ translation. Use OrderByVectorDistance(...).");

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
