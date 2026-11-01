using Polecat.Linq.Parsing;
using Polecat.Linq.SqlGeneration;
using Polecat.Metadata;
using Polecat.Storage;

namespace Polecat.Linq;

/// <summary>
///     The predicates a document query carries without the caller writing them: tenant, soft
///     delete, <c>doc_type</c> and the metadata windows.
/// </summary>
/// <remarks>
///     <para>
///         <b>This is the one place they are decided.</b> They used to be appended beside each
///         execution path, and the copies drifted: GroupBy kept the tenant filter and lost the rest,
///         every JSON path lost <c>doc_type</c>, joins never heard of <c>MaybeDeleted()</c> and only
///         the main path honored <c>DeletedSince()</c>. A path that forgets one does not fail. It
///         returns rows the caller excluded, which is the worst way for a filter to go wrong (#836, and
///         before it #683 and #723).
///     </para>
///     <para>
///         <c>PolecatLinqQueryProvider.Parse</c> applies them as it parses, so a new execution path
///         cannot get a statement without them. The two callers that do not go through it,
///         a join's two sides and the batched queryable, come here directly.
///     </para>
///     <para>
///         <c>QueryForNonStaleData()</c> is the same kind of promise and lives beside this in
///         <c>PolecatLinqQueryProvider.PrepareAsync</c>, because it is a wait rather than a predicate.
///     </para>
/// </remarks>
internal static class ImplicitFilters
{
    /// <summary>
    ///     The implicit predicates for a query over <paramref name="documentType" />.
    /// </summary>
    /// <param name="mapping">The mapping of the table being read.</param>
    /// <param name="documentType">The queried type, which is a subclass for <c>Query&lt;TSub&gt;()</c>.</param>
    /// <param name="tenantId">The tenant an unqualified conjoined query is scoped to.</param>
    /// <param name="operators">The query's soft-delete and metadata operators, if it has any.</param>
    /// <param name="tenancyOperators">
    ///     The query's tenancy operators, when they came from somewhere other than
    ///     <paramref name="operators" />. A join's <c>AnyTenant()</c> on the outer side covers both.
    /// </param>
    /// <param name="alias">The table alias to qualify columns with, for a query over more than one table.</param>
    public static IEnumerable<ISqlFragment> For(
        DocumentMapping mapping,
        Type documentType,
        string tenantId,
        LinqQueryParser? operators = null,
        LinqQueryParser? tenancyOperators = null,
        string? alias = null)
    {
        string Column(string name) => alias is null ? name : $"{alias}.{name}";

        var tenancy = tenancyOperators ?? operators;

        // #234: single-tenant tables have no tenant_id column, so no implicit tenant filter is
        // applied — every document is under the default tenant. Explicit AnyTenant()/TenantIsOneOf()
        // are likewise no-ops there.
        //
        // ⚠️ Per MAPPING, not per store. This read Options.Events.TenancyStyle until #682, which was
        // correct only while document tenancy was store-wide and inherited from the event store. Once
        // Schema.For<T>().MultiTenanted() existed, a conjoined document in a single-tenanted store got
        // its tenant_id column and then had no filter on any LINQ shape, every tenant reading every
        // other tenant's rows, while the keyed load path stayed correctly scoped. That asymmetry is what
        // DocumentConjoinedTenancyCompliance caught, and a join can have a conjoined side and a
        // single-tenanted one, so there is no single answer for a query.
        if (mapping.TenancyStyle == TenancyStyle.Conjoined && tenancy is not { IsAnyTenant: true })
        {
            yield return tenancy?.TenantIds is { } tenantIds
                ? new TenantInFilter(tenantIds, Column("tenant_id"))
                : new ComparisonFilter(Column("tenant_id"), "=", tenantId);
        }

        if (mapping.DeleteStyle == DeleteStyle.SoftDelete && operators is not { IsMaybeDeleted: true })
        {
            var deleted = operators is { IsDeletedOnly: true } ? 1 : 0;
            yield return new LiteralSqlFragment($"{Column("is_deleted")} = {deleted}");

            if (operators?.DeletedSinceTimestamp is { } deletedSince)
            {
                yield return new ComparisonFilter(Column("deleted_at"), ">=", deletedSince);
            }

            if (operators?.DeletedBeforeTimestamp is { } deletedBefore)
            {
                yield return new ComparisonFilter(Column("deleted_at"), "<", deletedBefore);
            }
        }

        if (operators?.ModifiedSinceTimestamp is { } modifiedSince)
        {
            yield return new ComparisonFilter(Column("last_modified"), ">=", modifiedSince);
        }

        if (operators?.ModifiedBeforeTimestamp is { } modifiedBefore)
        {
            yield return new ComparisonFilter(Column("last_modified"), "<", modifiedBefore);
        }

        if (operators?.CreatedSinceTimestamp is { } createdSince)
        {
            yield return new ComparisonFilter(Column("created_at"), ">=", createdSince);
        }

        if (operators?.CreatedBeforeTimestamp is { } createdBefore)
        {
            yield return new ComparisonFilter(Column("created_at"), "<", createdBefore);
        }

        // A subclass shares its root's table, so Query<TSub>() has to narrow to its own rows.
        if (mapping.IsHierarchy() && documentType != mapping.DocumentType)
        {
            yield return new ComparisonFilter(Column("doc_type"), "=", mapping.AliasFor(documentType));
        }
    }
}
