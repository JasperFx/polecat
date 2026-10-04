using JasperFx;
using Weasel.SqlServer;

namespace Polecat.Storage;

/// <summary>
///     The predicates every search surface owes the document table: soft deletion, conjoined
///     tenancy, and — since #723 — the hierarchy discriminator.
/// </summary>
/// <remarks>
///     <para>
///         <b>One place rather than three, and #723 is what three cost.</b>
///         <see cref="VectorSearchExtensions" />, <see cref="FullTextSearchExtensions" /> and —
///         through both of its legs — <see cref="HybridSearchExtensions" /> each built their own
///         WHERE. All three emitted the soft-delete and tenant predicates and <b>none</b> of them
///         emitted <c>doc_type</c>, so a search for a sub-class scanned the shared hierarchy table
///         unfiltered and handed every sibling and base-type row back <i>deserialized as the
///         sub-class the caller asked for</i>. A fourth search surface written the same way would
///         have missed it again.
///     </para>
///     <para>
///         ⚠️ <b>The discriminator predicate is deliberately the same one the LINQ path emits</b> —
///         <c>PolecatLinqQueryProvider</c> adds <c>doc_type = &lt;alias&gt;</c> whenever the requested
///         type is not the mapping's root, and calls <see cref="DocumentMapping.AliasFor" /> to get
///         it. Two read paths over one store disagreeing about what a sub-class <i>means</i> is the
///         bug (#723), and was the shape of #683 before it, so the answer is the same predicate
///         rather than a second one that is merely similar — including the refusal by name when the
///         requested type is not a registered sub-class.
///     </para>
///     <para>
///         <b>Two spellings of one filter, because the two surfaces bind parameters differently.</b>
///         The vector search composes through a <see cref="BatchBuilder" />; the full-text search
///         assembles a '?'-placeholder string whose parameters are bound positionally afterwards.
///         <see cref="SearchFilter.Sql" /> therefore stops exactly where its parameter goes, so
///         <see cref="Apply" /> can append it to a builder and
///         <see cref="SearchFilter.ToPlaceholderSql" /> can render it for the other route — and
///         neither surface needs to know how the other binds.
///     </para>
///     <para>
///         <b>What this deliberately does NOT do</b> is resolve a row to its concrete type. A search
///         for the <i>root</i> of a hierarchy still returns the whole hierarchy materialized as the
///         root, where <c>Query&lt;Root&gt;()</c> resolves each row through its <c>doc_type</c>.
///         That is a separate decision from the sub-class filter with a different blast radius —
///         #723 point 3, and marten#5440's other half — and bundling it into a filter fix would
///         change what a root search returns without saying so.
///     </para>
/// </remarks>
internal static class DocumentSearchFilters
{
    /// <summary>
    ///     One predicate, and the single parameter value it binds — or <c>null</c> when it binds
    ///     none. <see cref="Sql" /> ends where the parameter goes, so it is never a complete
    ///     statement on its own when <see cref="Parameter" /> is set.
    /// </summary>
    internal readonly record struct SearchFilter(string Sql, object? Parameter)
    {
        /// <summary>
        ///     The filter rendered for the '?'-placeholder route, which numbers its parameters by
        ///     counting placeholder characters in the text.
        /// </summary>
        internal string ToPlaceholderSql(char placeholder = '?')
            => Parameter is null ? Sql : Sql + placeholder;
    }

    /// <summary>
    ///     The implicit predicates for a search over <paramref name="requestedType" />, in a stable
    ///     order so the positional route can bind <see cref="SearchFilter.Parameter" /> by iterating
    ///     this list.
    /// </summary>
    /// <param name="mapping">
    ///     The mapping the provider registry resolved — for a sub-class that is the <b>root's</b>
    ///     mapping, which is why <paramref name="requestedType" /> has to be passed separately.
    /// </param>
    /// <param name="requestedType">The type the caller asked for, which may be a sub-class.</param>
    /// <param name="prefix">
    ///     A table qualifier for the columns, such as <c>"d."</c>. Empty where the statement selects
    ///     from the document table unaliased.
    /// </param>
    internal static IReadOnlyList<SearchFilter> For(
        DocumentMapping mapping, Type requestedType, string tenantId, string prefix = "")
    {
        var filters = new List<SearchFilter>(3);

        if (mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            filters.Add(new SearchFilter($"{prefix}is_deleted = 0", null));
        }

        if (mapping.TenancyStyle == TenancyStyle.Conjoined)
        {
            filters.Add(new SearchFilter($"{prefix}tenant_id = ", tenantId));
        }

        if (mapping.IsHierarchy() && requestedType != mapping.DocumentType)
        {
            filters.Add(new SearchFilter($"{prefix}doc_type = ", mapping.AliasFor(requestedType)));
        }

        return filters;
    }

    /// <summary>
    ///     Append the filters to a statement that already has a WHERE clause, each as
    ///     <c>AND &lt;predicate&gt;</c>.
    /// </summary>
    internal static void Apply(IReadOnlyList<SearchFilter> filters, ICommandBuilder builder)
    {
        foreach (var filter in filters)
        {
            builder.Append(" AND ");
            builder.Append(filter.Sql);
            if (filter.Parameter is not null) builder.AppendParameter(filter.Parameter);
        }
    }
}
