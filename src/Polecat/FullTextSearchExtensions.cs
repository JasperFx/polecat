using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;
using Polecat.Storage.FullText;
using Weasel.SqlServer;

namespace Polecat;

/// <summary>
///     Ranked full-text search over a member declared with
///     <c>Schema.For&lt;T&gt;().FullTextIndex(...)</c>, scored with Okapi BM25.
/// </summary>
/// <remarks>
///     <para>
///         <b>Separate from the LINQ operators on purpose.</b> <c>Where(x =&gt; x.Body.PlainTextSearch(...))</c>
///         answers which documents match; this answers how well, which a <c>Where</c> has nowhere to
///         put. A ranked search is also what a hybrid search fuses (gh-612), so the scored overload
///         exists for the same reason <c>VectorSearchWithScoresAsync</c> does.
///     </para>
///     <para>
///         <b>BM25 needs no extra schema.</b> Every input is derivable from the token rows already
///         stored: term frequency is a count per document and term, document length a count per
///         document, document frequency a distinct count per term, and the corpus size and average
///         length are aggregates over those. The cost is that a query computes them — there is no
///         maintained statistics table, so ranking reads the whole token table for the member. That is
///         the right first trade: correct before fast, and a materialized statistics table is a
///         change this can absorb later without moving the API.
///     </para>
///     <para>
///         <b>Scores are comparable within one result set and not between two.</b> BM25's IDF term
///         depends on the corpus, so a document's score moves as other documents are written. Use it
///         to order, or as a relative floor within a search — not as a stored measure of relevance.
///     </para>
/// </remarks>
public static class FullTextSearchExtensions
{
    /// <summary>
    ///     The <paramref name="limit" /> documents matching <paramref name="text" /> best, most
    ///     relevant first.
    /// </summary>
    /// <param name="filter">
    ///     An optional predicate, applied BEFORE <paramref name="limit" /> — so the result is the best
    ///     <paramref name="limit" /> of the filtered set rather than the filtered remains of the best
    ///     <paramref name="limit" /> (#633). It supports and refuses exactly what
    ///     <c>Query&lt;T&gt;().Where(...)</c> does, because it is parsed by the same parser.
    /// </param>
    public static async Task<IReadOnlyList<T>> FullTextSearchAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        string text,
        int limit = 10,
        FullTextSearchOptions? options = null,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken token = default) where T : notnull
    {
        var built = await BuildAsync(session, member, text, limit, options, filter, token).ConfigureAwait(false);
        if (built is null) return [];

        return await ((QuerySession)session).QueryByBatchAsync<T>(built, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     The same search, each document paired with its BM25 score — larger is more relevant — for a
    ///     relevance floor, or for fusing with a vector ranking.
    /// </summary>
    /// <inheritdoc cref="FullTextSearchAsync{T}" />
    public static async Task<IReadOnlyList<FullTextMatch<T>>> FullTextSearchWithScoresAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        string text,
        int limit = 10,
        FullTextSearchOptions? options = null,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken token = default) where T : notnull
    {
        var built = await BuildAsync(session, member, text, limit, options, filter, token).ConfigureAwait(false);
        if (built is null) return [];

        var rows = await ((QuerySession)session).QueryByBatchAsync<T, double>(built, token)
            .ConfigureAwait(false);

        return rows.Select(row => new FullTextMatch<T>(row.Item1, row.Item2)).ToList();
    }

    private static async Task<SqlBatch?> BuildAsync<T>(
        IQuerySession session,
        Expression<Func<T, object?>> member,
        string text,
        int limit,
        FullTextSearchOptions? options,
        Expression<Func<T, bool>>? filter,
        CancellationToken token) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(member);
        ArgumentNullException.ThrowIfNull(text);
        if (limit < 1) throw new ArgumentOutOfRangeException(nameof(limit), limit, "limit must be at least 1");

        // Already validated — FullTextSearchOptions refuses an out-of-range k1 or b at construction,
        // so a bad value fails at the caller's own `new` with its own stack rather than here.
        options ??= new FullTextSearchOptions();

        // The token table has to exist before a statement names it — the same reason the vector search
        // ensures its table first. A store whose first act is a search would otherwise meet "Invalid
        // object name".
        await ((QuerySession)session).EnsureDocumentTableAsync(typeof(T), token).ConfigureAwait(false);

        var mapping = ((QuerySession)session).Providers.GetProvider(typeof(T)).Mapping;
        var chain = ChainOf(member);
        var name = string.Join(".", chain.Select(x => x.Name));

        var index = mapping.FullTextIndexes.FirstOrDefault(x => x.MemberName == name)
                    ?? throw new InvalidOperationException(
                        mapping.FullTextIndexes.Count == 0
                            ? $"'{typeof(T).Name}' declares no full-text index, so there is nothing to "
                              + $"search. Declare one with "
                              + $"Schema.For<{typeof(T).Name}>().FullTextIndex(x => x.{name})."
                            : $"'{typeof(T).Name}.{name}' is not a declared full-text member. Declared: "
                              + string.Join(", ", mapping.FullTextIndexes.Select(x => x.MemberName)) + ".");

        var terms = FullTextIndex.Tokenize(text);

        // No terms is not an error and not everything — it is no match, the same answer the LINQ
        // operator gives for an empty search.
        if (terms.Length == 0) return null;

        var ftTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, FullTextIndex.TableNameFor(mapping));
        var docTable = mapping.QualifiedTableName;
        var conjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;

        // IAdvancedSql replaces each '?' with @p0, @p1, ... in the order they appear in the TEXT, so
        // this list is built in exactly the order the placeholders below are written.
        var parameters = new List<object>();
        var termList = string.Join(", ", terms.Select(_ => "?"));
        var tenantFilter = conjoined ? " AND tenant_id = ?" : string.Empty;

        void AddMemberScope()
        {
            parameters.Add(index.MemberName);
            parameters.AddRange(terms);
            if (conjoined) parameters.Add(session.TenantId);
        }

        // lens: member, [tenant]
        parameters.Add(index.MemberName);
        if (conjoined) parameters.Add(session.TenantId);

        var sql =
            $"""
             WITH lens AS (
                 SELECT doc_id, COUNT(*) AS dl FROM {ftTable}
                 WHERE member = ?{tenantFilter}
                 GROUP BY doc_id
             ),
             stats AS (SELECT COUNT(*) AS n, AVG(CAST(dl AS float)) AS avgdl FROM lens),
             tf AS (
                 SELECT doc_id, term, COUNT(*) AS tf FROM {ftTable}
                 WHERE member = ? AND term IN ({termList}){tenantFilter}
                 GROUP BY doc_id, term
             ),
             df AS (
                 SELECT term, COUNT(DISTINCT doc_id) AS df FROM {ftTable}
                 WHERE member = ? AND term IN ({termList}){tenantFilter}
                 GROUP BY term
             ),
             scored AS (
                 SELECT tf.doc_id,
                        SUM(LOG(1.0 + ((s.n - df.df + 0.5) / (df.df + 0.5)))
                            * ((tf.tf * (? + 1.0))
                               / (tf.tf + ? * (1.0 - ? + ? * (l.dl / s.avgdl))))) AS score
                 FROM tf
                 INNER JOIN df ON df.term = tf.term
                 INNER JOIN lens l ON l.doc_id = tf.doc_id
                 CROSS JOIN stats s
                 GROUP BY tf.doc_id
             )
             SELECT TOP(?) d.id, d.data, scored.score
             FROM scored INNER JOIN {docTable} d ON d.id = scored.doc_id
             {DocFilters(mapping, conjoined, filter is not null)}
             """;

        AddMemberScope();  // tf
        AddMemberScope();  // df
        parameters.Add(options.K1);
        parameters.Add(options.K1);
        parameters.Add(options.B);
        parameters.Add(options.B);
        parameters.Add(limit);
        if (conjoined) parameters.Add(session.TenantId);

        // ⚠️ Built through a BatchBuilder rather than IAdvancedSql's '?' route (#633). A caller's
        // filter is an ISqlFragment that binds parameters of its own, and IAdvancedSql numbers
        // parameters by counting '?' characters in the TEXT — so a fragment has no way to take part.
        // AppendWithParameters keeps the '?' spelling above exactly as it was, then the fragment
        // continues from the same parameter counter.
        var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);
        var bound = builder.AppendWithParameters(sql, '?');
        for (var i = 0; i < parameters.Count; i++)
        {
            bound[i].Value = parameters[i] ?? DBNull.Value;

            // ⚠️ AppendWithParameters hands back parameters typed as the provider's STRING type — it
            // only knows the placeholder, not the value. Assigning Value does not undo that, so an
            // nvarchar '50' reaches TOP(@p) and SQL Server refuses the whole statement. Resetting lets
            // the type be inferred from the value the way IAdvancedSql's own parameters always were.
            bound[i].ResetSqlDbType();
        }

        if (filter is not null)
        {
            builder.Append(" AND (");
            var memberFactory = new MemberFactory(((QuerySession)session).Options, mapping);
            new WhereClauseParser(memberFactory).Parse(filter.Body).Apply(builder);
            builder.Append(')');
        }

        builder.Append(" ORDER BY scored.score DESC");
        builder.Compile();

        return batch;
    }

    /// <summary>
    ///     Filters on the document table itself. Soft-deleted rows keep their tokens — the trigger
    ///     sees an UPDATE, not a DELETE — so they have to be excluded here or a deleted document would
    ///     come back from a search.
    /// </summary>
    /// <remarks>
    ///     <c>WHERE 1=1</c> appears only when a caller's filter is going to be appended and there is
    ///     nothing else to hang it off — the alternative is deciding between <c>WHERE</c> and
    ///     <c>AND</c> at the append site, in two places.
    /// </remarks>
    private static string DocFilters(DocumentMapping mapping, bool conjoined, bool hasFilter)
    {
        var wheres = new List<string>();
        if (mapping.DeleteStyle == DeleteStyle.SoftDelete) wheres.Add("d.is_deleted = 0");
        if (conjoined) wheres.Add("d.tenant_id = ?");

        if (wheres.Count > 0) return "WHERE " + string.Join(" AND ", wheres);

        return hasFilter ? "WHERE 1=1" : string.Empty;
    }

    /// <summary>The member chain of <c>x =&gt; x.A.B</c>, unwrapping the boxing convert.</summary>
    private static MemberInfo[] ChainOf<T>(Expression<Func<T, object?>> member)
    {
        var body = member.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            body = unary.Operand;
        }

        var chain = new List<MemberInfo>();
        while (body is MemberExpression access)
        {
            chain.Insert(0, access.Member);
            body = access.Expression!;
        }

        if (chain.Count == 0 || body is not ParameterExpression)
        {
            throw new ArgumentException(
                "The full-text member must be a plain member access on the document, like x => x.Body.",
                nameof(member));
        }

        return chain.ToArray();
    }
}

/// <summary>A document and its BM25 score. Larger is more relevant.</summary>
public sealed record FullTextMatch<T>(T Document, double Score);

/// <summary>
///     The Okapi BM25 tuning constants used by <see cref="FullTextSearchExtensions.FullTextSearchAsync{T}" />
///     and its scored sibling. The defaults are the conventional ones and are what every call used
///     before this type existed (gh-611), so omitting it changes nothing.
/// </summary>
/// <remarks>
///     <para>
///         <b>Per call rather than per index, and that is the point of it.</b> One corpus is often
///         queried two ways — a title field wants very little length normalization, a body field wants
///         the default — and binding the constants to the index would force a second index to express
///         that. They are bound as SQL parameters, so a different <c>k1</c> reuses the same query plan.
///     </para>
///     <para>
///         ⚠️ <b>Scores computed with different options are not comparable with each other</b>, on top
///         of BM25 scores already not being comparable across corpora. A relevance floor tuned against
///         the defaults is meaningless against <c>b = 0</c>. Ordering within one result set is what
///         these are safe for.
///     </para>
///     <para>
///         <b>Deliberately not carried by <c>HybridSearchOptions</c>.</b> That type is shared across
///         the Critter Stack from <c>JasperFx.Events.Vectors</c> (#633) and Polecat does not get to add
///         members to it — and gh-640 settled that a shared option Polecat cannot honor is refused by
///         name rather than ignored. A hybrid search therefore scores its text leg with the defaults.
///     </para>
/// </remarks>
public sealed record FullTextSearchOptions
{
    /// <summary>The conventional BM25 term-frequency saturation, and what gh-611 hard-coded.</summary>
    public const double DefaultK1 = 1.2;

    /// <summary>The conventional BM25 length normalization, and what gh-611 hard-coded.</summary>
    public const double DefaultB = 0.75;

    private readonly double _k1 = DefaultK1;
    private readonly double _b = DefaultB;

    /// <summary>
    ///     Construct a set of BM25 constants. Both are optional and both default to the conventional
    ///     value, so <c>new FullTextSearchOptions(B: 0)</c> changes only length normalization.
    /// </summary>
    /// <remarks>
    ///     ⚠️ <b>An ORDINARY constructor rather than a positional record, and the difference is not
    ///     cosmetic.</b> A positional record whose parameter has a hand-written property of the same
    ///     name does NOT assign that property — the compiler stops synthesizing one and warns CS8907
    ///     rather than erroring, so the backing fields keep their default <c>0</c>. Written that way
    ///     first, this type silently produced <c>k1 = 0, b = 0</c> for every caller, which is not a
    ///     crash: BM25 simply collapses to IDF, every document carrying a term ties every other, and
    ///     the ranking quietly stops ranking. Three existing gh-611 facts caught it. Validation has to
    ///     live in the <c>init</c> accessors (see <see cref="K1" />), so the assignment has to be
    ///     explicit and this is what makes it so.
    /// </remarks>
    /// <param name="K1">
    ///     Term-frequency saturation: how much the second and third occurrence of a term in a document
    ///     add over the first. <c>0</c> is legal and means "ignore term frequency entirely" — a
    ///     document's score becomes the sum of its matched terms' IDF. Must not be negative, NaN or
    ///     infinite.
    /// </param>
    /// <param name="B">
    ///     Length normalization: how much a long document is penalized for saying the same thing at
    ///     greater length. <c>0</c> turns it off, so a long document with the same term frequency ties
    ///     a short one; <c>1</c> is full normalization. Must be within <c>[0, 1]</c>.
    /// </param>
    public FullTextSearchOptions(double K1 = DefaultK1, double B = DefaultB)
    {
        this.K1 = K1;
        this.B = B;
    }

    /// <inheritdoc cref="FullTextSearchOptions(double, double)" />
    /// <remarks>
    ///     ⚠️ Validated in the <c>init</c> accessor rather than in the constructor on purpose. A
    ///     record's <c>with</c> expression runs the compiler-generated copy constructor, which does
    ///     NOT re-run the one above — so constructor validation alone would let
    ///     <c>options with { B = 4 }</c> through, and BM25 would return a nonsense ranking rather than
    ///     refuse.
    /// </remarks>
    public double K1
    {
        get => _k1;
        init => _k1 = value is >= 0 and <= double.MaxValue
            ? value
            : throw new ArgumentOutOfRangeException(nameof(K1), value,
                "BM25's k1 is a term-frequency saturation and cannot be negative, NaN or infinite. "
                + $"Use 0 to ignore term frequency entirely, or omit it for the conventional {DefaultK1}.");
    }

    /// <inheritdoc cref="K1" />
    public double B
    {
        get => _b;
        init => _b = value is >= 0 and <= 1
            ? value
            : throw new ArgumentOutOfRangeException(nameof(B), value,
                "BM25's b is a length normalization and has to be within [0, 1]. Use 0 to turn length "
                + $"normalization off, 1 for full normalization, or omit it for the conventional {DefaultB}.");
    }
}
