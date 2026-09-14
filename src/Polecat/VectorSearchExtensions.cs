using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Events.Vectors;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Linq.SqlGeneration;
using Polecat.Schema;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat;

/// <summary>
///     Vector similarity search over a member declared with
///     <c>Schema.For&lt;T&gt;().VectorIndex(...)</c>, on Marten.PgVector's API shape so application
///     code written against one store reads the same against the other.
/// </summary>
/// <remarks>
///     <para>
///         <b>Built as one statement through a <see cref="BatchBuilder" />.</b> A vector ordering key
///         is <c>VECTOR_DISTANCE('cosine', col, @query)</c> — it has a parameter in it — and since
///         #633 the WHERE can carry a caller's <c>filter</c> as well, which is an
///         <see cref="ISqlFragment" /> that binds parameters of its own. Neither can be expressed as a
///         '?'-placeholder string, so this no longer runs through <see cref="IAdvancedSql" />.
///     </para>
///     <para>
///         The query vector is bound as its text form and cast server-side, which is what
///         <c>VECTOR(n)</c> accepts, and the stored side is the persisted computed column — so the
///         distance is computed over a real vector on both sides rather than re-parsed per row.
///     </para>
/// </remarks>
public static class VectorSearchExtensions
{
    /// <summary>
    ///     The <paramref name="limit" /> documents nearest to <paramref name="query" /> under the
    ///     declaration's distance (or <paramref name="distance" /> when given), nearest first.
    /// </summary>
    /// <param name="filter">
    ///     An optional predicate, applied BEFORE <paramref name="limit" /> — so the result is the
    ///     top-k of the filtered set rather than the filtered remains of the top-k. See
    ///     <see cref="IDocumentSearchOperations.VectorSearchWithScoresAsync{T}" />.
    /// </param>
    public static async Task<IReadOnlyList<T>> VectorSearchAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        ReadOnlyMemory<float> query,
        int limit = 10,
        DistanceFunction? distance = null,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken token = default) where T : notnull
    {
        // The table has to exist before a statement names its computed column. Raw SQL is not on
        // the path that provisions one, so a store whose FIRST act is a vector search would meet
        // "Invalid column name" — and so would a type that gained its declaration after its table was
        // created, which is the whole point of a computed column. Fisher learned the same thing for
        // its LINQ reads in fisher#74.
        await session.EnsureVectorTableAsync<T>(token).ConfigureAwait(false);

        var batch = Build(session, member, query, limit, distance, filter);
        return await ((QuerySession)session).QueryByBatchAsync<T>(batch, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     The same search, each document paired with its distance — smaller is closer under every
    ///     metric — for a similarity floor, or for fusing with a keyword ranking.
    /// </summary>
    /// <inheritdoc cref="VectorSearchAsync{T}" />
    public static async Task<IReadOnlyList<VectorMatch<T>>> VectorSearchWithScoresAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        ReadOnlyMemory<float> query,
        int limit = 10,
        DistanceFunction? distance = null,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken token = default) where T : notnull
    {
        await session.EnsureVectorTableAsync<T>(token).ConfigureAwait(false);

        var batch = Build(session, member, query, limit, distance, filter);
        var rows = await ((QuerySession)session).QueryByBatchAsync<T, double>(batch, token)
            .ConfigureAwait(false);
        return rows.Select(row => new VectorMatch<T>(row.Item1, row.Item2)).ToList();
    }

    private static Task EnsureVectorTableAsync<T>(this IQuerySession session, CancellationToken token)
        => ((QuerySession)session).EnsureDocumentTableAsync(typeof(T), token);

    private static SqlBatch Build<T>(
        IQuerySession session,
        Expression<Func<T, object?>> member,
        ReadOnlyMemory<float> query,
        int limit,
        DistanceFunction? distance,
        Expression<Func<T, bool>>? filter) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(member);
        if (limit < 1) throw new ArgumentOutOfRangeException(nameof(limit), limit, "limit must be at least 1");

        var concrete = (QuerySession)session;
        var mapping = concrete.Providers.GetProvider(typeof(T)).Mapping;
        var index = ResolveIndex<T>(session, member, query);

        var column = SqlEscaping.QuoteIdentifier(index.ColumnName);
        var metric = VectorIndex.MetricName(distance ?? index.Distance);

        var batch = new SqlBatch();
        var builder = new BatchBuilder(batch);

        // TOP as a literal, exactly as Statement renders a LINQ Take(), so the only parameters in the
        // statement are the query vector, the tenant and whatever the filter binds.
        builder.Append($"SELECT TOP({limit}) id, data, VECTOR_DISTANCE('{metric}', {column}, CAST(");
        builder.AppendParameter(ToVectorLiteral(query.Span));
        builder.Append($" AS VECTOR({index.Dimensions}))) AS distance FROM {mapping.QualifiedTableName}");

        // ⚠️ No table alias, deliberately. A filter fragment comes out of the same where-parsing that
        // backs Query<T>().Where(...), which renders its column references UNQUALIFIED because a LINQ
        // statement selects from the table unaliased. Aliasing here would leave the composed halves
        // spelling the same column two ways.
        builder.Append($" WHERE {column} IS NOT NULL");

        if (mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            builder.Append(" AND is_deleted = 0");
        }

        if (mapping.TenancyStyle == TenancyStyle.Conjoined)
        {
            builder.Append(" AND tenant_id = ");
            builder.AppendParameter(session.TenantId);
        }

        if (filter is not null)
        {
            builder.Append(" AND (");
            ParseFilter(concrete, mapping, filter).Apply(builder);
            builder.Append(')');
        }

        builder.Append(" ORDER BY distance");
        builder.Compile();

        return batch;
    }

    /// <summary>
    ///     Turn a caller's <c>filter</c> into a WHERE fragment through the SAME parser that backs
    ///     <c>Query&lt;T&gt;().Where(...)</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Reusing the parser rather than writing a second one is the whole design of the filter
    ///         (jasperfx#843): the predicate supports and refuses exactly what LINQ does, down to the
    ///         message, and a new LINQ operator reaches vector search the day it lands. A private
    ///         translator would be a second dialect that drifts.
    ///     </para>
    ///     <para>
    ///         ⚠️ <b>The filter is applied in the same statement, before <c>TOP</c>.</b> On Polecat
    ///         that is exactly the filtered top-k with no recall caveat at all —
    ///         <c>VECTOR_DISTANCE</c> over a persisted computed column is an exact scan with no
    ///         approximate index bounding what the filter sees. A store with an ANN index has to
    ///         document that a selective filter can return fewer than <c>limit</c> rows; this one does
    ///         not.
    ///     </para>
    /// </remarks>
    private static ISqlFragment ParseFilter<T>(
        QuerySession session, DocumentMapping mapping, Expression<Func<T, bool>> filter) where T : notnull
    {
        var memberFactory = new MemberFactory(session.Options, mapping);
        return new WhereClauseParser(memberFactory).Parse(filter.Body);
    }

    /// <summary>
    ///     The vector declaration <paramref name="member" /> names, with every refusal this search
    ///     makes applied.
    /// </summary>
    /// <remarks>
    ///     Shared with the LINQ <c>OrderByVectorDistance</c> operator deliberately. A search with no
    ///     declared index, on a member that is not the declared one, or with a query vector of another
    ///     length is refused by NAME before any SQL runs — and two copies of those refusals would be two
    ///     chances for one path to stop making them.
    /// </remarks>
    internal static VectorIndex ResolveIndex<T>(
        IQuerySession session, Expression<Func<T, object?>> member, ReadOnlyMemory<float> query) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(member);

        // The concrete session, as every other extension over this seam does: the provider registry
        // and the store options are internal, and this needs the mapping's vector declarations.
        var mapping = ((QuerySession)session).Providers.GetProvider(typeof(T)).Mapping;
        var chain = ChainOf(member);
        var name = string.Join(".", chain.Select(x => x.Name));

        var index = mapping.VectorIndexes.FirstOrDefault(x => x.MemberName == name)
                    ?? throw new InvalidOperationException(
                        mapping.VectorIndexes.Count == 0
                            ? $"'{typeof(T).Name}' declares no vector index, so there is nothing to search. "
                              + $"Declare one with Schema.For<{typeof(T).Name}>().VectorIndex(x => x.{name}, dimensions)."
                            : $"'{typeof(T).Name}.{name}' is not a declared vector member. Declared: "
                              + string.Join(", ", mapping.VectorIndexes.Select(x => x.MemberName)) + ".");

        if (query.Length != index.Dimensions)
        {
            throw new ArgumentException(
                $"The query vector has {query.Length} dimensions but '{typeof(T).Name}.{index.MemberName}' "
                + $"was declared with {index.Dimensions}.", nameof(query));
        }

        return index;
    }

    /// <summary>
    ///     The query vector in the text form <c>CAST(... AS VECTOR(n))</c> accepts. Invariant
    ///     formatting on purpose: a culture that writes a decimal comma produces a JSON array SQL
    ///     Server rejects, and only on the machines that use one.
    /// </summary>
    internal static string ToVectorLiteral(ReadOnlySpan<float> vector)
    {
        var builder = new System.Text.StringBuilder(vector.Length * 8 + 2);
        builder.Append('[');
        for (var i = 0; i < vector.Length; i++)
        {
            if (i > 0) builder.Append(',');
            builder.Append(vector[i].ToString("R", System.Globalization.CultureInfo.InvariantCulture));
        }

        builder.Append(']');
        return builder.ToString();
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
                "The vector member must be a plain member access on the document, like x => x.Embedding.",
                nameof(member));
        }

        return chain.ToArray();
    }
}
