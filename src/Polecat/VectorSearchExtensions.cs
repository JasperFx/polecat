using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Events.Vectors;
using Polecat.Internal;
using Polecat.Schema;
using Polecat.Storage;

namespace Polecat;

/// <summary>
///     Vector similarity search over a member declared with
///     <c>Schema.For&lt;T&gt;().VectorIndex(...)</c>, on Marten.PgVector's API shape so application
///     code written against one store reads the same against the other.
/// </summary>
/// <remarks>
///     <para>
///         <b>Run through <see cref="IAdvancedSql" /> rather than through LINQ, and that is a
///         decision rather than a shortcut.</b> A statement's <c>Wheres</c> are composable
///         <c>ISqlFragment</c>s that can bind parameters; its <c>OrderBys</c> are bare strings
///         appended verbatim. A vector ordering key is
///         <c>VECTOR_DISTANCE('cosine', col, @query)</c> — it has a parameter in it, so there is no
///         way to express it from an ordering position today. Making <c>OrderBys</c> carry fragments
///         is worth doing and is its own piece of work; it is not a prerequisite for this, and Fisher
///         reached the same conclusion from the same constraint.
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
    public static async Task<IReadOnlyList<T>> VectorSearchAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        ReadOnlyMemory<float> query,
        int limit = 10,
        DistanceFunction? distance = null,
        CancellationToken token = default) where T : notnull
    {
        // The table has to exist before a statement names its computed column. Raw SQL is not on
        // the path that provisions one, so a store whose FIRST act is a vector search would meet
        // "Invalid column name" — and so would a type that gained its declaration after its table was
        // created, which is the whole point of a computed column. Fisher learned the same thing for
        // its LINQ reads in fisher#74.
        await session.EnsureVectorTableAsync<T>(token).ConfigureAwait(false);

        var (sql, parameters) = Build(session, member, query, limit, distance);
        return await session.AdvancedSql.QueryAsync<T>(sql, token, parameters).ConfigureAwait(false);
    }

    /// <summary>
    ///     The same search, each document paired with its distance — smaller is closer under every
    ///     metric — for a similarity floor, or for fusing with a keyword ranking.
    /// </summary>
    public static async Task<IReadOnlyList<VectorMatch<T>>> VectorSearchWithScoresAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        ReadOnlyMemory<float> query,
        int limit = 10,
        DistanceFunction? distance = null,
        CancellationToken token = default) where T : notnull
    {
        await session.EnsureVectorTableAsync<T>(token).ConfigureAwait(false);

        var (sql, parameters) = Build(session, member, query, limit, distance);
        var rows = await session.AdvancedSql.QueryAsync<T, double>(sql, token, parameters).ConfigureAwait(false);
        return rows.Select(row => new VectorMatch<T>(row.Item1, row.Item2)).ToList();
    }

    private static Task EnsureVectorTableAsync<T>(this IQuerySession session, CancellationToken token)
        => ((QuerySession)session).EnsureDocumentTableAsync(typeof(T), token);

    private static (string Sql, object[] Parameters) Build<T>(
        IQuerySession session,
        Expression<Func<T, object?>> member,
        ReadOnlyMemory<float> query,
        int limit,
        DistanceFunction? distance) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(member);
        if (limit < 1) throw new ArgumentOutOfRangeException(nameof(limit), limit, "limit must be at least 1");

        var mapping = ((QuerySession)session).Providers.GetProvider(typeof(T)).Mapping;
        var index = ResolveIndex<T>(session, member, query);

        var table = mapping.QualifiedTableName;
        var column = SqlEscaping.QuoteIdentifier(index.ColumnName);
        var metric = VectorIndex.MetricName(distance ?? index.Distance);
        var score = $"VECTOR_DISTANCE('{metric}', d.{column}, CAST(? AS VECTOR({index.Dimensions})))";

        var wheres = new List<string> { $"d.{column} IS NOT NULL" };
        if (mapping.DeleteStyle == DeleteStyle.SoftDelete) wheres.Add("d.is_deleted = 0");
        if (mapping.TenancyStyle == TenancyStyle.Conjoined) wheres.Add("d.tenant_id = ?");

        // IAdvancedSql replaces each '?' with @p0, @p1, ... in the order they appear in the TEXT, so
        // the parameter array below has to be in that same order: limit, query vector, tenant.
        var sql =
            $"SELECT TOP(?) d.id, d.data, {score} AS distance FROM {table} d "
            + $"WHERE {string.Join(" AND ", wheres)} ORDER BY distance";

        var parameters = mapping.TenancyStyle == TenancyStyle.Conjoined
            ? [limit, ToVectorLiteral(query.Span), session.TenantId]
            : new object[] { limit, ToVectorLiteral(query.Span) };

        return (sql, parameters);
    }

    /// <summary>
    ///     The query vector in the text form <c>CAST(... AS VECTOR(n))</c> accepts. Invariant
    ///     formatting on purpose: a culture that writes a decimal comma produces a JSON array SQL
    ///     Server rejects, and only on the machines that use one.
    /// </summary>
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
