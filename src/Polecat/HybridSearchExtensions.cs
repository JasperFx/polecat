using System.Linq.Expressions;
using JasperFx.Events.Vectors;
using Polecat.Internal;
using Polecat.Linq;

namespace Polecat;

/// <summary>
///     Hybrid search: reciprocal rank fusion over a full-text leg (gh-611) and a vector leg (gh-605).
/// </summary>
/// <remarks>
///     <para>
///         <b>Every type this surface used to declare now comes from
///         <c>JasperFx.Events.Vectors</c> (#633).</b> <see cref="HybridSearchOptions" />,
///         <see cref="HybridMatch{T}" /> and <see cref="HybridTextStyle" /> were Polecat's own, and
///         Marten and Fisher each had a copy; one type is what makes a default — notably
///         <c>Distance = null</c>, "the metric the index declared" — one decision rather than three.
///     </para>
///     <para>
///         ⚠️ <b>The shared <see cref="HybridTextStyle" /> has two members and Polecat's had three.</b>
///         <c>Phrase</c> is gone from this surface rather than renamed: both shared members are safe to
///         hand a search box's raw contents, and that is the property the short list exists to keep.
///         Phrase search is reachable from <c>Query&lt;T&gt;().Where(x =&gt; x.Body.PhraseSearch(...))</c>,
///         where a malformed query fails only the call the caller made rather than both legs of a fused
///         search.
///     </para>
///     <para>
///         <b>The shape matches Fisher's so application code ports unchanged</b>, with one deliberate
///         difference. Fisher's text leg searches the document; Polecat's full-text operators address
///         a MEMBER, mirroring Marten. So the portable overload takes the vector member and uses the
///         type's full-text index when it has exactly one — which is the common case and the one that
///         ports — and refuses clearly when the type declares several, pointing at the explicit
///         overload that names both members.
///     </para>
///     <para>
///         <b>Both legs read through their own tested paths.</b> The text leg is
///         <c>FullTextSearchAsync</c> with its BM25 ordering, the vector leg is
///         <c>VectorSearchAsync</c> — so the tenancy filters, soft-delete filters and existing
///         refusals all apply here without being restated, and a fix to either leg reaches this.
///     </para>
///     <para>
///         <b>Only the ranks are fused, never the scores.</b> BM25 and a vector distance are not on
///         one scale and no normalization makes them comparable across corpora; RRF's whole appeal is
///         that it needs only the ordering each leg already produces. The fusion itself is
///         <see cref="ReciprocalRankFusion" />, shared rather than private since #633.
///     </para>
/// </remarks>
public static class HybridSearchExtensions
{
    /// <summary>
    ///     The <paramref name="limit" /> documents ranked highest by reciprocal rank fusion over a
    ///     full-text search for <paramref name="text" /> and a vector search for
    ///     <paramref name="query" />.
    /// </summary>
    /// <param name="filter">
    ///     An optional predicate, applied to BOTH legs before each leg's candidate depth — otherwise
    ///     rows the caller will discard consume the depth, and the fused order is a ranking of a set
    ///     that includes them (jasperfx#843).
    /// </param>
    public static async Task<IReadOnlyList<T>> HybridSearchAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        string text,
        ReadOnlyMemory<float> query,
        int limit = 10,
        HybridSearchOptions? options = null,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken token = default) where T : notnull
        => (await session.HybridSearchWithScoresAsync(member, text, query, limit, options, filter, token)
                .ConfigureAwait(false))
            .Select(x => x.Document)
            .ToList();

    /// <summary>
    ///     The same search, each document paired with its fused score — larger is better — for a
    ///     relevance floor.
    /// </summary>
    /// <inheritdoc cref="HybridSearchAsync{T}" />
    public static Task<IReadOnlyList<HybridMatch<T>>> HybridSearchWithScoresAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        string text,
        ReadOnlyMemory<float> query,
        int limit = 10,
        HybridSearchOptions? options = null,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken token = default) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(session);

        var mapping = ((QuerySession)session).Providers.GetProvider(typeof(T)).Mapping;

        if (mapping.FullTextIndexes.Count == 0)
        {
            throw new InvalidOperationException(
                $"'{typeof(T).Name}' declares no full-text index, so a hybrid search has no text leg. "
                + $"Declare one with Schema.For<{typeof(T).Name}>().FullTextIndex(x => x.SomeText).");
        }

        if (mapping.FullTextIndexes.Count > 1)
        {
            throw new InvalidOperationException(
                $"'{typeof(T).Name}' declares {mapping.FullTextIndexes.Count} full-text members "
                + $"({string.Join(", ", mapping.FullTextIndexes.Select(x => x.MemberName))}), so this "
                + "overload cannot tell which one the text leg should search. Use the overload that "
                + "names both members explicitly.");
        }

        var textMember = mapping.FullTextIndexes[0].MemberName;

        return SearchAsync(session, textMember, member, text, query, limit, options, filter, token);
    }

    /// <summary>
    ///     Hybrid search naming both members explicitly — for a type whose text and vector live on
    ///     members the portable overload cannot infer.
    /// </summary>
    /// <inheritdoc cref="HybridSearchAsync{T}" />
    public static Task<IReadOnlyList<HybridMatch<T>>> HybridSearchWithScoresAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> textMember,
        Expression<Func<T, object?>> vectorMember,
        string text,
        ReadOnlyMemory<float> query,
        int limit = 10,
        HybridSearchOptions? options = null,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken token = default) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(textMember);
        return SearchAsync(session, NameOf(textMember), vectorMember, text, query, limit, options, filter, token);
    }

    private static async Task<IReadOnlyList<HybridMatch<T>>> SearchAsync<T>(
        IQuerySession session,
        string textMemberName,
        Expression<Func<T, object?>> vectorMember,
        string text,
        ReadOnlyMemory<float> query,
        int limit,
        HybridSearchOptions? options,
        Expression<Func<T, bool>>? filter,
        CancellationToken token) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(vectorMember);
        ArgumentNullException.ThrowIfNull(text);

        options ??= new HybridSearchOptions();

        // Every refusal — limit, K, a candidate depth below limit — now lives on the shared options
        // record (jasperfx#844). Three stores were each making them with identical messages, and one
        // copy means a store cannot quietly stop making one.
        var depth = options.ResolveCandidateDepth(limit);

        // ⚠️ REFUSED rather than ignored (#640, jasperfx#854). Polecat's full-text ranking addresses a
        // single member, so there is no second column to weigh and nothing here could honour a weight.
        // Ignoring it is the one option that is actually dangerous: a caller who weighted their title
        // column and silently got an unweighted ranking has no way to find out, because the search
        // still returns plausible documents in a plausible order. That is the same failure shape as
        // the Distance default the shared record was created to fix.
        options.AssertColumnWeightsAreNotSupported(
            "Polecat",
            "Its full-text ranking addresses a single member, so there is no second column to weigh. "
            + "Fisher is the store that honours per-column weights.");

        var textLeg = await TextLegAsync(session, textMemberName, text, options.TextStyle, depth, filter, token)
            .ConfigureAwait(false);
        var vectorLeg = await session
            .VectorSearchAsync(vectorMember, query, depth, options.Distance, filter, token)
            .ConfigureAwait(false);

        var mapping = ((QuerySession)session).Providers.GetProvider(typeof(T)).Mapping;

        return ReciprocalRankFusion.Fuse(textLeg, vectorLeg, document => mapping.GetId(document)!, limit, options.K);
    }

    /// <summary>
    ///     The text leg, ordered. <see cref="HybridTextStyle.PlainText" /> goes through
    ///     <c>FullTextSearchAsync</c> so the ordering is BM25 relevance;
    ///     <see cref="HybridTextStyle.WebStyle" /> has no useful ranking of its own — a document either
    ///     satisfies the query or it does not — so it reads through the LINQ operator and takes source
    ///     order.
    /// </summary>
    private static async Task<IReadOnlyList<T>> TextLegAsync<T>(
        IQuerySession session, string textMemberName, string text, HybridTextStyle style, int depth,
        Expression<Func<T, bool>>? filter, CancellationToken token) where T : notnull
    {
        if (style == HybridTextStyle.WebStyle)
        {
            // x => x.<member>.WebStyleSearch(text), built rather than written, because the member is
            // only known by name here. The parser sees the same MethodCallExpression it would from a
            // hand-written Where, so the leg reads through exactly the tested path.
            var parameter = Expression.Parameter(typeof(T), "x");
            Expression access = parameter;
            foreach (var part in textMemberName.Split('.'))
            {
                access = Expression.PropertyOrField(access, part);
            }

            var call = Expression.Call(
                typeof(LinqExtensions),
                nameof(LinqExtensions.WebStyleSearch),
                null,
                access,
                Expression.Constant(text));

            var predicate = Expression.Lambda<Func<T, bool>>(call, parameter);

            var queryable = session.Query<T>().Where(predicate);
            if (filter is not null) queryable = queryable.Where(filter);

            return await queryable.Take(depth).ToListAsync(token).ConfigureAwait(false);
        }

        var member = MemberExpressionFor<T>(textMemberName);
        return await session.FullTextSearchAsync(member, text, depth, filter, token).ConfigureAwait(false);
    }

    private static string NameOf<T>(Expression<Func<T, object?>> member)
    {
        var body = member.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
        {
            body = u.Operand;
        }

        var parts = new List<string>();
        while (body is MemberExpression access)
        {
            parts.Insert(0, access.Member.Name);
            body = access.Expression!;
        }

        if (parts.Count == 0)
        {
            throw new ArgumentException(
                "The member must be a plain member access on the document, like x => x.Body.",
                nameof(member));
        }

        return string.Join(".", parts);
    }

    /// <summary>
    ///     Rebuild <c>x =&gt; x.&lt;name&gt;</c> from the member name the portable overload inferred,
    ///     so the text leg can call the same strongly-typed search a caller would.
    /// </summary>
    private static Expression<Func<T, object?>> MemberExpressionFor<T>(string name)
    {
        var parameter = Expression.Parameter(typeof(T), "x");
        Expression body = parameter;
        foreach (var part in name.Split('.'))
        {
            body = Expression.PropertyOrField(body, part);
        }

        return Expression.Lambda<Func<T, object?>>(Expression.Convert(body, typeof(object)), parameter);
    }
}
