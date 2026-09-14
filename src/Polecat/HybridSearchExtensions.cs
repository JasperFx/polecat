using System.Linq.Expressions;
using JasperFx.Events.Vectors;
using Polecat.Internal;
using Polecat.Linq;

namespace Polecat;

/// <summary>
///     How the two legs of a <see cref="HybridSearchExtensions">hybrid search</see> are fused
///     (gh-612).
/// </summary>
/// <param name="K">
///     Reciprocal rank fusion's smoothing constant. Conventionally 60, which is what the original RRF
///     paper used and what every implementation since has defaulted to. Larger flattens the
///     difference between ranks; smaller makes the top of each leg dominate.
/// </param>
/// <param name="CandidateDepth">
///     How deep to read each leg before fusing. Null takes <c>max(limit × 4, 50)</c>.
///     <b>It has to exceed <c>limit</c>, and that is the whole point</b>: a document ranked 40th by
///     one leg and 1st by the other is exactly the result hybrid search exists to surface, and reading
///     only <c>limit</c> from each leg would never see it.
/// </param>
/// <param name="Distance">
///     Override the vector index's declared distance function, as <c>VectorSearchAsync</c> allows.
/// </param>
/// <param name="TextStyle">How the text is turned into a search. See <see cref="HybridTextStyle" />.</param>
public sealed record HybridSearchOptions(
    int K = 60,
    int? CandidateDepth = null,
    DistanceFunction? Distance = null,
    HybridTextStyle TextStyle = HybridTextStyle.PlainText);

/// <summary>
///     Which full-text operator the text leg uses.
/// </summary>
/// <remarks>
///     <b>Both members are safe to hand a search box's raw contents, and that is why the list is
///     short.</b> <see cref="PlainText" /> is named as Marten names it so the same call compiles
///     against either store (gh-627); <see cref="Phrase" /> is Polecat's own. Polecat's other full-text reach — addressing a specific member, or a syntax with
///     operators in it — stays on <c>Query&lt;T&gt;()</c>, where a malformed query fails the one
///     thing the caller asked for rather than failing both legs of a fused search. Fisher draws the
///     same line for the same reason.
/// </remarks>
public enum HybridTextStyle
{
    /// <summary>
    ///     Every term must appear, in any order. The sensible default for a search box, and the one
    ///     member guaranteed to mean the same thing on every Critter Stack store.
    /// </summary>
    PlainText,

    /// <summary>
    ///     The terms adjacent and in order.
    /// </summary>
    /// <remarks>
    ///     <b>Polecat-specific.</b> Marten has no phrase style, so code that must read the same
    ///     against both stores should stay on <see cref="PlainText" /> or <see cref="WebStyle" />.
    /// </remarks>
    Phrase,

    /// <summary>
    ///     A search box's raw contents: bare words required, <c>"quoted text"</c> a phrase, a leading
    ///     <c>-</c> excluding, and a bare <c>or</c> separating alternatives.
    /// </summary>
    WebStyle
}

/// <summary>A document and its fused score. Larger is better.</summary>
public sealed record HybridMatch<T>(T Document, double Score);

/// <summary>
///     Hybrid search: reciprocal rank fusion over a full-text leg (gh-611) and a vector leg (gh-605).
/// </summary>
/// <remarks>
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
///         that it needs only the ordering each leg already produces.
///     </para>
/// </remarks>
public static class HybridSearchExtensions
{
    /// <summary>
    ///     The <paramref name="limit" /> documents ranked highest by reciprocal rank fusion over a
    ///     full-text search for <paramref name="text" /> and a vector search for
    ///     <paramref name="query" />.
    /// </summary>
    public static async Task<IReadOnlyList<T>> HybridSearchAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        string text,
        ReadOnlyMemory<float> query,
        int limit = 10,
        HybridSearchOptions? options = null,
        CancellationToken token = default) where T : notnull
        => (await session.HybridSearchWithScoresAsync(member, text, query, limit, options, token)
                .ConfigureAwait(false))
            .Select(x => x.Document)
            .ToList();

    /// <summary>
    ///     The same search, each document paired with its fused score — larger is better — for a
    ///     relevance floor.
    /// </summary>
    public static Task<IReadOnlyList<HybridMatch<T>>> HybridSearchWithScoresAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> member,
        string text,
        ReadOnlyMemory<float> query,
        int limit = 10,
        HybridSearchOptions? options = null,
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

        return SearchAsync(session, textMember, member, text, query, limit, options, token);
    }

    /// <summary>
    ///     Hybrid search naming both members explicitly — for a type whose text and vector live on
    ///     members the portable overload cannot infer.
    /// </summary>
    public static Task<IReadOnlyList<HybridMatch<T>>> HybridSearchWithScoresAsync<T>(
        this IQuerySession session,
        Expression<Func<T, object?>> textMember,
        Expression<Func<T, object?>> vectorMember,
        string text,
        ReadOnlyMemory<float> query,
        int limit = 10,
        HybridSearchOptions? options = null,
        CancellationToken token = default) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(textMember);
        return SearchAsync(session, NameOf(textMember), vectorMember, text, query, limit, options, token);
    }

    private static async Task<IReadOnlyList<HybridMatch<T>>> SearchAsync<T>(
        IQuerySession session,
        string textMemberName,
        Expression<Func<T, object?>> vectorMember,
        string text,
        ReadOnlyMemory<float> query,
        int limit,
        HybridSearchOptions? options,
        CancellationToken token) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(vectorMember);
        ArgumentNullException.ThrowIfNull(text);
        if (limit < 1) throw new ArgumentOutOfRangeException(nameof(limit), limit, "limit must be at least 1");

        options ??= new HybridSearchOptions();

        if (options.K < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.K,
                "K must be at least 1. It is reciprocal rank fusion's smoothing constant, and 0 would "
                + "make the top-ranked document of either leg score infinitely.");
        }

        var depth = options.CandidateDepth ?? Math.Max(limit * 4, 50);

        if (depth < limit)
        {
            throw new ArgumentOutOfRangeException(nameof(options), depth,
                $"CandidateDepth ({depth}) is below limit ({limit}), so the fusion would have fewer "
                + "candidates than it is asked to return. It exists to read DEEPER than limit: a "
                + "document ranked low by one leg and first by the other is what hybrid search is for.");
        }

        var textLeg = await TextLegAsync<T>(session, textMemberName, text, options.TextStyle, depth, token)
            .ConfigureAwait(false);
        var vectorLeg = await session.VectorSearchAsync(vectorMember, query, depth, options.Distance, token)
            .ConfigureAwait(false);

        return Fuse(session, textLeg, vectorLeg, options.K, limit);
    }

    /// <summary>
    ///     The text leg, ordered. <see cref="HybridTextStyle.PlainText" /> goes through
    ///     <c>FullTextSearchAsync</c> so the ordering is BM25 relevance; the other two styles have no
    ///     useful ranking of their own — a document either satisfies the phrase or the web-style
    ///     query or it does not — so they read through the LINQ operator and take source order.
    /// </summary>
    private static async Task<IReadOnlyList<T>> TextLegAsync<T>(
        IQuerySession session, string textMemberName, string text, HybridTextStyle style, int depth,
        CancellationToken token) where T : notnull
    {
        if (style is HybridTextStyle.Phrase or HybridTextStyle.WebStyle)
        {
            // x => x.<member>.PhraseSearch(text), built rather than written, because the member is
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
                style == HybridTextStyle.WebStyle
                    ? nameof(LinqExtensions.WebStyleSearch)
                    : nameof(LinqExtensions.PhraseSearch),
                null,
                access,
                Expression.Constant(text));

            var predicate = Expression.Lambda<Func<T, bool>>(call, parameter);

            return await session.Query<T>().Where(predicate).Take(depth).ToListAsync(token)
                .ConfigureAwait(false);
        }

        var member = MemberExpressionFor<T>(textMemberName);
        return await session.FullTextSearchAsync(member, text, depth, token).ConfigureAwait(false);
    }

    /// <summary>
    ///     Reciprocal rank fusion: <c>score(d) = Σ 1 / (k + rank(d))</c> over the legs that found it.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Ranks are 1-based, which is what makes <c>k</c> mean what the literature says it means.
    ///     </para>
    ///     <para>
    ///         <b>Ties are broken deterministically, and that is not tidiness.</b> Two documents found
    ///         at the same rank by one leg and by neither in the other have identical scores, which is
    ///         common rather than exotic; without a total order the page they land on differs between
    ///         runs. Best rank first, then the identity, which is unique by construction.
    ///     </para>
    /// </remarks>
    private static IReadOnlyList<HybridMatch<T>> Fuse<T>(
        IQuerySession session, IReadOnlyList<T> textLeg, IReadOnlyList<T> vectorLeg, int k, int limit)
        where T : notnull
    {
        var mapping = ((QuerySession)session).Providers.GetProvider(typeof(T)).Mapping;
        var fused = new Dictionary<object, (T Document, double Score, int BestRank)>();

        void Accumulate(IReadOnlyList<T> leg)
        {
            for (var i = 0; i < leg.Count; i++)
            {
                var rank = i + 1;
                var id = mapping.GetId(leg[i]);
                var contribution = 1.0 / (k + rank);

                fused[id] = fused.TryGetValue(id, out var existing)
                    ? (existing.Document, existing.Score + contribution, Math.Min(existing.BestRank, rank))
                    : (leg[i], contribution, rank);
            }
        }

        Accumulate(textLeg);
        Accumulate(vectorLeg);

        return fused
            .OrderByDescending(x => x.Value.Score)
            .ThenBy(x => x.Value.BestRank)
            .ThenBy(x => x.Key.ToString(), StringComparer.Ordinal)
            .Take(limit)
            .Select(x => new HybridMatch<T>(x.Value.Document, x.Value.Score))
            .ToList();
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
