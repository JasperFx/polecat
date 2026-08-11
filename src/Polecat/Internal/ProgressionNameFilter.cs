using System.Text;

namespace Polecat.Internal;

/// <summary>
///     Builds the <c>WHERE</c> predicate that selects exactly one projection's or subscription's rows
///     in <c>pc_event_progression</c>, and nothing else (polecat#436, the twin of marten#5179).
/// </summary>
/// <remarks>
///     <para>
///         The predicate this replaces was <c>name LIKE @name</c> with <c>@name</c> bound to
///         <c>subscriptionName + "%"</c>. That is wrong twice over, and both halves delete another
///         projection's progression state — silently, and in a way a rebuild cannot undo:
///     </para>
///     <list type="number">
///         <item>
///             <description>
///                 <b>Unescaped wildcards.</b> <c>%</c>, <c>_</c> and <c>[</c> are all legal in a
///                 projection name and all three are T-SQL <c>LIKE</c> metacharacters. A projection
///                 named <c>day_summary</c> matches <c>dayXsummary</c> through the <c>_</c>; a name
///                 carrying <c>[</c> opens a character class and matches something else entirely.
///             </description>
///         </item>
///         <item>
///             <description>
///                 <b>Prefix over-match even with no wildcards at all.</b> A bare prefix on
///                 <c>day_summary</c> also sweeps <c>day_summary_v2</c> — a different, independently
///                 registered projection.
///             </description>
///         </item>
///     </list>
///     <para>
///         So every root here is matched with <b>exact equality</b> first, and the only pattern in
///         play is anchored on the <c>:</c> that separates the slots of the shard grammar
///         (<c>Name:ShardKey</c> / <c>Name:V{n}:ShardKey</c> / either of those plus <c>:Tenant</c>),
///         with the wildcards escaped. <c>day_summary</c> therefore never reaches
///         <c>day_summary_v2</c>: the anchor demands a literal <c>:</c> exactly where the sibling
///         has a <c>_</c>.
///     </para>
///     <para>
///         The pattern exists only because per-tenant rows are not enumerable: the registered shards
///         are store-global (<c>Trip:All</c>) while the persisted rows carry a tenant Polecat cannot
///         know up front (<c>Trip:All:acme</c>). The roots themselves are always
///         <see cref="JasperFx.Events.Projections.ShardName.Identity" /> values produced by
///         <c>ShardName</c> — never a hand-composed identity — and appending <c>":%"</c> to one is
///         building a <em>pattern</em>, not an identity.
///     </para>
/// </remarks>
internal static class ProgressionNameFilter
{
    /// <summary>
    ///     The <c>ESCAPE</c> character used by every pattern this type emits. Backslash has no
    ///     special meaning in a T-SQL string literal, so it needs no escaping of its own in the SQL
    ///     text — only in the bound pattern value, which <see cref="EscapeLikePattern" /> handles.
    /// </summary>
    public const char LikeEscapeCharacter = '\\';

    /// <summary>
    ///     Neutralize every T-SQL <c>LIKE</c> metacharacter in <paramref name="value" /> so the
    ///     result matches only itself, literally, under <c>ESCAPE '\'</c>.
    /// </summary>
    /// <remarks>
    ///     The set is <c>%</c> (any run), <c>_</c> (any single character), <c>[</c> (opens a
    ///     character class) and the escape character itself. A closing <c>]</c> outside a class is
    ///     an ordinary character in T-SQL and is deliberately left alone; escaping it would be
    ///     harmless but implies a symmetry the grammar does not have.
    /// </remarks>
    public static string EscapeLikePattern(string value)
    {
        var builder = new StringBuilder(value.Length + 8);
        foreach (var c in value)
        {
            if (c is '%' or '_' or '[' or LikeEscapeCharacter)
            {
                builder.Append(LikeEscapeCharacter);
            }

            builder.Append(c);
        }

        return builder.ToString();
    }

    /// <summary>
    ///     Compose the predicate — and the parameters it binds — matching each root exactly, plus
    ///     that root's per-tenant descendants (<c>{root}:{tenant}</c>).
    /// </summary>
    /// <param name="roots">
    ///     Either the exact <c>ShardName.Identity</c> of every registered shard of the projection
    ///     (the preferred, Marten-equivalent form), or — when nothing is registered under the name —
    ///     the projection/subscription name itself, in which case the anchored pattern is what picks
    ///     up its <c>{name}:{shardKey}</c> rows.
    /// </param>
    /// <returns>
    ///     A predicate suitable for <c>DELETE FROM {table} WHERE {Where}</c> and the varchar
    ///     parameters it references. An empty <paramref name="roots" /> yields a predicate that
    ///     matches nothing — deleting nothing is the only safe reading of "no rows were named".
    /// </returns>
    public static (string Where, IReadOnlyList<KeyValuePair<string, string>> Parameters) For(
        IReadOnlyList<string> roots)
    {
        if (roots.Count == 0)
        {
            return ("1 = 0", []);
        }

        var clauses = new List<string>(roots.Count);
        var parameters = new List<KeyValuePair<string, string>>(roots.Count * 2);

        for (var i = 0; i < roots.Count; i++)
        {
            var exact = $"@progression_exact{i}";
            var pattern = $"@progression_pattern{i}";

            clauses.Add($"(name = {exact} OR name LIKE {pattern} ESCAPE '{LikeEscapeCharacter}')");

            parameters.Add(new KeyValuePair<string, string>(exact, roots[i]));

            // Anchored on the shard grammar's ':' separator: matches this root's tenant-scoped
            // (and, for a bare projection name, shard- and version-scoped) rows, and can never
            // reach a sibling whose name merely starts with the same characters.
            parameters.Add(new KeyValuePair<string, string>(pattern, EscapeLikePattern(roots[i]) + ":%"));
        }

        return (string.Join(" OR ", clauses), parameters);
    }
}
