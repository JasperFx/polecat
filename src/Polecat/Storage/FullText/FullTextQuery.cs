namespace Polecat.Storage.FullText;

/// <summary>
///     One requirement on a document: a term, a phrase, or the absence of either.
/// </summary>
/// <param name="Terms">The tokenized terms. More than one only for a phrase.</param>
/// <param name="Phrase">Whether the terms must appear adjacent and in order.</param>
/// <param name="Negated">Whether the document must NOT satisfy this clause.</param>
internal sealed record FullTextClause(string[] Terms, bool Phrase, bool Negated);

/// <summary>
///     A parsed full-text query: OR over groups, AND within a group. Every operator — plain, phrase
///     and web style — reduces to this, so one SQL builder serves all three.
/// </summary>
internal sealed record FullTextQuery(IReadOnlyList<IReadOnlyList<FullTextClause>> OrGroups)
{
    /// <summary>Nothing to look for. Matches no document — see the remarks on <see cref="Parse" />.</summary>
    public bool IsEmpty => OrGroups.Count == 0 || OrGroups.All(g => g.Count == 0);

    /// <summary>Every term must appear, in any order.</summary>
    internal static FullTextQuery Plain(string text)
    {
        var clauses = FullTextIndex.Tokenize(text)
            .Select(t => new FullTextClause([t], false, false))
            .ToList();

        return new FullTextQuery(clauses.Count == 0 ? [] : [clauses]);
    }

    /// <summary>The terms adjacent and in order.</summary>
    internal static FullTextQuery Phrase(string text)
    {
        var terms = FullTextIndex.Tokenize(text);
        return new FullTextQuery(terms.Length == 0 ? [] : [[new FullTextClause(terms, true, false)]]);
    }

    /// <summary>
    ///     Web-style search: bare words are required, <c>"quoted text"</c> is a phrase, a leading
    ///     <c>-</c> excludes, and a bare <c>or</c> separates alternatives.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Modelled on PostgreSQL's <c>websearch_to_tsquery</c>, which is what Marten's operator of
    ///         the same name uses, so a search box's raw contents mean broadly the same thing on both
    ///         stores.
    ///     </para>
    ///     <para>
    ///         <b>Two differences from PostgreSQL, both worth knowing rather than discovering.</b>
    ///         First, Polecat does not stem, so <c>running</c> will not match <c>run</c> here any more
    ///         than it does anywhere else in this index. Second, <c>or</c> splits the query into
    ///         alternatives at the TOP level — <c>a b or c</c> is <c>(a AND b) OR (c)</c> — where
    ///         PostgreSQL binds it more tightly, as <c>a AND (b OR c)</c>. The simpler rule is
    ///         predictable to explain to an end user typing into a box, which is the audience this
    ///         operator has; anyone needing real boolean precedence should compose
    ///         <c>PlainTextSearch</c> and <c>PhraseSearch</c> with C#'s own <c>&amp;&amp;</c> and
    ///         <c>||</c>, where the precedence is the language's and not ours.
    ///     </para>
    ///     <para>
    ///         A query of nothing but exclusions matches nothing, rather than every document that
    ///         happens to lack them. "Not this" is not a search.
    ///     </para>
    /// </remarks>
    internal static FullTextQuery WebStyle(string text)
    {
        var groups = new List<List<FullTextClause>>();
        var current = new List<FullTextClause>();

        foreach (var (raw, quoted) in Segments(text))
        {
            if (!quoted && raw.Equals("or", StringComparison.OrdinalIgnoreCase))
            {
                if (current.Count > 0)
                {
                    groups.Add(current);
                    current = [];
                }

                continue;
            }

            var negated = raw.StartsWith('-');
            var body = negated ? raw[1..] : raw;
            var terms = FullTextIndex.Tokenize(body);
            if (terms.Length == 0) continue;

            current.Add(new FullTextClause(terms, quoted && terms.Length > 1, negated));
        }

        if (current.Count > 0) groups.Add(current);

        // Exclusions alone are not a search.
        groups.RemoveAll(g => g.All(c => c.Negated));

        return new FullTextQuery(groups.Select(g => (IReadOnlyList<FullTextClause>)g).ToList());
    }

    /// <summary>
    ///     Split on whitespace, keeping <c>"quoted runs"</c> together and reporting whether each
    ///     segment was quoted. A leading <c>-</c> survives on the segment so the caller can read it.
    /// </summary>
    private static IEnumerable<(string Text, bool Quoted)> Segments(string text)
    {
        var i = 0;
        while (i < text.Length)
        {
            while (i < text.Length && char.IsWhiteSpace(text[i])) i++;
            if (i >= text.Length) yield break;

            var negated = text[i] == '-' && i + 1 < text.Length && !char.IsWhiteSpace(text[i + 1]);
            var prefix = negated ? "-" : string.Empty;
            if (negated) i++;

            if (i < text.Length && text[i] == '"')
            {
                i++;
                var start = i;
                while (i < text.Length && text[i] != '"') i++;
                var body = text[start..i];
                if (i < text.Length) i++; // closing quote, if the user supplied one
                yield return (prefix + body, true);
            }
            else
            {
                var start = i;
                while (i < text.Length && !char.IsWhiteSpace(text[i])) i++;
                yield return (prefix + text[start..i], false);
            }
        }
    }
}
