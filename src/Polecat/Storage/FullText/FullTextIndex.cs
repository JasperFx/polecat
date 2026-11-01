using System.Reflection;
using Polecat.Internal;
using Weasel.SqlServer;

namespace Polecat.Storage.FullText;

/// <summary>
///     Declares a member of a document as full-text searchable, stored as an inverted index Polecat
///     owns and maintains: one row per token, in a side table kept in step by a trigger on the
///     document table.
/// </summary>
/// <remarks>
///     <para>
///         <b>Polecat's own index, not SQL Server's full-text engine, and the reason is the
///         environment rather than taste (gh-611).</b> SQL Server's engine is absent from the image
///         this project runs (<c>SERVERPROPERTY('IsFullTextInstalled')</c> is 0 on
///         <c>mssql/server:2025-latest</c>; creating an index there fails with Msg 7609), refuses to
///         run in <c>master</c> where the suite lives (Msg 9966), cannot index a computed column —
///         and the document body here is JSON — and populates asynchronously, so a document written
///         and immediately queried may not be found. That last one is a semantic divergence from
///         Marten, whose <c>tsvector</c> index is synchronous. An owned index is none of those
///         things: it runs on the stock image and on Azure SQL Edge, and a write is searchable the
///         moment it commits.
///     </para>
///     <para>
///         <b>What that costs, stated plainly because the docs have to say it too:</b> we own
///         tokenization. Terms are lowercased and split on punctuation and whitespace. Stemming is
///         opt-in per index (<see cref="FullTextTokenizer.Porter" />, #642); there is no thesaurus and
///         no language-aware word breaking. Marten's <c>regConfig</c> overloads have no counterpart
///         and are deliberately not offered rather than accepted and ignored.
///     </para>
///     <para>
///         <b>The schema this amounts to is three objects, and two of them are modeled (#685).</b>
///         <see cref="FullTextTokenTable" /> is a Weasel table that owns its own index, and
///         <see cref="FullTextTrigger" /> is a Weasel trigger; <c>FullTextSchemaObjects.For</c> is the
///         single answer to "which objects, in what order". The backfill below is the third and stays
///         raw SQL on purpose — it writes rows rather than declaring structure. See
///         <see cref="BackfillStatements" />.
///     </para>
///     <para>
///         <b>Maintained by a trigger, so the write path is untouched.</b> The same trade
///         <see cref="VectorIndex" /> makes with its computed column: the tokens are derived from
///         <c>data</c> and cannot drift from the document, and no session code changes to keep them
///         current.
///     </para>
///     <para>
///         <b>Declaring an index backfills, and that is not optional.</b> A trigger only fires on
///         writes made after it exists, so declaring an index on a store that already holds documents
///         would otherwise produce an index matching nothing — no error, no rows, just a search that
///         quietly answers empty. Fisher met exactly this and answered it with FTS5's
///         <c>rebuild</c>; see the remarks on its <c>Fts5Table</c>. Note the contrast with
///         <see cref="VectorIndex" />, which got this for free: a computed column over the JSON makes
///         every existing row searchable the moment the column exists.
///     </para>
/// </remarks>
public class FullTextIndex
{
    /// <summary>
    ///     The width of the <c>term</c> column. A longer token is clipped to this on both sides, so it
    ///     can neither fail the write nor stop being findable.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         #835: nothing used to bound a token, so one that overflowed the column — a base64 blob,
    ///         a long identifier — failed the whole save, inside the trigger, as a truncation error
    ///         naming a table the caller never wrote to.
    ///     </para>
    ///     <para>
    ///         <b>Clipped rather than dropped, and the two differ.</b> Dropping would let the save
    ///         succeed and leave a value the document does contain unfindable with no error, which is
    ///         the failure the rest of #835 is about. PostgreSQL drops, with a notice nobody reads.
    ///         Clipping both sides keeps "search for what is in the document" true. The cost is that
    ///         two tokens sharing their first 255 characters match each other, which is the right way
    ///         round for a search.
    ///     </para>
    /// </remarks>
    internal const int MaxTermLength = 255;

    /// <summary>
    ///     The characters a token is split on, beyond whitespace. Everything here is mapped to a
    ///     space before splitting, so <c>"quick,brown"</c> is two terms rather than one.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Kept as one string because <c>TRANSLATE</c> takes a from/to pair of EQUAL length — a
    ///         mismatch is Msg 9828 at write time, and <c>LEN</c> cannot be used to measure it because
    ///         <c>LEN</c> ignores trailing spaces. <see cref="TokenizeClause" /> renders the replacement
    ///         from C# for that reason.
    ///     </para>
    ///     <para>
    ///         #839: whitespace is not in this string because it is <see cref="SqlText.Whitespace" />,
    ///         which the LINQ predicates share. It used to be nowhere: both tokenizers split on U+0020
    ///         alone, so <c>"quick\nbrown"</c> was one term and neither word could be found.
    ///     </para>
    ///     <para>
    ///         ⚠️ <b><c>%</c>, <c>_</c>, <c>[</c> and <c>]</c> are load-bearing here, beyond being
    ///         punctuation.</b> <c>PrefixSearch</c> renders a <c>LIKE</c>, and it escapes nothing
    ///         because this list guarantees no term — searched or stored — can carry a LIKE
    ///         metacharacter. Drop one of them from this string and a one-character search becomes
    ///         <c>term LIKE '%%'</c>, which returns the entire table rather than erroring.
    ///         <c>tokenize_strips_the_like_metacharacters</c> and
    ///         <c>prefix_search_cannot_smuggle_a_like_wildcard</c> both fail if you do; add escaping
    ///         in <c>FullTextFilter.AppendClause</c> before widening this.
    ///     </para>
    /// </remarks>
    internal const string Punctuation = "!\"#$%&()*+,-./:;<=>?@[\\]^_{|}~'";

    /// <summary>
    ///     Tokenize a search string the same way the trigger tokenizes a document: lowercase, split on
    ///     whitespace and on <see cref="Punctuation" />, empties dropped, each clipped to
    ///     <see cref="MaxTermLength" />.
    /// </summary>
    /// <remarks>
    ///     <b>This and the T-SQL in <c>TokenizeClause</c> have to agree, and nothing but a test makes
    ///     them.</b> They are two implementations of one rule — a query tokenized differently from the
    ///     document it should match produces no error, just an empty result, which is the quiet
    ///     failure this whole design is most exposed to. <c>the_query_tokenizer_matches_the_stored_one</c>
    ///     pins them together.
    /// </remarks>
    internal static string[] Tokenize(string text)
    {
        if (string.IsNullOrWhiteSpace(text)) return [];

        var buffer = text.ToCharArray();
        for (var i = 0; i < buffer.Length; i++)
        {
            if (char.IsWhiteSpace(buffer[i]) || Punctuation.Contains(buffer[i])) buffer[i] = ' ';
        }

        return new string(buffer)
            .ToLowerInvariant()
            .Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Select(x => x.Length > MaxTermLength ? x[..MaxTermLength] : x)
            .ToArray();
    }

    internal FullTextIndex(string jsonPath, MemberInfo[]? memberChain,
        FullTextTokenizer tokenizer = FullTextTokenizer.Literal)
    {
        JsonPath = jsonPath;
        MemberChain = memberChain;
        Tokenizer = tokenizer;
    }

    /// <summary>How this index breaks text into terms (#642). <see cref="FullTextTokenizer.Literal" /> by default.</summary>
    public FullTextTokenizer Tokenizer { get; }

    /// <summary>
    ///     What the token rows for this index are keyed by in the <c>member</c> column, and therefore
    ///     what every search filters on (#642).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>The tokenizer is part of the key, and that is what makes changing it safe.</b>
    ///         Stemmed and literal terms for the same member are different data, and the backfill decides
    ///         what to (re)tokenize by asking which keys have no rows. Keyed by member alone, switching an
    ///         existing index to <see cref="FullTextTokenizer.Porter" /> would find the member already
    ///         "indexed" with literal terms, backfill nothing, and the stemmed searches would quietly miss
    ///         every document written before the switch. With the tokenizer in the key, a switch is a new
    ///         key with no rows, so the backfill re-tokenizes everything, and
    ///         <see cref="BackfillStatements" /> clears the old key's rows.
    ///     </para>
    ///     <para>
    ///         A <see cref="FullTextTokenizer.Literal" /> index keeps the bare member name, exactly as before
    ///         #642, so an existing store needs no migration of its token rows.
    ///     </para>
    /// </remarks>
    internal string TokenKey => Tokenizer == FullTextTokenizer.Porter ? MemberName + "#porter" : MemberName;

    /// <summary>
    ///     Tokenize search text exactly as this index tokenized the stored text: <see cref="Tokenize" />,
    ///     then each term stemmed by <see cref="PorterStemmer" /> for a Porter index.
    /// </summary>
    internal string[] TokenizeQuery(string text)
    {
        var terms = Tokenize(text);
        return Tokenizer == FullTextTokenizer.Porter
            ? terms.Select(PorterStemmer.Stem).ToArray()
            : terms;
    }

    /// <summary>
    ///     The stored term for a token row: <c>s.value</c> from <see cref="TokenizeClause" />, wrapped in
    ///     the schema's <c>pc_porter_stem</c> for a Porter index, clipped to <see cref="MaxTermLength" />.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         An explicit <c>CONVERT</c> because it truncates silently where the implicit conversion of
    ///         an <c>INSERT</c> raises Msg 2628, and because it clips by the column's own measure: in a
    ///         UTF-8 collation <c>varchar(255)</c> is 255 bytes, which <c>LEFT(x, 255)</c> can overflow.
    ///     </para>
    ///     <para>
    ///         Whether the clip comes before or after the stem makes no difference, which is why
    ///         <see cref="Tokenize" /> can clip first: <c>pc_porter_stem</c> and
    ///         <see cref="PorterStemmer" /> both leave anything over 64 characters alone.
    ///     </para>
    /// </remarks>
    internal string TermExpression(DocumentMapping mapping)
    {
        var term = Tokenizer == FullTextTokenizer.Porter
            ? PorterStemFunction.Call(mapping.DatabaseSchemaName, "s.value")
            : "s.value";

        return $"CONVERT(varchar({MaxTermLength}), {term})";
    }

    /// <summary>
    ///     The JSON path the text is read from, re-rendered under the store's serializer naming
    ///     policy by <see cref="ApplyNamingPolicy" />.
    /// </summary>
    public string JsonPath { get; private set; }

    /// <summary>
    ///     The member chain the path came from, kept so the path can be re-rendered once the store's
    ///     policy is in reach. Null for a hand-written path, which is left verbatim.
    /// </summary>
    internal MemberInfo[]? MemberChain { get; }

    /// <summary>The member as the caller wrote it, for error messages and for addressing the index.</summary>
    internal string MemberName => MemberChain is { Length: > 0 }
        ? string.Join(".", MemberChain.Select(x => x.Name))
        : JsonPath;

    /// <summary>
    ///     Re-render the path under the store's serializer naming policy — the same #510 pass
    ///     <see cref="VectorIndex.ApplyNamingPolicy" /> makes, and for the same reason. An index built
    ///     from the CLR member name while the serializer wrote camelCase reads SQL NULL for every
    ///     row, which fails as an empty result rather than as an error.
    /// </summary>
    internal void ApplyNamingPolicy(StoreOptions options)
    {
        if (MemberChain is { Length: > 0 })
        {
            JsonPath = SerializedNames.PathFor(MemberChain, options);
        }
    }

    /// <summary>The side table holding the tokens: <c>pc_doc_thing</c> becomes <c>pc_ft_thing</c>.</summary>
    internal static string TableNameFor(DocumentMapping mapping) =>
        mapping.TableName.StartsWith("pc_doc_", StringComparison.OrdinalIgnoreCase)
            ? "pc_ft_" + mapping.TableName["pc_doc_".Length..]
            : "pc_ft_" + mapping.TableName;

    /// <summary>
    ///     #685: the one-time backfill that makes a newly declared index cover rows written before it
    ///     existed — one statement per declared member.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>This is a data migration, not a schema object, and it stays raw SQL for that reason.</b>
    ///         The token table and the trigger are now Weasel objects (<see cref="FullTextTokenTable" />,
    ///         <see cref="FullTextTrigger" />) so they appear in a generated script, are compared by
    ///         <c>AssertDatabaseMatchesConfigurationAsync()</c>, and can be refused under
    ///         <c>AutoCreate.None</c>. A backfill is none of those things: it describes rows rather than
    ///         structure, there is nothing in the catalog for a delta to compare it against, and it is
    ///         correct to run it after the schema converges rather than as part of converging.
    ///     </para>
    ///     <para>
    ///         Scoped to rows this member has no tokens for, so it is idempotent and so declaring a
    ///         second member later does not re-tokenize the first. A trigger only fires on writes made
    ///         after it exists, so without this a declaration against a store that already holds
    ///         documents produces an index matching nothing — no error, no rows, just a search that
    ///         quietly answers empty.
    ///     </para>
    ///     <para>
    ///         ⚠️ <b>Repairing an existing index is not this statement's job, and it is held back from
    ///         doing so</b> (#835, #839). Two kinds of document are out of date after a tokenizer change.
    ///         One with SOME tokens is invisible here, however wrong they are:
    ///         <c>"quick\nbrown fox"</c> keeps its stale <c>quick\nbrown</c> until it is next written.
    ///         One with NONE, like a member over 4000 characters, is exactly what <c>NOT EXISTS</c> finds,
    ///         and left alone this would tokenize all of them, in one statement, inside whichever request
    ///         happened to touch the type first. Measured: 8,000 such documents of 5,000 characters ran
    ///         into the 30 second command timeout, the statement rolled back, and the next request began
    ///         the same doomed attempt, so the type stayed unusable. Even 2,000 took 28 seconds.
    ///     </para>
    ///     <para>
    ///         So once a member has tokens, the bare <c>JSON_VALUE</c> below keeps this to what it could
    ///         always reach, and those documents wait for
    ///         <see cref="AdvancedOperations.RebuildFullTextIndexAsync{T}(CancellationToken)" />, which is batched and is
    ///         something an operator chooses to run. A member with no tokens at all is a new declaration
    ///         or a changed tokenizer, where the promise above applies and every document is covered.
    ///     </para>
    /// </remarks>
    internal static string[] BackfillStatements(DocumentMapping mapping, IReadOnlyList<FullTextIndex> indexes)
    {
        if (indexes.Count == 0) return [];

        var docTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, mapping.TableName);
        var ftTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, TableNameFor(mapping));
        var tenant = TenantColumn(mapping);
        var nativeJson = DocumentIndex.UsesNativeJson(mapping);

        // #642: rows under a key no declared index produces any more -- an index whose tokenizer
        // changed, or one that was removed -- are cleared first. The rows they hold were tokenized by a
        // rule nothing searches with, and leaving them is only harmless until a key is reused.
        var declaredKeys = string.Join(", ", indexes.Select(x => SqlEscaping.Literal(x.TokenKey)));
        var cleanup = $"DELETE FROM {ftTable} WHERE member NOT IN ({declaredKeys});";

        return indexes.Select(x =>
            $"""
             DECLARE @established bit = IIF(EXISTS (SELECT 1 FROM {ftTable} WHERE member = {SqlEscaping.Literal(x.TokenKey)}), 1, 0);
             INSERT INTO {ftTable} (doc_id, tenant_id, member, term, pos)
             SELECT i.id, {tenant}, {SqlEscaping.Literal(x.TokenKey)}, {x.TermExpression(mapping)}, {PositionExpression(mapping)}
             FROM {docTable} i
             {x.TokenizeClause("i", nativeJson)}
             AND (@established = 0 OR JSON_VALUE(i.data, '{x.JsonPath}') IS NOT NULL)
             AND NOT EXISTS (SELECT 1 FROM {ftTable} ft
                             WHERE ft.doc_id = i.id AND ft.tenant_id = {tenant}
                               AND ft.member = {SqlEscaping.Literal(x.TokenKey)});
             """).Prepend(cleanup).ToArray();
    }

    /// <summary>
    ///     A term's position among the document's terms, which is what a phrase search compares.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Numbered after the empties are dropped rather than read from <c>s.ordinal</c>, which counts
    ///         them. Every separator is mapped to a space, so two in a row leave an empty entry between
    ///         the words and a gap in the ordinals, and <c>quick</c> and <c>brown</c> stopped being
    ///         adjacent in <c>"quick, brown"</c>. #839 made that matter more: a CRLF is two separators, so
    ///         a phrase would have crossed <c>\n</c> and not <c>\r\n</c>.
    ///     </para>
    ///     <para>
    ///         Partitioned by document because both of its callers read many at once. The sort adds 7% of
    ///         CPU to a pass over a million tokens (6368ms against 6809ms).
    ///     </para>
    /// </remarks>
    internal static string PositionExpression(DocumentMapping mapping)
    {
        var document = mapping.TenancyStyle == TenancyStyle.Conjoined ? "i.tenant_id, i.id" : "i.id";
        return $"ROW_NUMBER() OVER (PARTITION BY {document} ORDER BY s.ordinal)";
    }

    /// <summary>
    ///     The conjoined-tenancy column, or the default tenant literal for a single-tenant table.
    ///     The token rows carry it so a search can key on (doc_id, tenant_id) — document ids are only
    ///     unique per tenant under conjoined tenancy, so joining on doc_id alone would cross tenants.
    /// </summary>
    internal static string TenantColumn(DocumentMapping mapping) =>
        mapping.TenancyStyle == TenancyStyle.Conjoined ? "i.tenant_id" : "'*DEFAULT*'";

    /// <summary>
    ///     The extra join term the maintenance trigger's DELETE needs so it removes only the written
    ///     tenant's tokens (#625).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>Without it, an ordinary write in one tenant silently unindexes another tenant's
    ///         document.</b> A document id is only unique PER TENANT under conjoined tenancy, so
    ///         <c>ft.doc_id = d.id</c> alone matches every tenant holding that id: the trigger deletes
    ///         all of their token rows and then re-inserts from <c>inserted</c>, which carries only the
    ///         rows actually written. The other tenant's document disappears from every full-text path
    ///         with no error, and stays gone until it is next written.
    ///     </para>
    ///     <para>
    ///         Empty for a single-tenant store, and that is required rather than an optimisation:
    ///         #234 left the <c>tenant_id</c> column off non-conjoined document tables entirely, so
    ///         <c>d.tenant_id</c> would not resolve. Those tables store the <c>'*DEFAULT*'</c> literal
    ///         in the token table, where an id is unique on its own, so there is nothing to
    ///         disambiguate.
    ///     </para>
    /// </remarks>
    internal static string DeletedTenantPredicate(DocumentMapping mapping) =>
        mapping.TenancyStyle == TenancyStyle.Conjoined ? " AND ft.tenant_id = d.tenant_id" : "";

    /// <summary>
    ///     <c>CROSS APPLY STRING_SPLIT(...)</c> over the member's text, lowercased and with whitespace
    ///     and punctuation mapped to spaces. <c>STRING_SPLIT</c>'s third argument is what supplies
    ///     <c>ordinal</c>, and the position is what makes a phrase search possible at all — storing
    ///     it now rather than later keeps that from becoming a schema migration.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         #835: the member is read through <see cref="SqlText.UnlimitedJsonString" />. This used to
    ///         be <c>CAST(JSON_VALUE(...) AS nvarchar(max))</c>, which looks unlimited and is not: the
    ///         cast applies after <c>JSON_VALUE</c> has already answered NULL, so a member over 4000
    ///         characters produced zero tokens and no error. Full-text search is aimed at long text.
    ///     </para>
    ///     <para>
    ///         #839: <c>TRANSLATE</c> runs under a binary collation so that it maps exactly the characters
    ///         <see cref="Tokenize" /> maps; see <see cref="SqlText.BinaryCollated" />. <c>LOWER</c> then
    ///         runs back under the database's collation, as it always has.
    ///     </para>
    ///     <para>
    ///         ⚠️ <b>This text is stored in the trigger and compared against <c>sys.sql_modules</c>, so a
    ///         change here is a schema change.</b> Every existing trigger reports <c>Update</c> and is
    ///         replaced on the next migration, which is safe, but it does nothing for tokens already
    ///         stored. See <see cref="BackfillStatements" />.
    ///     </para>
    /// </remarks>
    internal string TokenizeClause(string alias, bool nativeJson)
    {
        // The replacement is rendered from C# rather than computed in SQL. TRANSLATE demands its
        // second and third arguments be the same LENGTH, and deriving that in T-SQL is a trap from
        // both ends: LEN ignores trailing spaces, and DATALENGTH counts bytes — so the /2 that is
        // right for an N-prefixed literal is wrong for the plain one SqlEscaping.Literal emits, and
        // the failure is Msg 9828 at write time, inside the trigger, where it is least expected.
        var separators = $"{SqlEscaping.Literal(Punctuation)}+{SqlText.WhitespaceExpression}";
        var spaces = $"REPLICATE(N' ', {Punctuation.Length + SqlText.Whitespace.Length})";
        var text = SqlText.BinaryCollated(SqlText.UnlimitedJsonString($"{alias}.data", JsonPath, nativeJson));

        return $"""
                CROSS APPLY STRING_SPLIT(
                    LOWER({SqlText.DatabaseCollated($"TRANSLATE({text}, {separators}, {spaces})")}), N' ', 1) s
                WHERE LEN(s.value) > 0
                """;
    }
}
