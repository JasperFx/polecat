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
///         tokenization. Terms are lowercased and split on punctuation and whitespace. There is no
///         stemming, no thesaurus, and no language-aware word breaking — <c>running</c> does not
///         match <c>run</c>. Marten's <c>regConfig</c> overloads have no counterpart and are
///         deliberately not offered rather than accepted and ignored.
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
    ///     The characters a token is split on, beyond whitespace. Everything here is mapped to a
    ///     space before splitting, so <c>"quick,brown"</c> is two terms rather than one.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Kept as one string because <c>TRANSLATE</c> takes a from/to pair of EQUAL length — a
    ///         mismatch is Msg 9828 at schema time, and <c>LEN</c> cannot be used to measure it because
    ///         <c>LEN</c> ignores trailing spaces. The replacement is built with
    ///         <c>REPLICATE(N' ', DATALENGTH(@p)/2)</c> for that reason.
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
    ///     whitespace and on <see cref="Punctuation" />, empties dropped.
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
            if (Punctuation.Contains(buffer[i])) buffer[i] = ' ';
        }

        return new string(buffer)
            .ToLowerInvariant()
            .Split(' ', StringSplitOptions.RemoveEmptyEntries);
    }

    internal FullTextIndex(string jsonPath, MemberInfo[]? memberChain)
    {
        JsonPath = jsonPath;
        MemberChain = memberChain;
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
    ///     The DDL that creates the token table, its index, the trigger that maintains it, and the
    ///     backfill for rows that predate the declaration — in that order, each its own statement
    ///     because a <c>CREATE TRIGGER</c> has to begin its batch.
    /// </summary>
    /// <remarks>
    ///     <b>Static over the whole collection, not per index, and that is load-bearing.</b> One
    ///     trigger maintains one table, so a per-index rendering would have the second declared
    ///     member's <c>CREATE OR ALTER</c> replace the first member's trigger — leaving the first
    ///     silently unmaintained, which is the same shape of quiet failure the backfill exists to
    ///     prevent. The trigger body therefore covers every declared member in one pass.
    /// </remarks>
    internal static string[] ToDdlStatements(DocumentMapping mapping, IReadOnlyList<FullTextIndex> indexes)
    {
        if (indexes.Count == 0) return [];

        var docTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, mapping.TableName);
        var ftTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, TableNameFor(mapping));
        var ftIndexName = "ix_" + TableNameFor(mapping) + "_term";
        var triggerName = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, "tr_" + TableNameFor(mapping));
        var idType = IdColumnType(mapping);
        var tenant = TenantColumn(mapping);
        var deletedTenant = DeletedTenantPredicate(mapping);

        var statements = new List<string>
        {
            $"""
             IF OBJECT_ID({SqlEscaping.Literal(ftTable)}, 'U') IS NULL
             CREATE TABLE {ftTable} (
                 doc_id {idType} NOT NULL,
                 tenant_id varchar(250) NOT NULL,
                 member varchar(200) NOT NULL,
                 term varchar(255) NOT NULL,
                 pos int NOT NULL
             );
             """,

            $"""
             IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = {SqlEscaping.Literal(ftIndexName)}
                            AND object_id = OBJECT_ID({SqlEscaping.Literal(ftTable)}))
             CREATE INDEX {SqlEscaping.QuoteIdentifier(ftIndexName)} ON {ftTable} (term, doc_id) INCLUDE (pos, member, tenant_id);
             """
        };

        // One SELECT per declared member, fused into a single INSERT. CREATE OR ALTER so a
        // re-declaration re-renders the body rather than needing a drop, and so this is idempotent
        // like every other statement here.
        var legs = string.Join("\n    UNION ALL\n", indexes.Select(x =>
            $"""
                 SELECT i.id, {tenant}, {SqlEscaping.Literal(x.MemberName)}, s.value, s.ordinal
                 FROM inserted i
                 {x.TokenizeClause("i")}
             """));

        statements.Add(
            $"""
             CREATE OR ALTER TRIGGER {triggerName} ON {docTable} AFTER INSERT, UPDATE, DELETE
             AS
             BEGIN
                 SET NOCOUNT ON;
                 DELETE ft FROM {ftTable} ft INNER JOIN deleted d ON ft.doc_id = d.id{deletedTenant};
                 INSERT INTO {ftTable} (doc_id, tenant_id, member, term, pos)
             {legs}
             END
             """);

        // Backfill, one statement per member. Scoped to rows this member has no tokens for, so it is
        // idempotent and so declaring a second member later does not re-tokenize the first.
        statements.AddRange(indexes.Select(x =>
            $"""
             INSERT INTO {ftTable} (doc_id, tenant_id, member, term, pos)
             SELECT i.id, {tenant}, {SqlEscaping.Literal(x.MemberName)}, s.value, s.ordinal
             FROM {docTable} i
             {x.TokenizeClause("i")}
             AND NOT EXISTS (SELECT 1 FROM {ftTable} ft
                             WHERE ft.doc_id = i.id AND ft.tenant_id = {tenant}
                               AND ft.member = {SqlEscaping.Literal(x.MemberName)});
             """));

        return statements.ToArray();
    }

    /// <summary>
    ///     The token table's key column has to match the document table's id column exactly, which
    ///     means the INNER type for a strongly-typed id — the same rule #296/#302 settled for
    ///     <c>DocumentTable</c>, and the same varchar landmine if it is got wrong.
    /// </summary>
    private static string IdColumnType(DocumentMapping mapping) =>
        mapping.InnerIdType == typeof(Guid) ? "uniqueidentifier"
        : mapping.InnerIdType == typeof(int) ? "int"
        : mapping.InnerIdType == typeof(long) ? "bigint"
        : "varchar(250)";

    /// <summary>
    ///     The conjoined-tenancy column, or the default tenant literal for a single-tenant table.
    ///     The token rows carry it so a search can key on (doc_id, tenant_id) — document ids are only
    ///     unique per tenant under conjoined tenancy, so joining on doc_id alone would cross tenants.
    /// </summary>
    private static string TenantColumn(DocumentMapping mapping) =>
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
    private static string DeletedTenantPredicate(DocumentMapping mapping) =>
        mapping.TenancyStyle == TenancyStyle.Conjoined ? " AND ft.tenant_id = d.tenant_id" : "";

    /// <summary>
    ///     <c>CROSS APPLY STRING_SPLIT(...)</c> over the member's text, lowercased and with
    ///     punctuation mapped to spaces. <c>STRING_SPLIT</c>'s third argument is what supplies
    ///     <c>ordinal</c>, and the position is what makes a phrase search possible at all — storing
    ///     it now rather than later keeps that from becoming a schema migration.
    /// </summary>
    private string TokenizeClause(string alias)
    {
        // The replacement is rendered from C# rather than computed in SQL. TRANSLATE demands its
        // second and third arguments be the same LENGTH, and deriving that in T-SQL is a trap from
        // both ends: LEN ignores trailing spaces, and DATALENGTH counts bytes — so the /2 that is
        // right for an N-prefixed literal is wrong for the plain one SqlEscaping.Literal emits, and
        // the failure is Msg 9828 at write time, inside the trigger, where it is least expected.
        var punct = SqlEscaping.Literal(Punctuation);
        var spaces = SqlEscaping.Literal(new string(' ', Punctuation.Length));

        return $"""
                CROSS APPLY STRING_SPLIT(
                    LOWER(TRANSLATE(
                        CAST(JSON_VALUE({alias}.data, '{JsonPath}') AS nvarchar(max)),
                        {punct},
                        {spaces})), N' ', 1) s
                WHERE LEN(s.value) > 0
                """;
    }
}
