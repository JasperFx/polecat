using System.Linq.Expressions;
using Polecat.Internal;

namespace Polecat.Storage;

/// <summary>
///     A native SQL Server 2025 JSON index (<c>CREATE JSON INDEX</c>) over a document's native
///     <c>json</c> <c>data</c> column. Unlike a computed-column <see cref="DocumentIndex" />, a single
///     JSON index can cover many JSON paths and accelerates <c>JSON_VALUE</c> (equality, including the
///     <c>RETURNING</c> form), <c>JSON_PATH_EXISTS</c>, and <c>JSON_CONTAINS</c> predicates without any
///     per-path computed columns.
///
///     Requirements (enforced / documented):
///     - The <c>data</c> column must be the native <c>json</c> type — i.e. <c>UseNativeJsonType = true</c>
///       (SQL Server 2025+). A computed-column <see cref="DocumentIndex" /> is the portable alternative.
///     - The table needs a clustered primary key whose key is ≤128 bytes. Polecat's single-tenant
///       <c>id</c> PK satisfies this; per-tenant tables (whose PK prepends a <c>varchar</c> tenant_id)
///       can exceed the limit, in which case SQL Server rejects the index.
///     - Only one JSON index can exist per <c>json</c> column, so a table has at most one JSON index.
///     - Indexed paths can't overlap (e.g. <c>$.a</c> and <c>$.a.b</c>).
///     - Does not support UNIQUE, filtered (WHERE), INCLUDE, ORDER BY/range seeks, or LIKE/IS NULL —
///       use a computed-column <see cref="DocumentIndex" /> for those.
/// </summary>
public class JsonIndex
{
    public JsonIndex(string[] jsonPaths, string? indexName = null)
    {
        JsonPaths = jsonPaths;
        IndexName = indexName;
    }

    /// <summary>
    ///     The JSON paths to index (e.g. "$.serviceName", "$.address.city"). When empty, the entire
    ///     JSON document is indexed (the <c>FOR</c> clause is omitted).
    /// </summary>
    /// <remarks>
    ///     #510: settable for the same reason as <see cref="DocumentIndex.JsonPaths" /> — re-rendered
    ///     once the store's naming policy is known. A hand-written path is left verbatim.
    /// </remarks>
    public string[] JsonPaths { get; set; }

    /// <summary>
    ///     #510: the member chains the paths came from, when they came from a lambda. Null for a
    ///     hand-written path.
    /// </summary>
    internal System.Reflection.MemberInfo[][]? MemberChains { get; set; }

    /// <summary>
    ///     #510: re-render the paths under the store's serializer naming policy. A JSON index names
    ///     its paths in a <c>FOR</c> clause, so a camelCased path on a snake_case store indexes
    ///     something the document does not contain — the same silent miss as
    ///     <see cref="DocumentIndex" />, and invisible for the same reason.
    /// </summary>
    internal void ApplyNamingPolicy(StoreOptions options)
    {
        if (MemberChains is { Length: > 0 })
        {
            JsonPaths = MemberChains.Select(chain => SerializedNames.PathFor(chain, options)).ToArray();
        }
    }

    /// <summary>
    ///     Optional explicit index name. Auto-derived from the table name when null.
    /// </summary>
    public string? IndexName { get; set; }

    /// <summary>
    ///     Maps to <c>WITH (OPTIMIZE_FOR_ARRAY_SEARCH = ON)</c> — tunes the index for searching inside
    ///     JSON arrays (e.g. <c>JSON_CONTAINS</c> over an array property).
    /// </summary>
    public bool OptimizeForArraySearch { get; set; }

    /// <summary>
    ///     Optional <c>WITH (FILLFACTOR = n)</c>, 1–100.
    /// </summary>
    public int? FillFactor { get; set; }

    /// <summary>
    ///     Derives the index name. One JSON index per table, so the table name alone is unique.
    /// </summary>
    internal string GetIndexName(string tableName) => IndexName ?? $"jidx_{tableName}";

    // #685: ToDdlStatements is gone. The CREATE JSON INDEX grammar now lives in Weasel's
    // JsonIndexDefinition (weasel#661) and is declared on DocumentTable, so there is ONE description
    // of this object rather than a rendered string beside a model that could not see it. The native
    // json column type is still required, and DocumentTable.AddDeclaredJsonIndexes is where that is
    // now checked.

    /// <summary>
    ///     Resolves a lambda to the JSON paths to index — a single (possibly nested) member or an
    ///     anonymous type combining several. Reuses <see cref="DocumentIndex.ResolveJsonPaths{T}" />.
    /// </summary>
    internal static string[] ResolveJsonPaths<T>(Expression<Func<T, object?>> expression)
        => DocumentIndex.ResolveJsonPaths(expression);

    /// <summary>
    ///     #510: member chains behind an expression. Reuses
    ///     <see cref="DocumentIndex.ResolveMemberChains{T}" />.
    /// </summary>
    internal static System.Reflection.MemberInfo[][] ResolveMemberChains<T>(
        Expression<Func<T, object?>> expression)
        => DocumentIndex.ResolveMemberChains(expression);
}
