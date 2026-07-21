using System.Linq.Expressions;

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
    public string[] JsonPaths { get; }

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

    /// <summary>
    ///     Generates the <c>CREATE JSON INDEX</c> DDL. Throws when the store isn't using the native
    ///     json column type, since the statement is invalid against <c>nvarchar(max)</c> storage.
    /// </summary>
    internal string[] ToDdlStatements(DocumentMapping mapping)
    {
        if (mapping.JsonColumnType != "json")
        {
            throw new InvalidOperationException(
                $"A JSON index on '{mapping.DocumentType.Name}' requires the native json column type. " +
                "Set UseNativeJsonType = true (SQL Server 2025+), or use a computed-column Index(...) instead.");
        }

        var schema = mapping.DatabaseSchemaName;
        var table = mapping.TableName;
        var qualifiedTable = $"[{schema}].[{table}]";
        var name = GetIndexName(table);

        var forClause = JsonPaths.Length > 0
            ? " FOR (" + string.Join(", ", JsonPaths.Select(p => $"'{p}'")) + ")"
            : "";

        var withOptions = new List<string>();
        if (OptimizeForArraySearch) withOptions.Add("OPTIMIZE_FOR_ARRAY_SEARCH = ON");
        if (FillFactor.HasValue) withOptions.Add($"FILLFACTOR = {FillFactor.Value}");
        var withClause = withOptions.Count > 0 ? " WITH (" + string.Join(", ", withOptions) + ")" : "";

        // CREATE JSON INDEX requires SET QUOTED_IDENTIFIER ON; set it explicitly so the statement is
        // robust regardless of the caller's session options. Only one JSON index per json column is
        // allowed, so existence is keyed on the table, not the index name.
        return
        [
            $"""
             SET QUOTED_IDENTIFIER ON;
             IF NOT EXISTS (SELECT 1 FROM sys.json_indexes WHERE object_id = OBJECT_ID('{qualifiedTable}'))
                 CREATE JSON INDEX [{name}] ON {qualifiedTable} (data){forClause}{withClause};
             """
        ];
    }

    /// <summary>
    ///     Resolves a lambda to the JSON paths to index — a single (possibly nested) member or an
    ///     anonymous type combining several. Reuses <see cref="DocumentIndex.ResolveJsonPaths{T}" />.
    /// </summary>
    internal static string[] ResolveJsonPaths<T>(Expression<Func<T, object?>> expression)
        => DocumentIndex.ResolveJsonPaths(expression);
}
