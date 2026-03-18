using System.Linq.Expressions;
using System.Reflection;

namespace Polecat.Storage;

/// <summary>
///     Defines a custom index on a document table. Indexes are implemented as
///     persisted computed columns (using JSON_VALUE) with standard SQL Server indexes.
/// </summary>
public class DocumentIndex
{
    public DocumentIndex(string[] jsonPaths, string? indexName = null)
    {
        JsonPaths = jsonPaths;
        IndexName = indexName;
    }

    /// <summary>
    ///     The JSON paths (e.g., "$.userName", "$.address.city") to index.
    /// </summary>
    public string[] JsonPaths { get; }

    /// <summary>
    ///     Optional explicit index name. Auto-generated if null.
    /// </summary>
    public string? IndexName { get; set; }

    /// <summary>
    ///     If true, creates a UNIQUE index.
    /// </summary>
    public bool IsUnique { get; set; }

    /// <summary>
    ///     Tenancy scope for the index. When PerTenant, includes tenant_id in the index.
    /// </summary>
    public TenancyScope TenancyScope { get; set; } = TenancyScope.Global;

    /// <summary>
    ///     Optional WHERE clause predicate for a filtered (partial) index.
    /// </summary>
    public string? Predicate { get; set; }

    /// <summary>
    ///     SQL type hint for the indexed value (default: varchar(250) for string paths).
    /// </summary>
    public string? SqlType { get; set; }

    /// <summary>
    ///     Sort order for the index columns.
    /// </summary>
    public SortOrder SortOrder { get; set; } = SortOrder.Ascending;

    /// <summary>
    ///     Derives the computed column name for a JSON path.
    /// </summary>
    internal static string ColumnNameForPath(string jsonPath)
    {
        return "cc_" + jsonPath.Replace("$.", "").Replace(".", "_").ToLowerInvariant();
    }

    /// <summary>
    ///     Gets the index name (explicit or derived).
    /// </summary>
    internal string GetIndexName(string tableName)
    {
        return IndexName ?? DeriveIndexName(tableName);
    }

    /// <summary>
    ///     Generates DDL to add persisted computed columns and create the index.
    ///     Returns multiple SQL statements separated by newlines.
    /// </summary>
    internal string[] ToDdlStatements(DocumentMapping mapping)
    {
        var schema = mapping.DatabaseSchemaName;
        var table = mapping.TableName;
        var qualifiedTable = $"[{schema}].[{table}]";
        var name = GetIndexName(table);
        var unique = IsUnique ? "UNIQUE " : "";
        var sqlType = SqlType ?? "varchar(250)";

        var statements = new List<string>();

        // Add persisted computed columns for each JSON path
        foreach (var path in JsonPaths)
        {
            var colName = ColumnNameForPath(path);
            statements.Add($"""
                IF COL_LENGTH('{schema}.{table}', '{colName}') IS NULL
                    ALTER TABLE {qualifiedTable} ADD [{colName}] AS CAST(JSON_VALUE(data, '{path}') AS {sqlType}) PERSISTED;
                """);
        }

        // Build index column list
        var indexColumns = new List<string>();
        if (TenancyScope == TenancyScope.PerTenant)
        {
            indexColumns.Add("tenant_id");
        }

        foreach (var path in JsonPaths)
        {
            var colName = ColumnNameForPath(path);
            var sortDir = SortOrder == SortOrder.Descending ? " DESC" : "";
            indexColumns.Add($"[{colName}]{sortDir}");
        }

        var columnList = string.Join(", ", indexColumns);
        var where = !string.IsNullOrEmpty(Predicate) ? $" WHERE {Predicate}" : "";

        statements.Add($"""
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{name}'
                           AND object_id = OBJECT_ID('{qualifiedTable}'))
                CREATE {unique}NONCLUSTERED INDEX [{name}] ON {qualifiedTable} ({columnList}){where};
            """);

        return statements.ToArray();
    }

    private string DeriveIndexName(string tableName)
    {
        var prefix = IsUnique ? "ux" : "ix";
        var pathSuffix = string.Join("_", JsonPaths.Select(p =>
            p.Replace("$.", "").Replace(".", "_").ToLowerInvariant()));
        return $"{prefix}_{tableName}_{pathSuffix}";
    }

    /// <summary>
    ///     Resolves a member expression to a JSON path (e.g., x => x.UserName → "$.userName").
    /// </summary>
    internal static string MemberToJsonPath(MemberInfo member)
    {
        var name = member.Name;
        return "$." + char.ToLowerInvariant(name[0]) + name[1..];
    }

    /// <summary>
    ///     Resolves a lambda expression to JSON paths for indexing.
    /// </summary>
    internal static string[] ResolveJsonPaths<T>(Expression<Func<T, object?>> expression)
    {
        var body = expression.Body;

        // Unwrap Convert for value types
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            body = unary.Operand;
        }

        // Single member: x => x.Property
        if (body is MemberExpression memberExpr)
        {
            return [MemberToJsonPath(memberExpr.Member)];
        }

        // Anonymous type: x => new { x.Prop1, x.Prop2 }
        if (body is NewExpression newExpr)
        {
            return newExpr.Arguments
                .OfType<MemberExpression>()
                .Select(m => MemberToJsonPath(m.Member))
                .ToArray();
        }

        throw new ArgumentException(
            $"Expression '{expression}' is not a supported index expression. " +
            "Use a single property (x => x.Prop) or anonymous type (x => new {{ x.Prop1, x.Prop2 }}).");
    }
}

/// <summary>
///     Sort order for index columns.
/// </summary>
public enum SortOrder
{
    Ascending,
    Descending
}

/// <summary>
///     Tenancy scope for indexes.
/// </summary>
public enum TenancyScope
{
    /// <summary>
    ///     Index applies globally (across all tenants).
    /// </summary>
    Global,

    /// <summary>
    ///     Index is scoped per tenant (includes tenant_id column).
    /// </summary>
    PerTenant
}
