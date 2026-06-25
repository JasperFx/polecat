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
    ///     SQL type hint applied to every path in the index. When null, the type is derived from
    ///     each member's CLR type (so an int member indexes as int, a DateTimeOffset as
    ///     datetimeoffset, a string as varchar(250)). A non-null value overrides every path.
    ///     For per-path control in a composite index, use <see cref="SqlTypeByPath" />.
    /// </summary>
    public string? SqlType { get; set; }

    /// <summary>
    ///     #223: per-path SQL type overrides for composite indexes, keyed by JSON path
    ///     (e.g. "$.bucketEnd"). Lets a composite over a string + a date type each column
    ///     correctly. Takes precedence over <see cref="SqlType" /> and the CLR-derived type.
    /// </summary>
    public Dictionary<string, string> SqlTypeByPath { get; } = new();

    /// <summary>
    ///     JSON paths whose values are carried in the index as non-key INCLUDE columns (covering
    ///     index). Each include path gets its own persisted computed column. Including the columns a
    ///     query also selects lets SQL Server satisfy the query from the index alone, avoiding a key
    ///     lookup back into the table. Include columns are always Default casing (payload, not keys).
    /// </summary>
    public string[] IncludePaths { get; set; } = [];

    /// <summary>
    ///     Sort order for the index columns.
    /// </summary>
    public SortOrder SortOrder { get; set; } = SortOrder.Ascending;

    /// <summary>
    ///     Marks the column value as upper/lower casing for case-insensitive indexing.
    ///     Only applies to string-typed columns; non-string columns ignore this setting.
    /// </summary>
    public IndexCasing Casing { get; set; } = IndexCasing.Default;

    /// <summary>
    ///     Derives the computed column name for a JSON path.
    /// </summary>
    internal static string ColumnNameForPath(string jsonPath)
    {
        return ColumnNameForPath(jsonPath, IndexCasing.Default);
    }

    /// <summary>
    ///     Derives the computed column name for a JSON path with casing suffix.
    /// </summary>
    internal static string ColumnNameForPath(string jsonPath, IndexCasing casing)
    {
        var baseName = "cc_" + jsonPath.Replace("$.", "").Replace(".", "_").ToLowerInvariant();
        return casing switch
        {
            IndexCasing.Upper => baseName + "_upper",
            IndexCasing.Lower => baseName + "_lower",
            _ => baseName
        };
    }

    /// <summary>
    ///     #223: resolves the SQL type for a single indexed path. Precedence:
    ///     per-path override → index-wide <see cref="SqlType" /> → CLR-derived type → varchar(250).
    /// </summary>
    internal string ResolveSqlType(string jsonPath, Type? clrType)
    {
        if (SqlTypeByPath.TryGetValue(jsonPath, out var perPath)) return perPath;
        if (SqlType != null) return SqlType;
        if (clrType != null && SqlTypeMap.For(clrType) is { } derived) return derived;
        return "varchar(250)";
    }

    /// <summary>
    ///     #223: builds the computed-column / predicate expression for a path. This single method
    ///     feeds both the index DDL and the LINQ translator (via MemberFactory), which is what lets
    ///     SQL Server match a predicate to the persisted computed column and seek the index.
    ///     Date/time types use a deterministic CONVERT(..., 126) so the column can be PERSISTED
    ///     (a CAST of a string to a date/time type is non-deterministic and rejected by PERSISTED).
    /// </summary>
    internal static string ComputedColumnExpression(string jsonPath, string sqlType, IndexCasing casing)
    {
        var json = $"JSON_VALUE(data, '{jsonPath}')";
        var expr = SqlTypeMap.IsDateTimeFamily(sqlType)
            ? $"CONVERT({sqlType}, {json}, 126)"
            : $"CAST({json} AS {sqlType})";

        return casing switch
        {
            IndexCasing.Upper => $"UPPER({expr})",
            IndexCasing.Lower => $"LOWER({expr})",
            _ => expr
        };
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

        var statements = new List<string>();

        // Add persisted computed columns for each key JSON path
        foreach (var path in JsonPaths)
        {
            var colName = ColumnNameForPath(path, Casing);
            var sqlType = ResolveSqlType(path, mapping.ResolveClrMemberType(path));
            var castedExpr = ComputedColumnExpression(path, sqlType, Casing);

            statements.Add($"""
                IF COL_LENGTH('{schema}.{table}', '{colName}') IS NULL
                    ALTER TABLE {qualifiedTable} ADD [{colName}] AS {castedExpr} PERSISTED;
                """);
        }

        // Add persisted computed columns for each INCLUDE (covering) path — always Default casing.
        foreach (var path in IncludePaths)
        {
            var colName = ColumnNameForPath(path, IndexCasing.Default);
            var sqlType = ResolveSqlType(path, mapping.ResolveClrMemberType(path));
            var castedExpr = ComputedColumnExpression(path, sqlType, IndexCasing.Default);

            statements.Add($"""
                IF COL_LENGTH('{schema}.{table}', '{colName}') IS NULL
                    ALTER TABLE {qualifiedTable} ADD [{colName}] AS {castedExpr} PERSISTED;
                """);
        }

        // Build index column list
        var indexColumns = new List<string>();
        if (TenancyScope == TenancyScope.PerTenant)
        {
            indexColumns.Add(JasperFx.StorageConstants.TenantIdColumn);
        }

        foreach (var path in JsonPaths)
        {
            var colName = ColumnNameForPath(path, Casing);
            var sortDir = SortOrder == SortOrder.Descending ? " DESC" : "";
            indexColumns.Add($"[{colName}]{sortDir}");
        }

        var columnList = string.Join(", ", indexColumns);
        var include = IncludePaths.Length > 0
            ? " INCLUDE (" + string.Join(", ",
                IncludePaths.Select(p => $"[{ColumnNameForPath(p, IndexCasing.Default)}]")) + ")"
            : "";
        var where = !string.IsNullOrEmpty(Predicate) ? $" WHERE {Predicate}" : "";

        statements.Add($"""
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{name}'
                           AND object_id = OBJECT_ID('{qualifiedTable}'))
                CREATE {unique}NONCLUSTERED INDEX [{name}] ON {qualifiedTable} ({columnList}){include}{where};
            """);

        return statements.ToArray();
    }

    private string DeriveIndexName(string tableName)
    {
        var prefix = IsUnique ? "ux" : "ix";
        var pathSuffix = string.Join("_", JsonPaths.Select(p =>
            p.Replace("$.", "").Replace(".", "_").ToLowerInvariant()));
        var casingSuffix = Casing switch
        {
            IndexCasing.Upper => "_upper",
            IndexCasing.Lower => "_lower",
            _ => ""
        };
        return $"{prefix}_{tableName}_{pathSuffix}{casingSuffix}";
    }

    /// <summary>
    ///     Resolves a single, non-nested member to a JSON path (e.g., UserName → "$.userName").
    ///     Used for attribute-based indexes, where the member is always a direct property.
    /// </summary>
    internal static string MemberToJsonPath(MemberInfo member)
    {
        var name = member.Name;
        return "$." + char.ToLowerInvariant(name[0]) + name[1..];
    }

    /// <summary>
    ///     Resolves a (possibly nested) member access expression to a JSON path by walking the
    ///     member chain, e.g. x => x.Address.City → "$.address.city". This mirrors the LINQ
    ///     translator's MemberFactory.BuildJsonPath so an index path and the query predicate path
    ///     are identical (otherwise the computed-column index can never match). Each segment is
    ///     camelCased, matching the default serializer casing the rest of the index DSL assumes.
    /// </summary>
    internal static string MemberExpressionToJsonPath(MemberExpression expression)
    {
        var segments = new List<string>();
        var current = expression;

        while (current != null)
        {
            var name = current.Member.Name;
            segments.Insert(0, char.ToLowerInvariant(name[0]) + name[1..]);

            if (current.Expression is ParameterExpression)
                break;

            current = current.Expression as MemberExpression;
        }

        return "$." + string.Join(".", segments);
    }

    /// <summary>
    ///     Resolves a lambda expression to JSON paths for indexing. Supports a single (possibly
    ///     nested) member — x => x.Prop or x => x.Address.City — and an anonymous type combining
    ///     several — x => new { x.Prop1, x.Address.City }.
    /// </summary>
    internal static string[] ResolveJsonPaths<T>(Expression<Func<T, object?>> expression)
    {
        var body = expression.Body;

        // Unwrap Convert for value types
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            body = unary.Operand;
        }

        // Single (possibly nested) member: x => x.Property / x => x.Address.City
        if (body is MemberExpression memberExpr)
        {
            return [MemberExpressionToJsonPath(memberExpr)];
        }

        // Anonymous type: x => new { x.Prop1, x.Address.City }
        if (body is NewExpression newExpr)
        {
            return newExpr.Arguments
                .Select(UnwrapToMemberExpression)
                .Where(m => m != null)
                .Select(m => MemberExpressionToJsonPath(m!))
                .ToArray();
        }

        throw new ArgumentException(
            $"Expression '{expression}' is not a supported index expression. " +
            "Use a single property (x => x.Prop), a nested property (x => x.Address.City), " +
            "or an anonymous type (x => new {{ x.Prop1, x.Prop2 }}).");
    }

    /// <summary>
    ///     Strips a Convert wrapper (value-type members boxed to object in some anonymous-type
    ///     arguments) and returns the underlying MemberExpression, or null if it isn't one.
    /// </summary>
    private static MemberExpression? UnwrapToMemberExpression(Expression expression)
    {
        if (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            expression = unary.Operand;
        }

        return expression as MemberExpression;
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

/// <summary>
///     Case transformation for indexed string columns.
///     Non-string columns ignore this setting.
/// </summary>
public enum IndexCasing
{
    /// <summary>
    ///     Leave the casing as-is (default).
    /// </summary>
    Default,

    /// <summary>
    ///     Transform the indexed value to uppercase for case-insensitive lookups.
    /// </summary>
    Upper,

    /// <summary>
    ///     Transform the indexed value to lowercase for case-insensitive lookups.
    /// </summary>
    Lower
}
