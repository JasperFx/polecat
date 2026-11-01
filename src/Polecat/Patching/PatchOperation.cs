using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Polecat.Internal;
using Polecat.Serialization;
using Weasel.Core;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Patching;

/// <summary>
///     IStorageOperation that generates SQL UPDATE statements using JSON_MODIFY()
///     to patch document JSON data in-place.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: serializes patch values via ISerializer.ToJson when building the JSON_MODIFY() command. Document types are preserved at registration; AOT consumers supply a source-generator-backed ISerializer impl per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.ToJson is annotated RDC for the same reason as IL2026 above. AOT consumers supply a source-generator-backed ISerializer impl.")]
internal class PatchOperation : Polecat.Internal.IStorageOperation, Weasel.Storage.NoDataReturnedCall
{
    private readonly DocumentMapping _mapping;
    private readonly List<Action<ICommandBuilder>> _actions;
    private readonly Action<ICommandBuilder> _whereClauseWriter;

    public PatchOperation(DocumentMapping mapping, List<Action<ICommandBuilder>> actions,
        Action<ICommandBuilder> whereClauseWriter)
    {
        _mapping = mapping;
        _actions = actions;
        _whereClauseWriter = whereClauseWriter;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role() => OperationRole.Patch;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        foreach (var action in _actions)
        {
            builder.Append($"UPDATE {_mapping.QualifiedTableName} SET data = ");
            action(builder);
            builder.Append(", last_modified = SYSDATETIMEOFFSET() WHERE ");
            _whereClauseWriter(builder);
            builder.Append(";\n");
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;

    // OPENJSON's [type] column
    private const int JsonKindString = 1;
    private const int JsonKindNumber = 2;
    private const int JsonKindBoolean = 3;

    /// <summary>
    ///     Stands in for a number or a bool until its text replaces it. See
    ///     <see cref="WriteMemberValue" />. A fixed value rather than a new one per patch, so the SQL stays
    ///     the same text and reuses its plan.
    /// </summary>
    private const string LiteralPlaceholder = "pc_patch_literal_6f1d0c3ae2b84d7f9a51c0e47b2d8f36";

    /// <summary>
    ///     The array element in OPENJSON row <c>j</c>, as the JSON it was, for building an array back up
    ///     out of its rows. <c>j.value</c> alone is not that: a string comes back without its quotes or
    ///     its escapes, and a JSON null comes back as a SQL NULL, which <c>STRING_AGG</c> drops.
    /// </summary>
    private const string ElementAsJson =
        "CASE j.[type] WHEN 0 THEN N'null' WHEN 1 THEN N'\"' + STRING_ESCAPE(j.value, 'json') + N'\"' ELSE j.value END";

    // --- Static helpers for building JSON_MODIFY expressions ---

    /// <summary>
    ///     Escapes a JSON path (or a single path segment) that is inlined into a single-quoted
    ///     <c>'$.{path}'</c> literal inside a JSON_MODIFY / JSON_VALUE / JSON_QUERY call. The path is
    ///     assembled from runtime-supplied strings — a dictionary indexer key (see
    ///     <see cref="JsonPathHelper"/>), or a property/key name passed straight to the patch API
    ///     (e.g. <c>Patch.Set("name", ...)</c>, <c>Append(expr, key, ...)</c>) — so a value containing
    ///     a single quote would otherwise terminate the literal and inject arbitrary SQL into the
    ///     generated UPDATE (SQL injection, CWE-89; the same class of hole as JasperFx/marten#4911).
    ///     Doubling embedded single quotes keeps the value as data. The '.' path separators are not
    ///     quote characters, so escaping only single quotes leaves legitimate nested paths intact.
    /// </summary>
    internal static string EscapePath(string path) => SqlEscaping.LiteralBody(path);

    /// <summary>
    ///     The sort position of an inserted element, as the second column of its row. Existing elements
    ///     sit at the odd positions, so an even one lands before the element at the same index.
    /// </summary>
    /// <remarks>
    ///     #848: it is written into the SQL, and a negative index is allowed, so it cannot use the
    ///     culture's negative sign. sv-SE writes U+2212, which T-SQL refuses.
    /// </remarks>
    private static string SortOrderLiteral(int index)
        => string.Create(CultureInfo.InvariantCulture, $", {index * 2}");

    internal static Action<ICommandBuilder> SetScalar(string jsonPath, object? value)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', ");
            builder.AppendParameter(value ?? DBNull.Value);
            builder.Append(")");
        };
    }

    internal static Action<ICommandBuilder> SetComplex(string jsonPath, string jsonValue)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.AppendParameter(jsonValue);
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> IncrementInt(string jsonPath, object increment, string sqlType)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append(
                $"JSON_MODIFY(data, '$.{jsonPath}', ISNULL(CAST(JSON_VALUE(data, '$.{jsonPath}') AS {sqlType}), 0) + ");
            builder.AppendParameter(increment);
            builder.Append(")");
        };
    }

    internal static Action<ICommandBuilder> IncrementFloat(string jsonPath, object increment, string sqlType)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append(
                $"JSON_MODIFY(data, '$.{jsonPath}', CAST(ISNULL(CAST(JSON_VALUE(data, '$.{jsonPath}') AS {sqlType}), 0) + CAST(");
            builder.AppendParameter(increment);
            builder.Append($" AS {sqlType}) AS {sqlType}))");
        };
    }

    internal static Action<ICommandBuilder> AppendScalar(string jsonPath, object element)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', ");
            builder.AppendParameter(element);
            builder.Append(")");
        };
    }

    internal static Action<ICommandBuilder> AppendComplex(string jsonPath, string jsonElement)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', JSON_QUERY(");
            builder.AppendParameter(jsonElement);
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> AppendIfNotExistsScalar(string jsonPath, object element, string elementText)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append("WHERE value = CAST(");
            builder.AppendParameter(elementText);
            builder.Append($" AS nvarchar(max))) ");
            builder.Append($"THEN JSON_MODIFY(data, 'append $.{jsonPath}', ");
            builder.AppendParameter(element);
            builder.Append(") ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> AppendIfNotExistsComplex(string jsonPath, string jsonElement)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append("WHERE value = ");
            builder.AppendParameter(jsonElement);
            var paramName = "@" + builder.LastParameterName!;
            builder.Append(") ");
            builder.Append($"THEN JSON_MODIFY(data, 'append $.{jsonPath}', JSON_QUERY({paramName})) ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> SetDictKey(string dictPath, string key, string jsonValue)
    {
        dictPath = EscapePath(dictPath);
        key = EscapePath(key);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{dictPath}.{key}', JSON_QUERY(");
            builder.AppendParameter(jsonValue);
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> SetDictKeyIfNotExists(string dictPath, string key,
        Action<ICommandBuilder> set, bool nativeJson)
    {
        dictPath = EscapePath(dictPath);
        key = EscapePath(key);
        var scalar = SqlText.UnlimitedJsonString("data", $"$.{dictPath}.{key}", nativeJson);
        return builder =>
        {
            builder.Append($"CASE WHEN {scalar} IS NULL AND ");
            builder.Append($"JSON_QUERY(data, '$.{dictPath}.{key}') IS NULL THEN ");
            set(builder);
            builder.Append(" ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> InsertAtEnd(string jsonPath, object element, bool isComplex,
        string? jsonElement)
    {
        return isComplex ? AppendComplex(jsonPath, jsonElement!) : AppendScalar(jsonPath, element);
    }

    internal static Action<ICommandBuilder> InsertAtIndex(string jsonPath, int index, object element, bool isComplex,
        string? jsonElement, ISerializer serializer)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                "SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
            builder.Append("FROM (");
            builder.Append(
                $"SELECT {ElementAsJson} as val, CAST(j.[key] AS int) * 2 + 1 as sort_order FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("UNION ALL ");
            builder.Append("SELECT ");
            builder.AppendParameter(isComplex ? jsonElement! : serializer.ToJson(element));
            builder.Append(SortOrderLiteral(index));
            builder.Append(") t");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> InsertIfNotExistsScalar(string jsonPath, object element, string elementText,
        int? index, ISerializer serializer)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append("WHERE value = CAST(");
            builder.AppendParameter(elementText);
            builder.Append(" AS nvarchar(max))) THEN ");

            if (index.HasValue)
            {
                builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
                builder.Append("COALESCE(");
                builder.Append("'[' + (");
                builder.Append(
                    "SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
                builder.Append("FROM (");
                builder.Append(
                    $"SELECT {ElementAsJson} as val, CAST(j.[key] AS int) * 2 + 1 as sort_order FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
                builder.Append("UNION ALL ");
                builder.Append("SELECT ");
                builder.AppendParameter(serializer.ToJson(element));
                builder.Append(SortOrderLiteral(index.Value));
                builder.Append(") t");
                builder.Append(") + ']', '[]')");
                builder.Append("))");
            }
            else
            {
                builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', ");
                builder.AppendParameter(element);
                builder.Append(")");
            }

            builder.Append(" ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> InsertIfNotExistsComplex(string jsonPath, string jsonElement, int? index,
        ISerializer serializer)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) ");
            builder.Append("WHERE value = ");
            builder.AppendParameter(jsonElement);
            var paramName = "@" + builder.LastParameterName!;
            builder.Append(") THEN ");

            if (index.HasValue)
            {
                builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
                builder.Append("COALESCE(");
                builder.Append("'[' + (");
                builder.Append(
                    "SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
                builder.Append("FROM (");
                builder.Append(
                    $"SELECT {ElementAsJson} as val, CAST(j.[key] AS int) * 2 + 1 as sort_order FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
                builder.Append("UNION ALL ");
                builder.Append($"SELECT {paramName}");
                builder.Append(SortOrderLiteral(index.Value));
                builder.Append(") t");
                builder.Append(") + ']', '[]')");
                builder.Append("))");
            }
            else
            {
                builder.Append($"JSON_MODIFY(data, 'append $.{jsonPath}', JSON_QUERY({paramName}))");
            }

            builder.Append(" ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> RemoveScalarFirst(string jsonPath, string elementText)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                $"SELECT STRING_AGG({ElementAsJson}, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("WHERE CAST(j.[key] AS int) != ISNULL((");
            builder.Append(
                $"SELECT MIN(CAST(j2.[key] AS int)) FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j2 ");
            builder.Append("WHERE j2.value = CAST(");
            builder.AppendParameter(elementText);
            builder.Append(" AS nvarchar(max))");
            builder.Append("), -1)");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> RemoveScalarAll(string jsonPath, string elementText)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                $"SELECT STRING_AGG({ElementAsJson}, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("WHERE j.value IS NULL OR j.value != CAST(");
            builder.AppendParameter(elementText);
            builder.Append(" AS nvarchar(max))");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> RemoveComplexFirst(string jsonPath, string jsonElement)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                $"SELECT STRING_AGG({ElementAsJson}, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("WHERE CAST(j.[key] AS int) != ISNULL((");
            builder.Append(
                $"SELECT MIN(CAST(j2.[key] AS int)) FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j2 ");
            builder.Append("WHERE j2.value = ");
            builder.AppendParameter(jsonElement);
            builder.Append("), -1)");
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> RemoveComplexAll(string jsonPath, string jsonElement)
    {
        jsonPath = EscapePath(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', JSON_QUERY(");
            builder.Append("COALESCE(");
            builder.Append("'[' + (");
            builder.Append(
                $"SELECT STRING_AGG({ElementAsJson}, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
            builder.Append($"FROM OPENJSON(JSON_QUERY(data, '$.{jsonPath}')) j ");
            builder.Append("WHERE j.value IS NULL OR j.value != ");
            builder.AppendParameter(jsonElement);
            builder.Append(") + ']', '[]')");
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> RemoveDictKey(string dictPath, string key)
    {
        dictPath = EscapePath(dictPath);
        key = EscapePath(key);
        return builder => { builder.Append($"JSON_MODIFY(data, '$.{dictPath}.{key}', NULL)"); };
    }

    internal static Action<ICommandBuilder> DeleteProperty(string jsonPath)
    {
        jsonPath = EscapePath(jsonPath);
        return builder => { builder.Append($"JSON_MODIFY(data, '$.{jsonPath}', NULL)"); };
    }

    internal static Action<ICommandBuilder> RenameProperty(string oldJsonPath, string newJsonPath, bool nativeJson)
    {
        oldJsonPath = EscapePath(oldJsonPath);
        newJsonPath = EscapePath(newJsonPath);

        // A source that isn't there leaves the destination alone. This is the second run of a migration,
        // or a document already in the new shape, and writing the NULL would delete the renamed value.
        var removeOld = $"JSON_MODIFY(data, '$.{oldJsonPath}', NULL)";
        var sql = WriteMemberValue(oldJsonPath, nativeJson, removeOld,
            value => $"JSON_MODIFY(JSON_MODIFY(data, '$.{newJsonPath}', {value}), '$.{oldJsonPath}', NULL)");

        return builder => builder.Append(sql);
    }

    internal static Action<ICommandBuilder> DuplicateProperty(string sourcePath, string[] destPaths, bool nativeJson)
    {
        sourcePath = EscapePath(sourcePath);
        destPaths = Array.ConvertAll(destPaths, EscapePath);

        string writeAll(string value) => destPaths.Aggregate("data",
            (json, destination) => $"JSON_MODIFY({json}, '$.{destination}', {value})");

        // The serializer leaves a null member out, so a missing source is a null one, and copying a
        // null over the destinations is what was asked for.
        var sql = WriteMemberValue(sourcePath, nativeJson, writeAll("NULL"), writeAll);

        return builder => builder.Append(sql);
    }

    /// <summary>
    ///     Writes the value at <paramref name="escapedPath" /> somewhere else in the document, as the
    ///     same JSON it is now.
    /// </summary>
    /// <param name="escapedPath">The source, already through <see cref="EscapePath" />.</param>
    /// <param name="nativeJson">How <c>data</c> is stored. See <see cref="SqlText.UnlimitedJsonString" />.</param>
    /// <param name="whenMissing">The document to end up with when the source is absent or JSON null.</param>
    /// <param name="write">The <c>JSON_MODIFY</c> that writes a value, given the expression for it.</param>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>The kind has to come from the document, not from the CLR member.</b> One read cannot
    ///         cover all of these, because <c>JSON_MODIFY</c> decides what to store from the type of its
    ///         argument. Given <c>nvarchar</c> it stores a JSON string, so a bare <c>JSON_VALUE</c> turns
    ///         5 into "5" and <c>true</c> into "true", and that document then fails to load. Given a
    ///         <c>JSON_QUERY</c> it stores a fragment, but <c>JSON_QUERY</c> answers NULL for anything
    ///         that isn't an object or array. A CLR type cannot pick between them: <c>IsScalarType</c>
    ///         calls <see cref="TimeSpan" />, <see cref="Uri" /> and <c>byte[]</c> complex, each is
    ///         stored as a JSON string, and an <see cref="object" /> member can hold anything.
    ///     </para>
    ///     <para>
    ///         There is no argument that stores a number or a bool from its text. <c>JSON_QUERY</c>
    ///         refuses a bare <c>5</c> as malformed, <c>CAST(... AS json)</c> doesn't exist on SQL Server
    ///         2022, and casting to <c>decimal</c> or <c>float</c> changes the value: <c>long.MaxValue</c>
    ///         loses digits as a float, 1.10 loses its scale, and <c>JSON_MODIFY</c> renders a float to
    ///         16 significant digits, which is one short of a double. So those two kinds are written as
    ///         <see cref="LiteralPlaceholder" /> and the text of the value is put in its place.
    ///     </para>
    /// </remarks>
    private static string WriteMemberValue(string escapedPath, bool nativeJson, string whenMissing,
        Func<string, string> write)
    {
        var path = $"$.{escapedPath}";

        var separator = escapedPath.LastIndexOf('.');
        var parent = separator < 0 ? "$" : $"$.{escapedPath[..separator]}";
        var key = escapedPath[(separator + 1)..];

        // The same limit on every kind: JSON_QUERY has none, and for a number or a bool the unlimited
        // string read answers exactly the text that is stored.
        var fragment = $"JSON_QUERY(data, '{path}')";
        var text = SqlText.UnlimitedJsonString("data", path, nativeJson);
        var kind = $"(SELECT j.[type] FROM OPENJSON(data, '{parent}') j WHERE j.[key] = N'{key}')";

        return $"CASE WHEN {fragment} IS NOT NULL THEN {write(fragment)} "
               + $"WHEN {kind} = {JsonKindString} THEN {write(text)} "
               + $"WHEN {kind} IN ({JsonKindNumber}, {JsonKindBoolean}) THEN "
               + $"REPLACE(CAST({write($"N'{LiteralPlaceholder}'")} AS nvarchar(max)), N'\"{LiteralPlaceholder}\"', {text}) "
               + $"ELSE {whenMissing} END";
    }

    internal static bool IsScalarType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying.IsPrimitive || underlying == typeof(string) || underlying == typeof(decimal) ||
               underlying == typeof(Guid) || underlying == typeof(DateTime) ||
               underlying == typeof(DateTimeOffset) ||
               underlying == typeof(DateOnly) || underlying == typeof(TimeOnly) ||
               underlying.IsEnum;
    }
}
