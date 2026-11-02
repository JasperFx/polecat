using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
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

    /// <summary>
    ///     What every <c>JSON_MODIFY</c> here writes into, in place of the <c>data</c> column.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>SQL Server 2025 RTM-CU5 damages a native <c>json</c> value when <c>JSON_MODIFY</c> adds
    ///         a key to it</b>, and any value at all will do it, a string or a number included. The row
    ///         still reads back correctly, which is why nothing notices, but the next
    ///         <c>JSON_MODIFY</c> with a path through an object fails with "Encountered corrupted JSON
    ///         type" or, worse, writes to some other member: <c>$.middle.child</c> was seen going into a
    ///         sibling called <c>map</c>, with no error. In one statement or in two, it makes no difference.
    ///     </para>
    ///     <para>
    ///         Text doesn't have the problem, and assigning text to the column parses it from scratch.
    ///         Every case that failed on the column passed this way, and a row already in that state is
    ///         put right by the next patch that touches it, which is why this is on every write and
    ///         not only on those that might add a key.
    ///     </para>
    ///     <para>
    ///         This has a price, because it replaces a change to the stored value with a full parse.
    ///         Per update, measured on the same server: 0.7ms to 0.9ms at 1KB, 1.0ms to 2.8ms at 10KB,
    ///         1.7ms to 14.8ms at 100KB and 4.1ms to 170ms at 1MB. <c>nvarchar(max)</c> pays nothing, the
    ///         cast is a no-op. If a later build fixes this, here is the one place to take it out.
    ///     </para>
    /// </remarks>
    private const string Document = "CAST(data AS nvarchar(max))";

    // OPENJSON's [type] column
    internal const int JsonKindNull = 0;
    internal const int JsonKindString = 1;
    internal const int JsonKindNumber = 2;
    internal const int JsonKindBoolean = 3;

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

    /// <summary>
    ///     The two halves of the text of a <c>float</c>, either side of it. Style 3 is the only one
    ///     that keeps all 17 digits a double can need, and it writes an exponent, which is what
    ///     <c>nvarchar(max)</c> is happy to hold and the <c>json</c> type is not. So for that type the
    ///     digits are written out, with the rule in <see cref="NativeJsonNumbers" />.
    /// </summary>
    /// <remarks>
    ///     A sum can leave the range even where both sides of it are inside. There is no statement to
    ///     raise that from in an expression, so the conversion to <c>int</c> is what fails, with the
    ///     text of the number in its message. Past the limit the server would refuse an integer anyway,
    ///     but a fraction it would round, and that is the case this has to stop.
    /// </remarks>
    private static (string Before, string After) FloatAsJson(bool nativeJson)
    {
        if (!nativeJson)
        {
            return ("CONVERT(nvarchar(30), ", ", 3)");
        }

        const string length =
            "CASE WHEN pc_n.point_at <= 0 THEN LEN(pc_n.digits) - pc_n.point_at " +
            "WHEN pc_n.point_at > LEN(pc_n.digits) THEN pc_n.point_at ELSE LEN(pc_n.digits) END";

        return (
            "(SELECT CASE WHEN pc_n.digits = N'' THEN N'0' " +
            $"WHEN {length} > {NativeJsonNumbers.MaxDigits} THEN " +
            "CONVERT(nvarchar(30), CONVERT(int, N'The native json type cannot hold the number ' + pc_s.t)) " +
            "WHEN pc_n.point_at <= 0 THEN pc_n.pc_sign + N'0.' + REPLICATE(N'0', -pc_n.point_at) + pc_n.digits " +
            "WHEN pc_n.point_at >= LEN(pc_n.digits) " +
            "THEN pc_n.pc_sign + pc_n.digits + REPLICATE(N'0', pc_n.point_at - LEN(pc_n.digits)) " +
            "ELSE pc_n.pc_sign + STUFF(pc_n.digits, pc_n.point_at + 1, 0, N'.') END " +
            "FROM (SELECT CONVERT(varchar(30), ",
            ", 3) AS t) pc_s CROSS APPLY (SELECT " +
            "CASE WHEN LEFT(pc_s.t, 1) = '-' THEN N'-' ELSE N'' END AS pc_sign, " +
            "CONVERT(nvarchar(30), TRIM('0' FROM REPLACE(REPLACE(LEFT(pc_s.t, CHARINDEX('e', pc_s.t) - 1), '-', ''), '.', ''))) AS digits, " +
            "CONVERT(int, SUBSTRING(pc_s.t, CHARINDEX('e', pc_s.t) + 1, 5)) + 1 AS point_at) pc_n)");
    }

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
    ///     A path as a literal. <c>N</c>-prefixed because a plain one goes through the database's code
    ///     page first (#880): that is a refused path for some keys and, worse, a silent miss for others,
    ///     since <c>$."日本語"</c> arrives as <c>$."???"</c> and is perfectly well formed.
    /// </summary>
    private static string PathLiteral(string path) => $"N'$.{EscapePath(path)}'";

    private static string AppendPathLiteral(string path) => $"N'append $.{EscapePath(path)}'";

    /// <summary>
    ///     The kind of the value at a path as OPENJSON's <c>[type]</c> column has it, NULL if there is
    ///     no such member. The length is there because <c>=</c> ignores trailing spaces under every
    ///     collation, and a second row in here is an error.
    /// </summary>
    private static string KindOf(string[] segments)
    {
        var key = JsonPathHelper.KeyOf(segments[^1]);
        var source = segments.Length == 1 ? "data" : $"data, {PathLiteral(string.Join(".", segments[..^1]))}";

        return $"(SELECT j.[type] FROM OPENJSON({source}) j WHERE j.[key] = N'{EscapePath(key)}' " +
               $"AND DATALENGTH(j.[key]) = {key.Length * 2})";
    }

    /// <summary>
    ///     The document to write <paramref name="path" /> in: <see cref="Document" />, with anything that
    ///     path needs above it put there first (#880). <c>JSON_MODIFY</c> to a path with an absent parent changes
    ///     nothing and says nothing.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Mirrors Marten's <c>mt_jsonb_fix_null_parent</c>, so a parent that is absent or a JSON
    ///         null becomes an object, and one that holds anything else is left alone. The difference
    ///         is in how far it reaches: Marten only calls it for Rename and Duplicate, and here it is
    ///         on everything that writes.
    ///     </para>
    ///     <para>
    ///         This has to be an expression in the same statement. A statement of its own in front
    ///         would have to run the WHERE clause again afterwards, and a patch by a filter that reads
    ///         the parent would no longer match what it just created.
    ///     </para>
    ///     <para>
    ///         Only for the document being written to. A read of the old value goes on using
    ///         <c>data</c>, and this can't be nested in itself, because each level would repeat the one
    ///         below three times. That is why a duplicate to more than one nested destination is more
    ///         than one action.
    ///     </para>
    /// </remarks>
    private static string WithParents(string path)
    {
        var segments = JsonPathHelper.SplitPath(path);
        if (segments.Length == 1)
        {
            return Document;
        }

        var sql = new StringBuilder(
            $"CASE WHEN JSON_QUERY(data, {PathLiteral(string.Join(".", segments[..^1]))}) IS NOT NULL THEN {Document} ");

        for (var depth = 1; depth < segments.Length; depth++)
        {
            var missing = segments[depth..^1].Reverse()
                .Aggregate("{}", (below, segment) => $"{{{JsonPathHelper.MemberName(segment)}:{below}}}");

            sql.Append($"WHEN ISNULL({KindOf(segments[..depth])}, {JsonKindNull}) = {JsonKindNull} ");
            sql.Append($"THEN JSON_MODIFY({Document}, {PathLiteral(string.Join(".", segments[..depth]))}, ");
            sql.Append($"JSON_QUERY(N'{EscapePath(missing)}')) ");
        }

        return sql.Append($"ELSE {Document} END").ToString();
    }

    /// <summary>
    ///     A <c>JSON_MODIFY</c> of one value, whatever kind it is. See <see cref="NumberText" />.
    /// </summary>
    private static void WriteModify(ICommandBuilder builder, string document, string path, object? value)
    {
        if (value is NumberText number)
        {
            builder.Append(
                $"REPLACE(CAST(JSON_MODIFY({document}, {path}, N'{LiteralPlaceholder}') AS nvarchar(max)), " +
                $"N'\"{LiteralPlaceholder}\"', ");
            builder.AppendParameter(number.Json);
            builder.Append(")");
            return;
        }

        builder.Append($"JSON_MODIFY({document}, {path}, ");
        builder.AppendParameter(value ?? DBNull.Value);
        builder.Append(")");
    }

    /// <summary>
    ///     Whether the element in OPENJSON row <paramref name="alias" /> is this one, exactly. Plain
    ///     <c>=</c> isn't that (#880). JSON is case-sensitive and the database's collation may not be,
    ///     and under every collation <c>=</c> ignores trailing spaces, which is what the length is for.
    ///     The kind is there because the row holds <c>1</c> and <c>"1"</c> as the same text.
    /// </summary>
    private static void WriteScalarMatch(ICommandBuilder builder, string alias, ScalarElement element)
    {
        builder.Append($"{alias}.[type] = {element.Kind}");
        if (element.Kind == JsonKindNull)
        {
            return;
        }

        builder.Append($" AND {SqlText.BinaryCollated($"{alias}.value")} = CAST(");
        builder.AppendParameter(element.Text);
        var parameter = "@" + builder.LastParameterName!;
        builder.Append($" AS nvarchar(max)) AND DATALENGTH({alias}.value) = DATALENGTH({parameter})");
    }

    /// <summary>
    ///     Whether the element in OPENJSON row <paramref name="alias" /> is this object or array, and the
    ///     name of the parameter holding it.
    /// </summary>
    /// <remarks>
    ///     ⚠️ On native <c>json</c> OPENJSON renders an object from what is stored, with its strings
    ///     unescaped, and that isn't what the serializer wrote: <c>clé</c> comes back as <c>clé</c>
    ///     and <c>a<b</c> as <c>a&lt;b</c>. So the object to look for goes through the same rendering
    ///     rather than being compared as written, and the same goes for a number in it.
    /// </remarks>
    private static string WriteComplexMatch(ICommandBuilder builder, string alias, string json, bool nativeJson)
    {
        builder.Append($"{SqlText.BinaryCollated($"{alias}.value")} = ");

        if (!nativeJson)
        {
            builder.AppendParameter(json);
            return "@" + builder.LastParameterName!;
        }

        builder.Append("(SELECT pc_element.value FROM OPENJSON(CAST(N'[' + CAST(");
        builder.AppendParameter(json);
        var parameter = "@" + builder.LastParameterName!;
        builder.Append(" AS nvarchar(max)) + N']' AS json)) pc_element)");
        return parameter;
    }

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

    private static void WriteInsertAtIndex(ICommandBuilder builder, string jsonPath, int index,
        Action<ICommandBuilder> writeElement)
    {
        var path = PathLiteral(jsonPath);

        builder.Append($"JSON_MODIFY({WithParents(jsonPath)}, {path}, JSON_QUERY(");
        builder.Append("COALESCE(");
        builder.Append("'[' + (");
        builder.Append(
            "SELECT STRING_AGG(t.val, ',') WITHIN GROUP (ORDER BY t.sort_order) ");
        builder.Append("FROM (");
        builder.Append(
            $"SELECT {ElementAsJson} as val, CAST(j.[key] AS int) * 2 + 1 as sort_order FROM OPENJSON(JSON_QUERY(data, {path})) j ");
        builder.Append("UNION ALL ");
        builder.Append("SELECT ");
        writeElement(builder);
        builder.Append(SortOrderLiteral(index));
        builder.Append(") t");
        builder.Append(") + ']', '[]')");
        builder.Append("))");
    }

    internal static Action<ICommandBuilder> SetScalar(string jsonPath, object? value)
    {
        var path = PathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder => WriteModify(builder, document, path, value);
    }

    internal static Action<ICommandBuilder> SetComplex(string jsonPath, string jsonValue)
    {
        var path = PathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY({document}, {path}, JSON_QUERY(");
            builder.AppendParameter(jsonValue);
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> IncrementInt(string jsonPath, object increment, string sqlType)
    {
        var path = PathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder =>
        {
            builder.Append(
                $"JSON_MODIFY({document}, {path}, ISNULL(CAST(JSON_VALUE(data, {path}) AS {sqlType}), 0) + ");
            builder.AppendParameter(increment);
            builder.Append(")");
        };
    }

    internal static Action<ICommandBuilder> IncrementDecimal(string jsonPath, object increment, string sqlType)
    {
        var path = PathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder =>
        {
            builder.Append(
                $"JSON_MODIFY({document}, {path}, CAST(ISNULL(CAST(JSON_VALUE(data, {path}) AS {sqlType}), 0) + CAST(");
            builder.AppendParameter(increment);
            builder.Append($" AS {sqlType}) AS {sqlType}))");
        };
    }

    /// <summary>
    ///     An increment of a <c>float</c> or a <c>real</c>, written as text.
    /// </summary>
    /// <remarks>
    ///     <c>JSON_MODIFY</c> can't be given the sum as a float (#880). On <c>nvarchar(max)</c> it renders
    ///     16 significant digits, one short of a double, so <c>0.1 + 0.2</c> loses the last one. On
    ///     native <c>json</c> it goes through <c>numeric(38,10)</c>, which takes 1E-11 + 1E-11 to 0 and
    ///     overflows from 1E+29.
    /// </remarks>
    internal static Action<ICommandBuilder> IncrementFloat(string jsonPath, object increment, string sqlType,
        bool nativeJson)
    {
        var path = PathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        var (before, after) = FloatAsJson(nativeJson);

        return builder =>
        {
            builder.Append(
                $"REPLACE(CAST(JSON_MODIFY({document}, {path}, N'{LiteralPlaceholder}') AS nvarchar(max)), " +
                $"N'\"{LiteralPlaceholder}\"', {before}");
            builder.Append($"CAST(ISNULL(CAST(JSON_VALUE(data, {path}) AS {sqlType}), 0) + CAST(");
            builder.AppendParameter(increment);
            builder.Append($" AS {sqlType}) AS float){after})");
        };
    }

    internal static Action<ICommandBuilder> AppendScalar(string jsonPath, object? element)
    {
        var path = AppendPathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder => WriteModify(builder, document, path, element);
    }

    internal static Action<ICommandBuilder> AppendComplex(string jsonPath, string jsonElement)
    {
        var path = AppendPathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder =>
        {
            builder.Append($"JSON_MODIFY({document}, {path}, JSON_QUERY(");
            builder.AppendParameter(jsonElement);
            builder.Append("))");
        };
    }

    internal static Action<ICommandBuilder> AppendIfNotExistsScalar(string jsonPath, ScalarElement element)
    {
        var path = PathLiteral(jsonPath);
        var appendPath = AppendPathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, {path})) j WHERE ");
            WriteScalarMatch(builder, "j", element);
            builder.Append(") THEN ");
            WriteModify(builder, document, appendPath, element.Value);
            builder.Append(" ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> AppendIfNotExistsComplex(string jsonPath, string jsonElement, bool nativeJson)
    {
        var path = PathLiteral(jsonPath);
        var appendPath = AppendPathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, {path})) j WHERE ");
            var parameter = WriteComplexMatch(builder, "j", jsonElement, nativeJson);
            builder.Append(") ");
            builder.Append($"THEN JSON_MODIFY({document}, {appendPath}, JSON_QUERY({parameter})) ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> SetDictKey(string dictPath, string key, string jsonValue)
        => SetComplex($"{dictPath}.{key}", jsonValue);

    internal static Action<ICommandBuilder> SetDictKeyIfNotExists(string dictPath, string key,
        Action<ICommandBuilder> set, bool nativeJson)
    {
        var path = PathLiteral($"{dictPath}.{key}");
        var scalar = SqlText.UnlimitedJsonStringAt("data", path, nativeJson);
        return builder =>
        {
            builder.Append($"CASE WHEN {scalar} IS NULL AND ");
            builder.Append($"JSON_QUERY(data, {path}) IS NULL THEN ");
            set(builder);
            builder.Append(" ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> InsertAtIndex(string jsonPath, int index, string jsonElement)
        => builder => WriteInsertAtIndex(builder, jsonPath, index, b => b.AppendParameter(jsonElement));

    internal static Action<ICommandBuilder> InsertIfNotExistsScalar(string jsonPath, ScalarElement element, int? index)
    {
        var path = PathLiteral(jsonPath);
        var appendPath = AppendPathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, {path})) j WHERE ");
            WriteScalarMatch(builder, "j", element);
            builder.Append(") THEN ");

            if (index.HasValue)
            {
                WriteInsertAtIndex(builder, jsonPath, index.Value, b => b.AppendParameter(element.Json));
            }
            else
            {
                WriteModify(builder, document, appendPath, element.Value);
            }

            builder.Append(" ELSE data END");
        };
    }

    internal static Action<ICommandBuilder> InsertIfNotExistsComplex(string jsonPath, string jsonElement, int? index,
        bool nativeJson)
    {
        var path = PathLiteral(jsonPath);
        var appendPath = AppendPathLiteral(jsonPath);
        var document = WithParents(jsonPath);
        return builder =>
        {
            builder.Append($"CASE WHEN NOT EXISTS (SELECT 1 FROM OPENJSON(JSON_QUERY(data, {path})) j WHERE ");
            var parameter = WriteComplexMatch(builder, "j", jsonElement, nativeJson);
            builder.Append(") THEN ");

            if (index.HasValue)
            {
                WriteInsertAtIndex(builder, jsonPath, index.Value, b => b.Append(parameter));
            }
            else
            {
                builder.Append($"JSON_MODIFY({document}, {appendPath}, JSON_QUERY({parameter}))");
            }

            builder.Append(" ELSE data END");
        };
    }

    private static void WriteRebuiltArray(ICommandBuilder builder, string path, Action<ICommandBuilder> writeKeep)
    {
        builder.Append($"JSON_MODIFY({Document}, {path}, JSON_QUERY(");
        builder.Append("COALESCE(");
        builder.Append("'[' + (");
        builder.Append(
            $"SELECT STRING_AGG({ElementAsJson}, ',') WITHIN GROUP (ORDER BY CAST(j.[key] AS int)) ");
        builder.Append($"FROM OPENJSON(JSON_QUERY(data, {path})) j ");
        builder.Append("WHERE ");
        writeKeep(builder);
        builder.Append(") + ']', '[]')");
        builder.Append("))");
    }

    internal static Action<ICommandBuilder> RemoveScalarFirst(string jsonPath, ScalarElement element)
    {
        var path = PathLiteral(jsonPath);
        return builder => WriteRebuiltArray(builder, path, keep =>
        {
            keep.Append("CAST(j.[key] AS int) != ISNULL((");
            keep.Append($"SELECT MIN(CAST(j2.[key] AS int)) FROM OPENJSON(JSON_QUERY(data, {path})) j2 WHERE ");
            WriteScalarMatch(keep, "j2", element);
            keep.Append("), -1)");
        });
    }

    internal static Action<ICommandBuilder> RemoveScalarAll(string jsonPath, ScalarElement element)
    {
        var path = PathLiteral(jsonPath);
        return builder => WriteRebuiltArray(builder, path, keep =>
        {
            keep.Append("NOT (");
            WriteScalarMatch(keep, "j", element);
            keep.Append(")");
        });
    }

    internal static Action<ICommandBuilder> RemoveComplexFirst(string jsonPath, string jsonElement, bool nativeJson)
    {
        var path = PathLiteral(jsonPath);
        return builder => WriteRebuiltArray(builder, path, keep =>
        {
            keep.Append("CAST(j.[key] AS int) != ISNULL((");
            keep.Append($"SELECT MIN(CAST(j2.[key] AS int)) FROM OPENJSON(JSON_QUERY(data, {path})) j2 WHERE ");
            WriteComplexMatch(keep, "j2", jsonElement, nativeJson);
            keep.Append("), -1)");
        });
    }

    internal static Action<ICommandBuilder> RemoveComplexAll(string jsonPath, string jsonElement, bool nativeJson)
    {
        var path = PathLiteral(jsonPath);
        return builder => WriteRebuiltArray(builder, path, keep =>
        {
            keep.Append("j.value IS NULL OR NOT (");
            WriteComplexMatch(keep, "j", jsonElement, nativeJson);
            keep.Append(")");
        });
    }

    internal static Action<ICommandBuilder> RemoveDictKey(string dictPath, string key)
        => DeleteProperty($"{dictPath}.{key}");

    internal static Action<ICommandBuilder> DeleteProperty(string jsonPath)
    {
        var path = PathLiteral(jsonPath);
        return builder => { builder.Append($"JSON_MODIFY({Document}, {path}, NULL)"); };
    }

    internal static Action<ICommandBuilder> RenameProperty(string oldJsonPath, string newJsonPath, bool nativeJson)
    {
        var oldPath = PathLiteral(oldJsonPath);
        var newPath = PathLiteral(newJsonPath);

        // A source that isn't there leaves the destination alone. This is the second run of a migration,
        // or a document already in the new shape, and writing the NULL would delete the renamed value.
        // The two are siblings, so a source that is there means the parent is as well.
        var removeOld = $"JSON_MODIFY({Document}, {oldPath}, NULL)";
        var sql = WriteMemberValue(oldJsonPath, nativeJson, removeOld,
            value => $"JSON_MODIFY(JSON_MODIFY({Document}, {newPath}, {value}), {oldPath}, NULL)");

        return builder => builder.Append(sql);
    }

    /// <param name="sourcePath">Where to copy from.</param>
    /// <param name="destPaths">
    ///     Where to copy to. Only the first gets its parents, see <see cref="WithParents" />, so anything
    ///     nested after that has to be a second action.
    /// </param>
    /// <param name="nativeJson">How <c>data</c> is stored.</param>
    internal static Action<ICommandBuilder> DuplicateProperty(string sourcePath, string[] destPaths, bool nativeJson)
    {
        string writeAll(string first, string value) => destPaths.Aggregate(first,
            (json, destination) => $"JSON_MODIFY({json}, {PathLiteral(destination)}, {value})");

        // The serializer leaves a null member out, so a missing source is a null one, and copying a
        // null over the destinations is what was asked for.
        var sql = WriteMemberValue(sourcePath, nativeJson, writeAll(Document, "NULL"),
            value => writeAll(WithParents(destPaths[0]), value));

        return builder => builder.Append(sql);
    }

    /// <summary>
    ///     Writes the value at <paramref name="jsonPath" /> somewhere else in the document, as the
    ///     same JSON it is now.
    /// </summary>
    /// <param name="jsonPath">The source.</param>
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
    ///         that isn't an object or array. A CLR type cannot pick between them: a
    ///         <see cref="TimeSpan" />, a <see cref="Uri" /> and a <c>byte[]</c> are each stored as a
    ///         JSON string, and an <see cref="object" /> member can hold anything.
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
    private static string WriteMemberValue(string jsonPath, bool nativeJson, string whenMissing,
        Func<string, string> write)
    {
        var path = PathLiteral(jsonPath);

        // The same limit on every kind: JSON_QUERY has none, and for a number or a bool the unlimited
        // string read answers exactly the text that is stored. It reads the text as well because on
        // the column it answers json, which JSON_MODIFY refuses as a value when it is working on text.
        var fragment = $"JSON_QUERY({Document}, {path})";
        var text = SqlText.UnlimitedJsonStringAt("data", path, nativeJson);
        var kind = KindOf(JsonPathHelper.SplitPath(jsonPath));

        return $"CASE WHEN {fragment} IS NOT NULL THEN {write(fragment)} "
               + $"WHEN {kind} = {JsonKindString} THEN {write(text)} "
               + $"WHEN {kind} IN ({JsonKindNumber}, {JsonKindBoolean}) THEN "
               + $"REPLACE(CAST({write($"N'{LiteralPlaceholder}'")} AS nvarchar(max)), N'\"{LiteralPlaceholder}\"', {text}) "
               + $"ELSE {whenMissing} END";
    }
}

/// <summary>
///     A number that has to reach the document as exactly this text. No SQL type gets all of them
///     there: a double loses digits as a <c>float</c> argument, and a <c>ulong</c> or an
///     <see cref="Int128" /> can't be bound at all. See <see cref="PatchOperation" />.
/// </summary>
internal sealed record NumberText(string Json);

/// <summary>
///     An array element that isn't an object or an array, in each of the forms a patch needs it.
/// </summary>
/// <param name="Value">What to hand <c>JSON_MODIFY</c> to store it.</param>
/// <param name="Json">It as JSON, for building an array by hand.</param>
/// <param name="Text">What OPENJSON's <c>value</c> column holds for it.</param>
/// <param name="Kind">What OPENJSON's <c>[type]</c> column holds for it.</param>
internal sealed record ScalarElement(object? Value, string Json, string Text, int Kind);
