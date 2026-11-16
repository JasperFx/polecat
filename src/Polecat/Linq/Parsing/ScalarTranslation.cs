using JasperFx;
using System.Globalization;
using System.Linq.Expressions;
using JasperFx.Core.Reflection;
using Polecat.Internal;
using Polecat.Linq.Members;
using Polecat.Storage;

namespace Polecat.Linq.Parsing;

/// <summary>
///     A scalar SQL expression and the .NET type it computes.
/// </summary>
internal sealed record ScalarSql(string Sql, Type Type)
{
    /// <summary>The SQL type <see cref="Sql" /> yields, where Polecat maps one; null for text.</summary>
    public string? SqlType => SqlTypeMap.For(Nullable.GetUnderlyingType(Type) ?? Type);

    /// <summary>
    ///     Whether <see cref="Sql" /> reads a subquery, which SQL Server refuses inside an aggregate and in a
    ///     GROUP BY (Msg 130/144).
    /// </summary>
    public bool IsSubquery => Sql.Contains("(SELECT ", StringComparison.OrdinalIgnoreCase);

    /// <summary>The value as a member, for the paths that read one.</summary>
    public ComputedMember AsMember() => new(Sql, Type, SqlType ?? "nvarchar(max)", IsSubquery);
}

/// <summary>
///     gh-1012 / gh-1017 (marten#5957, #5968, #5942, #5955): translates a scalar expression over the queried
///     row -- an ordering key, the value a <c>Select()</c> computes, either side of a comparison -- into the
///     SQL Server expression that computes the SAME value .NET would, or refuses it with the reason.
/// </summary>
/// <remarks>
///     <para>
///         Exactness is the whole rule. What is accepted is what SQL Server computes the way .NET does:
///         members and indexes of the document, negation, integer and floating point arithmetic over one
///         type (C# has already inserted the promotions as conversions), <c>%</c> over integers, integer
///         division (both truncate toward zero), widening numeric conversions, an enum to its underlying
///         integer where enums are stored as integers, string concatenation (<c>CONCAT</c>, which reads null as
///         empty as .NET does), <c>ToUpper()</c>, <c>ToLower()</c>, <c>Length</c>, and <c>ToString()</c> of an
///         integer or a Guid. A part that reads nothing from the row is worked out before the query runs.
///     </para>
///     <para>
///         Refused, because SQL Server would compute something else: decimal division (SQL Server rounds the
///         scale to fit decimal(38)), <c>%</c> over floating point or decimal, a narrowing or lossy cast
///         (<c>(byte)256</c> is 0 in .NET), a conditional, a coalesce, a comparison or a negation as a value,
///         a <c>Math</c> call, <c>Substring</c>, and the text form of anything whose .NET text SQL Server does not
///         write (a double, a date). The same list as Marten's, which is what makes the two stores agree.
///     </para>
///     <para>
///         ⚠️ One divergence is accepted, as Marten accepts it: integer overflow. .NET wraps an unchecked
///         <c>int</c>, and SQL Server raises an arithmetic overflow error instead of answering. That is a
///         failed query rather than a wrong one.
///     </para>
/// </remarks>
internal sealed class ScalarTranslation
{
    private static readonly Dictionary<Type, int> NumericRank = new()
    {
        [typeof(byte)] = 1, [typeof(sbyte)] = 1,
        [typeof(short)] = 2, [typeof(ushort)] = 2,
        [typeof(int)] = 3, [typeof(uint)] = 3,
        [typeof(long)] = 4, [typeof(ulong)] = 4,
        [typeof(float)] = 5,
        [typeof(double)] = 6,
        [typeof(decimal)] = 7
    };

    private static readonly HashSet<Type> IntegralTypes = [typeof(short), typeof(int), typeof(long)];

    private static readonly HashSet<Type> ArithmeticTypes =
        [typeof(short), typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal)];

    /// <summary>The scalar types a translated value can have, and so the types it can be read back as.</summary>
    private static readonly HashSet<Type> ResultTypes =
    [
        typeof(string), typeof(short), typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal),
        typeof(Guid), typeof(DateTime), typeof(DateTimeOffset), typeof(DateOnly), typeof(TimeOnly)
    ];

    private readonly IMemberResolver _members;

    private ScalarTranslation(IMemberResolver members)
    {
        _members = members;
    }

    /// <summary>Whether a value of <paramref name="type" /> can be the result of a translation.</summary>
    public static bool IsScalarResult(Type type) => ResultTypes.Contains(Underlying(type));

    /// <summary>
    ///     The SQL for <paramref name="body" />, or false with the reason it has no exact translation.
    /// </summary>
    public static bool TryTranslate(Expression body, IMemberResolver members, out ScalarSql sql, out string reason)
    {
        sql = null!;
        reason = string.Empty;

        if (!IsScalarResult(body.Type))
        {
            reason = $"its result is a {body.Type.ShortNameInCode()}, which is not a scalar SQL computes";
            return false;
        }

        try
        {
            sql = new ScalarSql(new ScalarTranslation(members).Translate(body), body.Type);
            return true;
        }
        catch (NotTranslatable e)
        {
            reason = e.Message;
            return false;
        }
    }

    private string Translate(Expression node)
    {
        if (!ReadsTheRow.In(node)) return Constant(node);

        switch (node)
        {
            case MemberExpression or BinaryExpression { NodeType: ExpressionType.ArrayIndex }
                when IsDocumentValue(node):
                return DocumentValue(node);

            case MethodCallExpression call when IsDocumentValue(call):
                return DocumentValue(call);

            case ParameterExpression:
                throw new NotTranslatable("the whole row is not a scalar");

            case MemberExpression { Member.Name: nameof(string.Length) } length
                when length.Expression?.Type == typeof(string):
                return $"CAST(DATALENGTH(CAST({Translate(length.Expression)} AS nvarchar(max))) / 2 AS int)";

            case ConditionalExpression conditional when TryReadThroughNullGuard(conditional, out var guarded):
                return Translate(guarded);

            case UnaryExpression unary:
                return TranslateUnary(unary);

            case BinaryExpression binary:
                return TranslateBinary(binary);

            case MethodCallExpression call:
                return TranslateMethod(call);
        }

        throw new NotTranslatable($"'{node}' has no SQL translation");
    }

    /// <summary>
    ///     A chain of members and indexes that starts at the row. A member of something computed, such as
    ///     <c>(x.A + x.B).Length</c>, is not one.
    /// </summary>
    private static bool IsDocumentValue(Expression node)
        => node switch
        {
            MemberExpression member => DocumentPaths.IsRooted(member),
            _ => DocumentPaths.IsIndexPath(node)
        };

    private string DocumentValue(Expression node)
    {
        var member = _members.ResolvePath(node);
        var type = Underlying(member.MemberType);

        if (member.IsBoolean || type == typeof(bool))
        {
            throw new NotTranslatable($"'{node}' is a bool, which is stored as text and has no SQL value to compute with");
        }

        if (type.IsEnum)
        {
            throw new NotTranslatable(
                $"'{node}' is an enum; only its conversion to its underlying integer, where enums are stored as "
                + "integers, is translated");
        }

        if (!IsScalarResult(member.MemberType))
        {
            throw new NotTranslatable($"'{node}' is a {member.MemberType.ShortNameInCode()}, not a scalar");
        }

        return member.ValueLocator();
    }

    /// <summary>
    ///     gh-1015: <c>o == null ? null : o.Amount</c>, which is how a member of the side of a left or right join
    ///     that may have no match is read. SQL reads that member as NULL exactly when there is no match, so the
    ///     guard is already what the member means there. Any other conditional is refused.
    /// </summary>
    private static bool TryReadThroughNullGuard(ConditionalExpression conditional, out Expression guarded)
    {
        guarded = null!;

        if (conditional.Test is not BinaryExpression { NodeType: ExpressionType.Equal or ExpressionType.NotEqual } test)
        {
            return false;
        }

        var tested = IsNull(test.Right) ? test.Left : IsNull(test.Left) ? test.Right : null;
        if (tested == null || !ReadsTheRow.In(tested)) return false;

        var (whenNull, otherwise) = test.NodeType == ExpressionType.Equal
            ? (conditional.IfTrue, conditional.IfFalse)
            : (conditional.IfFalse, conditional.IfTrue);

        if (!IsNull(whenNull) || !IsReadOff(otherwise, tested)) return false;

        guarded = otherwise;
        return true;
    }

    private static bool IsNull(Expression expression)
        => !ReadsTheRow.In(expression) && WhereClauseParser.ExtractValue(expression) == null;

    /// <summary>Whether <paramref name="expression" /> is a member chain read off <paramref name="owner" />.</summary>
    private static bool IsReadOff(Expression expression, Expression owner)
    {
        var current = LinqQueryParser.StripConvert(expression);
        while (current is MemberExpression member)
        {
            if (LinqQueryParser.StripConvert(member.Expression!) is var next && ExpressionEquals(next, LinqQueryParser.StripConvert(owner)))
            {
                return true;
            }

            current = LinqQueryParser.StripConvert(member.Expression!);
        }

        return false;
    }

    private static bool ExpressionEquals(Expression a, Expression b)
        => a == b || a.ToString() == b.ToString() && a.Type == b.Type;

    private static string Constant(Expression node)
    {
        var value = WhereClauseParser.ExtractValue(node);
        switch (value)
        {
            case null:
                throw new NotTranslatable($"'{node}' is null");
            case string text:
                return "N" + SqlEscaping.Literal(text);
            case int or short:
                return Number(Convert.ToInt64(value).ToString(CultureInfo.InvariantCulture));
            case long number:
                return $"CAST({Number(number.ToString(CultureInfo.InvariantCulture))} AS bigint)";
            case double number when double.IsFinite(number):
                return $"CAST({number.ToString("R", CultureInfo.InvariantCulture)} AS float)";
            case float number when float.IsFinite(number):
                return $"CAST({number.ToString("R", CultureInfo.InvariantCulture)} AS real)";
            case decimal number:
                // A literal with a point is decimal(p,s) of exactly its own digits, which is what keeps the
                // scale of what it is multiplied by from being rounded away.
                var text2 = number.ToString(CultureInfo.InvariantCulture);
                return Number(text2.Contains('.') ? text2 : text2 + ".0");
            case Guid guid:
                return $"CONVERT(uniqueidentifier, '{guid:D}')";
        }

        throw new NotTranslatable($"the value '{node}' is a {value.GetType().ShortNameInCode()}");
    }

    private static string Number(string text) => text.StartsWith('-') ? $"({text})" : text;

    private string TranslateUnary(UnaryExpression node)
    {
        switch (node.NodeType)
        {
            case ExpressionType.Negate or ExpressionType.NegateChecked when ArithmeticTypes.Contains(Underlying(node.Type)):
                return $"(-{Translate(node.Operand)})";

            case ExpressionType.Convert or ExpressionType.ConvertChecked:
                return TranslateConversion(node);
        }

        throw new NotTranslatable($"'{node}' has no SQL translation");
    }

    private string TranslateConversion(UnaryExpression node)
    {
        var source = Underlying(node.Operand.Type);
        var target = Underlying(node.Type);

        // Identity, or wrapping into / unwrapping from Nullable<T>
        if (source == target) return Translate(node.Operand);

        if (source.IsEnum)
        {
            if (target != Enum.GetUnderlyingType(source) || !IsDocumentValue(node.Operand))
            {
                throw new NotTranslatable($"'{node}' converts an enum to something other than its underlying integer");
            }

            var member = _members.ResolvePath(node.Operand);
            if (member.LocatorSqlType != "int")
            {
                throw new NotTranslatable(
                    $"'{node}' converts an enum stored by name, whose number SQL cannot see; store enums as integers");
            }

            return member.TypedLocator;
        }

        if (IsWidening(source, target))
        {
            return $"CAST({Translate(node.Operand)} AS {WideningType(source, target)})";
        }

        throw new NotTranslatable(
            $"'{node}' is a narrowing or lossy conversion, which SQL Server does not compute the way .NET does");
    }

    private static bool IsWidening(Type source, Type target)
    {
        if (!NumericRank.TryGetValue(source, out var sourceRank) || !NumericRank.TryGetValue(target, out var targetRank))
        {
            return false;
        }

        // int, uint, long and ulong to float, and long and ulong to double, lose precision
        if (target == typeof(float)) return sourceRank <= 2;
        if (target == typeof(double)) return sourceRank <= 3 || source == typeof(float);

        return sourceRank <= 4 && targetRank > sourceRank;
    }

    /// <summary>
    ///     The SQL type to widen to. An integer to decimal is a decimal of exactly the integer's digits, so it
    ///     cannot push a product past decimal(38) and have its scale rounded away.
    /// </summary>
    private static string WideningType(Type source, Type target)
    {
        if (target != typeof(decimal)) return SqlTypeMap.For(target)!;

        var digits = NumericRank[source] switch { 1 => 3, 2 => 5, 3 => 10, _ => 20 };
        return $"decimal({digits},0)";
    }

    private string TranslateBinary(BinaryExpression node)
    {
        if (node.NodeType is ExpressionType.Add or ExpressionType.AddChecked && node.Type == typeof(string))
        {
            return Concat([node.Left, node.Right]);
        }

        var type = Underlying(node.Left.Type);
        if (!ArithmeticTypes.Contains(type) || Underlying(node.Right.Type) != type || Underlying(node.Type) != type)
        {
            throw new NotTranslatable($"'{node}' is not arithmetic over numbers of one type");
        }

        var op = node.NodeType switch
        {
            ExpressionType.Add or ExpressionType.AddChecked => "+",
            ExpressionType.Subtract or ExpressionType.SubtractChecked => "-",
            ExpressionType.Multiply or ExpressionType.MultiplyChecked => "*",
            ExpressionType.Divide when type != typeof(decimal) => "/",
            ExpressionType.Modulo when IntegralTypes.Contains(type) => "%",
            _ => throw new NotTranslatable(
                $"'{node}' ({node.NodeType} over {type.Name}) is not computed by SQL Server the way .NET computes it")
        };

        return $"({Translate(node.Left)} {op} {Translate(node.Right)})";
    }

    /// <summary>
    ///     <c>CONCAT</c> reads a null as empty, as .NET's concatenation does. It starts from an empty
    ///     <c>nvarchar(max)</c> so the result is not cut at 4000 characters, which is where <c>CONCAT</c> of
    ///     shorter arguments stops.
    /// </summary>
    private string Concat(IEnumerable<Expression> parts)
        => $"CONCAT(CAST(N'' AS nvarchar(max)), {string.Join(", ", parts.Select(Text))})";

    /// <summary>One operand of a concatenation. A non-string operand reaches string.Concat boxed to object.</summary>
    private string Text(Expression node)
    {
        if (node is UnaryExpression { NodeType: ExpressionType.Convert } convert && node.Type == typeof(object))
        {
            node = convert.Operand;
        }

        return AsText(node);
    }

    /// <summary>The text of an integer or a Guid, which SQL Server writes the way .NET does.</summary>
    private string AsText(Expression node)
    {
        var type = Underlying(node.Type);
        if (type == typeof(string)) return Translate(node);

        if (IntegralTypes.Contains(type)) return $"CONVERT(nvarchar(20), {Translate(node)})";

        // .NET writes a Guid in lower case, SQL Server in upper
        if (type == typeof(Guid)) return $"LOWER(CONVERT(nvarchar(36), {Translate(node)}))";

        throw new NotTranslatable(
            $"'{node}' is a {node.Type.ShortNameInCode()}, whose text SQL Server does not write the way .NET does");
    }

    private string TranslateMethod(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(string) && node.Method.Name == nameof(string.Concat)
                                                        && node.Object == null && node.Arguments.Count is >= 2 and <= 4
                                                        && node.Arguments.All(x => x.Type != typeof(string[]) && x.Type != typeof(object[])))
        {
            return Concat(node.Arguments);
        }

        if (node.Object != null && node.Arguments.Count == 0)
        {
            if (node.Object.Type == typeof(string))
            {
                switch (node.Method.Name)
                {
                    case nameof(string.ToUpper) or nameof(string.ToUpperInvariant):
                        return $"UPPER({Translate(node.Object)})";
                    case nameof(string.ToLower) or nameof(string.ToLowerInvariant):
                        return $"LOWER({Translate(node.Object)})";
                }
            }

            if (node.Method.Name == nameof(ToString)) return AsText(node.Object);
        }

        throw new NotTranslatable($"the method call '{node}' has no SQL translation");
    }

    private static Type Underlying(Type type) => Nullable.GetUnderlyingType(type) ?? type;

    private sealed class NotTranslatable(string message) : Exception(message);
}
